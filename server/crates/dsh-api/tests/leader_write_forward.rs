//! 写转发中间件契约（K3s/Service 负载均衡场景）：
//!   - 写请求命中 follower → 服务端转发到 leader → 200（核心断言）；
//!   - /api/v1/cluster/join 的 428 不被转发（客户端跟随契约保留，join_cluster 自行跟随 hint）。
//!
//! 装配说明：中间件按 428 响应体里的 leader_hint（= leader 的 NodeInfo.http_addr）转发，
//! 因此每个节点的 HTTP 监听地址必须【等于】其 NodeInfo.http_addr——
//! 装配顺序：先绑 127.0.0.1:0 拿真实地址 → 用该地址构造 NodeInfo → 建 raft 节点 →
//! seed 建群 → 等待 leader 稳定 → 再 axum::serve。
//!
//! 健壮性：三节点并发 seed 建群后 leader 可能短暂迁移，因此测试内通过
//! /api/v1/cluster/members 动态确认「当前 leader / follower」，不依赖启动时定格。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use dsh_api::{build_router, ApiState};
use dsh_core::StateMachine;
use dsh_raft::*;
use dsh_storage::RedbStorage;
use dsh_watch::WatchHub;

static SEQ: AtomicU64 = AtomicU64::new(0);

struct TestNode {
    base: String,
}

/// 3 节点真实 raft 集群（seed 建群，全员 voter）+ 每节点完整 HTTP 路由。
async fn start3() -> Vec<TestNode> {
    let network = NetworkFactory::new();
    let mut rafts: Vec<RaftHandle> = Vec::new();
    let mut seed: BTreeMap<NodeId, NodeInfo> = BTreeMap::new();
    // 先绑 HTTP 端口（ephemeral）拿真实地址，再建 raft 节点
    let mut pending = Vec::new();
    for id in 1..=3u64 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = listener.local_addr().unwrap().to_string();
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("dsh-fwd-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let storage = RedbStorage::open(&dir.display().to_string()).unwrap();
        let db = storage.raw_db();
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage))));
        let sm_store = Arc::new(StateMachineStore::new(sm.clone(), db.clone()));
        let log_store = LogStore::new(db.clone());
        let info = NodeInfo {
            grpc_addr: format!("127.0.0.1:{}", 18000 + id),
            http_addr, // ← 与真实监听地址一致，428 leader_hint 可直达
            raft_addr: format!("127.0.0.1:{}", 17000 + id),
        };
        seed.insert(id, info.clone());
        let raft = new_raft_node(
            id,
            info.clone(),
            log_store,
            sm_store,
            &network,
            dev_config(),
        )
        .await
        .unwrap();
        network.register(id, raft.clone());
        rafts.push(raft.clone());
        pending.push((id, listener, sm, raft));
    }
    // seed 建群（与 README 推荐路径一致；所有节点传相同 map）
    for raft in &rafts {
        initialize_cluster(raft, seed.clone()).await.unwrap();
    }
    // 等待 leader 产生并稳定（并发建群初期 leader 可能短暂迁移）
    let mut leader_id = None;
    for _ in 0..100 {
        for r in &rafts {
            if let Some(l) = r.current_leader().await {
                leader_id = Some(l);
                break;
            }
        }
        if leader_id.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let leader_id = leader_id.expect("cluster should elect a leader");
    // 稳定窗口：leader 连续多轮不变（或短暂等待）后再挂 HTTP，减少测试内竞态
    let mut stable = 0usize;
    for _ in 0..50 {
        let cur = rafts[0].current_leader().await;
        if cur == Some(leader_id) {
            stable += 1;
            if stable >= 3 {
                break;
            }
        } else {
            stable = 0;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // 每节点挂 HTTP（监听地址 = NodeInfo.http_addr）
    let mut nodes = Vec::new();
    for (id, listener, sm, raft) in pending {
        let state = ApiState::new(
            sm,
            WatchHub::new(),
            Some(raft),
            Some(id),
            None,
            Duration::from_secs(86400),
            "admin-pw".into(),
            None,
        );
        let app = build_router(state);
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        nodes.push(TestNode {
            base: format!("http://{addr}"),
        });
    }
    nodes
}

/// 经任意节点登录（login 有内联 leader 转发），返回全集群有效 token。
async fn login(any_base: &str) -> String {
    let login = reqwest::Client::new()
        .post(format!("{any_base}/api/v1/login"))
        .json(&serde_json::json!({ "password": "admin-pw" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        login.status().as_u16(),
        200,
        "login via any node should forward to leader"
    );
    login.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string()
}

/// 查询集群视图（轮询至 current_leader 非空）：返回 (leader_id, [(node_id, http_base)]*)。
async fn cluster_view(any_base: &str, token: &str) -> (String, Vec<(String, String)>) {
    let mut last_body = serde_json::Value::Null;
    for _ in 0..50 {
        let resp = reqwest::Client::new()
            .get(format!("{any_base}/api/v1/cluster/members"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        last_body = body.clone();
        // current_leader 序列化为 JSON 数字（NodeId = u64）
        let leader = body["current_leader"].as_u64();
        if let Some(leader) = leader {
            let members = body["members"]
                .as_array()
                .unwrap()
                .iter()
                .map(|m| {
                    let node_id = m["node_id"].as_str().expect("member node_id").to_string();
                    let base = format!(
                        "http://{}",
                        m["http_addr"].as_str().expect("member http_addr")
                    );
                    (node_id, base)
                })
                .collect::<Vec<_>>();
            return (leader.to_string(), members);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("cluster should have a leader; last members body: {last_body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_through_follower_is_forwarded() {
    let nodes = start3().await;
    let any = &nodes[0].base;
    let token = login(any).await;
    let (leader, members) = cluster_view(any, &token).await;

    // follower = 非 leader 成员（其 http_base 即真实监听地址）
    let follower = members
        .iter()
        .find(|(node_id, _)| node_id != &leader)
        .map(|(_, base)| base.clone())
        .expect("follower member");

    // 核心断言：经 follower 写项目 → 中间件转发到 leader → 200 + 转发标记头
    let resp = reqwest::Client::new()
        .post(format!("{follower}/api/v1/projects"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "svc-a" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "follower 写应被转发成功: {:?}",
        resp.text().await
    );
    assert!(
        resp.headers().contains_key("x-defing-forwarded-to"),
        "转发响应应携带 X-Defing-Forwarded-To"
    );

    // 复制生效：任意节点可读到
    let list = reqwest::Client::new()
        .get(format!("{any}/api/v1/projects"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = list.json().await.unwrap();
    assert!(
        body.as_array().unwrap().iter().any(|p| p["id"] == "svc-a"),
        "project should be replicated: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_428_is_not_forwarded() {
    // 反例：/api/v1/cluster/join 的 428 保持「客户端跟随」契约，不被中间件转发
    let nodes = start3().await;
    let any = &nodes[0].base;
    let token = login(any).await;
    let (leader, members) = cluster_view(any, &token).await;
    let follower = members
        .iter()
        .find(|(node_id, _)| node_id != &leader)
        .map(|(_, base)| base.clone())
        .expect("follower member");

    let resp = reqwest::Client::new()
        .post(format!("{follower}/api/v1/cluster/join"))
        .json(&serde_json::json!({
            "node_id": 9,
            "http_addr": "127.0.0.1:9009",
            "raft_addr": "127.0.0.1:7009",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        428,
        "join 428 不应被中间件转发（客户端跟随契约）"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "ERR_LEADER_REDIRECT");
    assert!(body["detail"]["leader_hint"].is_string());
}
