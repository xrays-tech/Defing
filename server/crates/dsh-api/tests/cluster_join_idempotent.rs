//! /api/v1/cluster/join 幂等性集成测试（崩溃恢复 / 静态命令重启场景，F14 回归）。
//!
//! 背景：compose/k8s 的启动命令是静态的（每次启动同一参数）。节点带 `--join` 重启时，
//! 若 join 端点对「已在集群中的 node_id」一律返回 409，客户端会 30s 重试后退出 → 崩溃循环
//! （dev_docs/defing-cluster.md 坑 C3；修复见 dsh-cli join_cluster / dsh-api cluster_join）。
//!
//! 本测试验证修复后的行为契约：
//!   - 首次 join（node_id 未占用）→ 200 `added_learner`，`rejoined=false`；
//!   - 重复 join 同一 learner（模拟 join 已注册但响应丢失 / 节点崩溃于追赶中，重启重试）
//!     → 200 幂等成功，`rejoined=true`（节点 resume 后经 Raft 复制追赶）；
//!   - join 已存在的 voter（如冒用 node1 的 id）→ 409（防劫持，F14 保留）；
//!   - 地址格式校验仍生效 → 400。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use dsh_api::{build_router, ApiState};
use dsh_core::StateMachine;
use dsh_raft::*;
use dsh_storage::RedbStorage;
use dsh_watch::WatchHub;

static SEQ: AtomicU64 = AtomicU64::new(0);

struct TestServer {
    base: String,
    _state: ApiState,
}

/// 单节点真实 raft（redb 持久化 + bootstrap 为 leader）+ 完整 HTTP 路由。
async fn start() -> TestServer {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("dsh-join-test-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let storage = RedbStorage::open(&dir.display().to_string()).unwrap();
    let db = storage.raw_db();
    let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage))));
    let sm_store = Arc::new(StateMachineStore::new(sm.clone(), db.clone(), dir.clone()));
    let log_store = LogStore::new(db.clone());
    let network = NetworkFactory::new();
    let node = NodeInfo {
        grpc_addr: "127.0.0.1:8001".into(),
        http_addr: "127.0.0.1:9001".into(),
        raft_addr: "127.0.0.1:7001".into(),
    };
    let raft = new_raft_node(1, node.clone(), log_store, sm_store, &network, dev_config())
        .await
        .unwrap();
    network.register(1, raft.clone());
    initialize_single(&raft, 1, node.clone()).await.unwrap();
    assert!(
        wait_for_leader(&raft, Duration::from_secs(5))
            .await
            .is_some(),
        "node1 should become leader"
    );

    let state = ApiState::new(
        sm,
        WatchHub::new(),
        Some(raft),
        Some(1),
        None,
        Duration::from_secs(86400),
        "admin-pw".into(),
        None,
    );
    let app = build_router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    TestServer {
        base: format!("http://{addr}"),
        _state: state,
    }
}

async fn join(base: &str, node_id: u64) -> (u16, serde_json::Value) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/v1/cluster/join"))
        .json(&serde_json::json!({
            "node_id": node_id,
            "http_addr": format!("127.0.0.1:9{node_id:02}"),
            "raft_addr": format!("127.0.0.1:7{node_id:02}"),
        }))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let body: serde_json::Value = resp.json().await.unwrap_or_default();
    (status, body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_is_idempotent_for_existing_learner() {
    let server = start().await;

    // 1) 首次 join → 200，learner 加入
    let (s, b) = join(&server.base, 2).await;
    assert_eq!(s, 200, "first join should succeed: {b}");
    assert_eq!(b["added_learner"], 2);
    assert_eq!(b["rejoined"], false);

    // 2) 重复 join 同一 learner（崩溃恢复重试）→ 200 幂等成功
    let (s, b) = join(&server.base, 2).await;
    assert_eq!(
        s, 200,
        "re-join of existing learner must be idempotent success: {b}"
    );
    assert_eq!(b["rejoined"], true);

    // 3) 另一个节点正常加入
    let (s, b) = join(&server.base, 3).await;
    assert_eq!(s, 200, "third node join should succeed: {b}");
    assert_eq!(b["rejoined"], false);

    // 4) 冒用已存在 voter 的 id → 409（F14 防劫持，且重启恢复本就无需 join）
    let (s, b) = join(&server.base, 1).await;
    assert_eq!(s, 409, "re-join of a voter must still be rejected: {b}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_still_validates_address_format() {
    let server = start().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/v1/cluster/join", server.base))
        .json(&serde_json::json!({
            "node_id": 9,
            "http_addr": "127.0.0.1:9009",
            "raft_addr": "not-a-host-port",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        422,
        "bad raft_addr must be rejected"
    );
}
