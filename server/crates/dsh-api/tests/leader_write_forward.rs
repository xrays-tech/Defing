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
// raft 日志读取（LogStore 经 openraft 的 reader/storage 协议 trait 才有方法）
use dsh_raft::openraft::storage::{RaftLogReader, RaftLogStorage};
use dsh_storage::RedbStorage;
use dsh_watch::WatchHub;

static SEQ: AtomicU64 = AtomicU64::new(0);

struct TestNode {
    base: String,
    // 该节点 raft 日志 store 的 redb 句柄（raw_db 前 StateMachineStore/LogStore 各自 clone）。
    db: dsh_raft::store::DbHandle,
}

/// 3 节点真实 raft 集群（seed 建群，全员 voter）+ 每节点完整 HTTP 路由。
/// `join_token` 为真时注入节点 `--join-token`（`ops-secret`），开启恢复通道
///（admin/reset-password 以 ops token 调用）——仅无 GA 的日志测试需要它；其余测试
/// 保持 join_token 未配置（cluster/join 无 Bearer 时仍返回 428 leader-redirect，而非 401）。
async fn start3(join_token: bool) -> Vec<TestNode> {
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
        let sm_store = Arc::new(StateMachineStore::new(sm.clone(), db.clone(), dir.clone()));
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
        pending.push((id, listener, sm, raft, db));
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
    let jt: Option<Arc<str>> = if join_token {
        Some(Arc::from("ops-secret"))
    } else {
        None
    };
    for (id, listener, sm, raft, db) in pending {
        let state = ApiState::with_retention(
            sm,
            WatchHub::new(),
            Some(raft),
            Some(id),
            None,
            Duration::from_secs(86400),
            "admin-pw".into(),
            None,
            0,
            0,
            // 恢复通道 ops token（仅当 join_token 为真）：reset-password 测试用;
            // cluster/join 无 Bearer 时仍 428 leader-redirect when未配置.
            jt.clone(),
            Arc::new(dsh_api::TrustedProxies::empty()),
            None,
        );
        let app = build_router(state);
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        nodes.push(TestNode {
            base: format!("http://{addr}"),
            db,
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
    let nodes = start3(false).await;
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
    // 反例：/api/v1/cluster/join 的 428 保持「客户端跟随」契约,不被中间件转发
    let nodes = start3(false).await;
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

/// 全局管理员在集群下的转发与授权（设计 global-admin §13 第 15 条）：
/// 经 follower 登录 GA → 200 且响应回填 role=global_admin / username（转发回落）；
/// 集群下 members → 200（只读）、promote → 403、建项目 → 403。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ga_login_forwarded_with_username_and_cluster_authz() {
    let nodes = start3(false).await;
    let any = &nodes[0].base;
    let sup = login(any).await;

    // 建 GA（写请求命中 follower 时会由中间件转发到 leader）
    let resp = reqwest::Client::new()
        .post(format!("{any}/api/v1/admins"))
        .bearer_auth(&sup)
        .json(&serde_json::json!({"username": "ga1", "password": "pw12345"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 201, "create ga1");

    let (leader, members) = cluster_view(any, &sup).await;
    let follower = members
        .iter()
        .find(|(node_id, _)| node_id != &leader)
        .map(|(_, base)| base.clone())
        .expect("follower member");

    // 经 follower 登录 GA：login 内联转发到 leader。
    // 注意 follower 的 GA 分派读的是**本地** sm（read_mode 默认 Stale，login 无线性读门控），
    // 复制未跟上时会回落 pa_login → 401 ⇒ 这里做有界重试（避免偶发红）。
    let mut last = (0u16, serde_json::Value::Null);
    for _ in 0..40 {
        let resp = reqwest::Client::new()
            .post(format!("{follower}/api/v1/login"))
            .json(&serde_json::json!({"username": "ga1", "password": "pw12345"}))
            .send()
            .await
            .unwrap();
        let code = resp.status().as_u16();
        let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
        if code == 200 {
            last = (code, body);
            break;
        }
        last = (code, body);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(last.0, 200, "经 follower 的 GA 登录应成功: {}", last.1);
    let body = last.1;
    assert_eq!(body["role"], "global_admin");
    assert_eq!(body["username"], "ga1", "转发响应必须回填 username");
    let ga = body["token"].as_str().unwrap().to_string();
    assert!(ga.starts_with("ga.ga1."), "token 前缀: {ga}");

    // 集群环境（这里才是 members 200 的正确断言位置）
    let resp = reqwest::Client::new()
        .get(format!("{follower}/api/v1/cluster/members"))
        .bearer_auth(&ga)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200, "GA 可只读集群成员");

    let resp = reqwest::Client::new()
        .post(format!("{follower}/api/v1/cluster/promote"))
        .bearer_auth(&ga)
        .json(&serde_json::json!({"node_id": 2}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 403, "GA 不得 promote");

    let resp = reqwest::Client::new()
        .post(format!("{follower}/api/v1/projects"))
        .bearer_auth(&ga)
        .json(&serde_json::json!({"name": "by-ga"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 403, "GA 不得创建项目");
}

/// 读取该节点 raft 日志，标记是否出现三个目标命令（无 GA 混合版本兼容边界断言用）。
/// 通过节点本地 `LogStore(db.clone())` 读取：leader 的 `client_write` 在提交后同步返回
/// 200，因此命令必在 leader 自身日志中可见（容忍 follower 复制滞后）。
/// 返回 `(has_global_admins_revoke_all, has_multi_session_logout_all, has_admin_set_password)`。
async fn scan_admin_log(db: dsh_raft::store::DbHandle) -> (bool, bool, bool) {
    let mut store = dsh_raft::LogStore::new(db.clone());
    let st = store.get_log_state().await.unwrap();
    let start = st.last_purged_log_id.map(|l| l.index + 1).unwrap_or(1);
    let last = st.last_log_id.map(|l| l.index).unwrap_or(0);
    let mut has_revoke = false;
    let mut has_logout = false;
    let mut has_setpw = false;
    if start <= last {
        let entries = store.try_get_log_entries(start..=last).await.unwrap();
        for e in entries {
            if let dsh_raft::openraft::EntryPayload::Normal(cmd) = &e.payload {
                match cmd {
                    dsh_core::command::Command::GlobalAdminSessionsRevokeAll => {
                        has_revoke = true;
                    }
                    dsh_core::command::Command::MultiSessionLogoutAll => {
                        has_logout = true;
                    }
                    dsh_core::command::Command::AdminSetPassword { .. } => {
                        has_setpw = true;
                    }
                    _ => {}
                }
            }
        }
    }
    (has_revoke, has_logout, has_setpw)
}

/// 混合版本兼容承诺（设计 §11.1 / §0）：无 GA 账号时，`force-logout {}` 与
/// `reset-password`（ops token）都不得把 `GlobalAdminSessionsRevokeAll` 写入 raft 日志——
/// 滚动升级窗口内的旧版本节点无法 apply 该新命令，因此全 super 下线必须保持旧日志形状；
/// 同时 `MultiSessionLogoutAll` / `AdminSetPassword` 仍应正常出现。
/// 断言基于真实 raft 日志（leader 的 LogStore）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_ga_admin_ops_do_not_write_sessions_revoke_all() {
    let nodes = start3(true).await;
    let any = &nodes[0].base;
    let sup = login(any).await; // 无 GA in cluster

    // 当前 leader's own raft log store (synchronous commit at client_write return)
    let (leader_id, members) = cluster_view(any, &sup).await;
    let leader_base = members
        .iter()
        .find(|(nid, _)| nid == &leader_id)
        .map(|(_, base)| base.clone())
        .expect("leader member http base");
    let leader_node = nodes
        .iter()
        .find(|n| n.base == leader_base)
        .expect("leader TestNode db handle");

    // 1) force-logout {} (缺省 branch: (None, None)) → 200
    let resp = reqwest::Client::new()
        .post(format!("{leader_base}/api/v1/admin/force-logout"))
        .bearer_auth(&sup)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "force-logout {{}} (no GA) 应 200: {:?}",
        resp.text().await,
    );

    let (has_revoke, has_logout, _) = scan_admin_log(leader_node.db.clone()).await;
    assert!(
        has_logout,
        "force-logout {{}} 应写 MultiSessionLogoutAll (旧 log shape)"
    );
    assert!(
        !has_revoke,
        "无 GA ⇒ force-logout must not write GlobalAdminSessionsRevokeAll (mixed-version compat)",
    );

    // 2) reset-password (ops token = --join-token "ops-secret") → 200
    let resp = reqwest::Client::new()
        .post(format!("{leader_base}/api/v1/admin/reset-password"))
        .bearer_auth("ops-secret")
        .json(&serde_json::json!({"password": "newpass123"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "reset-password (ops token, no GA) 应 200: {:?}",
        resp.text().await,
    );

    let (has_revoke, _, has_setpw) = scan_admin_log(leader_node.db.clone()).await;
    assert!(has_setpw, "reset-password 应写 AdminSetPassword");
    assert!(
        !has_revoke,
        "无 GA ⇒ reset-password must not write GlobalAdminSessionsRevokeAll (mixed-version compat)",
    );
}

/// M2c：集群下的 GlobalAdminSessionsRevokeAll 转发+两段写覆盖——
/// 经 follower 创建 GA（POST /admins 的 428→leader 转发 → 201）、经 follower 登录 GA（200）、
/// 经 follower `force-logout {}`（200，428→leader 转发；leader 上 `MultiSessionLogoutAll`
/// + `GlobalAdminSessionsRevokeAll` 两段写）；断言 GA 与 super 会话都失效（经 leader 直读），
/// 且 leader 日志中 both commands 出现。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ga_cluster_force_logout_writes_sessions_revoke_all() {
    let nodes = start3(false).await;
    let any = &nodes[0].base;
    let sup = login(any).await;

    let (leader_id, members) = cluster_view(any, &sup).await;
    let leader_base = members
        .iter()
        .find(|(nid, _)| nid == &leader_id)
        .map(|(_, base)| base.clone())
        .expect("leader member http base");
    let follower = members
        .iter()
        .find(|(nid, _)| nid != &leader_id)
        .map(|(_, base)| base.clone())
        .expect("follower member");
    let leader_node = nodes
        .iter()
        .find(|n| n.base == leader_base)
        .expect("leader TestNode db handle");

    // ① 经 follower 创建 GA：写命中 follower → 428 → 中间件透明转发到 leader → 201
    let resp = reqwest::Client::new()
        .post(format!("{follower}/api/v1/admins"))
        .bearer_auth(&sup)
        .json(&serde_json::json!({
            "username": "ga-cluster",
            "password": "pw12345",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        201,
        "经 follower 创建 GA 应被转发成功: {:?}",
        resp.text().await,
    );

    // ② 经 follower 登录 GA → 200， 拿 token。
    // 与 ga_login_forwarded_with_username_and_cluster_authz 里：follower 的 GA 分派读
    // 是**本地** sm（read_mode 默认 Stale，login 无线性读门控）；复制未跟上时
    // 回落 pa_login → 401 ⇒ 有有界重试（避免偶发红）。
    let mut last = (0u16, serde_json::Value::Null);
    for _ in 0..40 {
        let resp = reqwest::Client::new()
            .post(format!("{follower}/api/v1/login"))
            .json(&serde_json::json!({
                "username": "ga-cluster",
                "password": "pw12345",
            }))
            .send()
            .await
            .unwrap();
        let code = resp.status().as_u16();
        let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
        if code == 200 {
            last = (code, body);
            break;
        }
        last = (code, body);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(last.0, 200, "经 follower 的 GA 登录应成功: {}", last.1);
    let ga = last.1["token"].as_str().unwrap().to_string();

    // ③ 经 follower force-logout {} → 428 转发 → 200（覆盖 leader 上两段写）
    let resp = reqwest::Client::new()
        .post(format!("{follower}/api/v1/admin/force-logout"))
        .bearer_auth(&sup)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "经 follower force-logout {{}} 应被转发成功: {:?}",
        resp.text().await,
    );

    // ④ 两段写失效面：GA token 与 super token 全失效（经 leader 直读，避免复制滞后）
    let resp = reqwest::Client::new()
        .get(format!("{leader_base}/api/v1/projects"))
        .bearer_auth(&ga)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        401,
        "force-logout {{}} 的 GA token 应失效: {:?}",
        resp.text().await,
    );
    let resp = reqwest::Client::new()
        .get(format!("{leader_base}/api/v1/projects"))
        .bearer_auth(&sup)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        401,
        "force-logout {{}} 的连调用者自己的 super 会话应失效: {:?}",
        resp.text().await,
    );

    // ⑤ leader 日志中 both commands 出现（leader 的 own LogStore：client_write 在 commit
    // 后同步返回 200，命令必在 leader 自身日志中可见）
    let (has_revoke, has_logout, _) = scan_admin_log(leader_node.db.clone()).await;
    assert!(
        has_revoke,
        "GA 存在时集群下 force-logout 应写 GlobalAdminSessionsRevokeAll"
    );
    assert!(has_logout, "force-logout 应写 MultiSessionLogoutAll");
}
