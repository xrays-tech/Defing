//! P2b3: membership InProgress 重试（join / promote / remove）。
//!
//! openraft 0.10 拒绝并发 membership 提案
//! (ClientWriteError::ChangeMembershipError(InProgress))。生产 handler 必须
//! 经 bounded retry 吸收 it,而非 500。
//!   - 单元:is_membership_in_progress:InProgress=true、EmptyMembership=false;
//!   - 集成:并发 join/remove → 4xx/2xx 组合,never 500。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use dsh_api::{build_router, membership_retry, ApiState};
use dsh_core::StateMachine;
use dsh_raft::*;
use dsh_storage::RedbStorage;
use dsh_watch::WatchHub;

static SEQ: AtomicU64 = AtomicU64::new(0);

struct TestServer {
    base: String,
    _state: ApiState,
}

/// 单节点真实 raft（redb + bootstrap leader）+ 完整 HTTP 路由。
async fn start() -> TestServer {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("dsh-memretry-test-{}-{n}", std::process::id()));
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

async fn super_token(base: &str) -> String {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"password": "admin-pw"}))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap_or_default();
    body["token"].as_str().unwrap().to_string()
}

async fn join(base: String, token: String, node_id: u64) -> (u16, serde_json::Value) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/v1/cluster/join"))
        .bearer_auth(&token)
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

async fn remove(base: String, token: String, node_id: u64) -> (u16, serde_json::Value) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/v1/cluster/remove"))
        .bearer_auth(&token)
        .json(&serde_json::json!({"node_id": node_id}))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let body: serde_json::Value = resp.json().await.unwrap_or_default();
    (status, body)
}

#[test]
fn membership_in_progress_predicate() {
    use dsh_raft::openraft::error::ChangeMembershipError;
    use dsh_raft::openraft::error::ClientWriteError;
    use dsh_raft::openraft::error::EmptyMembership;
    use dsh_raft::openraft::error::InProgress;
    use dsh_raft::openraft::error::RaftError;
    use dsh_raft::TypeConfig;

    let transient: RaftError<TypeConfig, ClientWriteError<TypeConfig>> = RaftError::APIError(
        ClientWriteError::ChangeMembershipError(ChangeMembershipError::InProgress(InProgress {
            committed: None,
            membership_log_id: None,
        })),
    );
    assert!(
        membership_retry::is_membership_in_progress(&transient),
        "InProgress must be absorbed (retried), not 500"
    );

    let permanent: RaftError<TypeConfig, ClientWriteError<TypeConfig>> =
        RaftError::APIError(ClientWriteError::ChangeMembershipError(
            ChangeMembershipError::EmptyMembership(EmptyMembership {}),
        ));
    assert!(
        !membership_retry::is_membership_in_progress(&permanent),
        "EmptyMembership must surface unchanged"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_membership_ops_never_500() {
    let server = start().await;
    let token = super_token(&server.base).await;

    // 两个 learners(join 本身 idempotent 路径已覆盖 elsewhere)
    let (s, b) = join(server.base.clone(), token.clone(), 2).await;
    assert_eq!(s, 200, "join node2: {b}");
    let (s, b) = join(server.base.clone(), token.clone(), 3).await;
    assert_eq!(s, 200, "join node3: {b}");

    // 4 并发 membership 提案:overlap ⇒ openraft  losers InProgress → retried.
    // Contract:2xx/4xx acceptable,500 (raw InProgress surfaced) is the regression.
    let h1 = tokio::spawn(remove(server.base.clone(), token.clone(), 2));
    let h2 = tokio::spawn(remove(server.base.clone(), token.clone(), 3));
    let h3 = tokio::spawn(join(server.base.clone(), token.clone(), 2));
    let h4 = tokio::spawn(join(server.base.clone(), token.clone(), 3));
    let results = vec![
        h1.await.unwrap(),
        h2.await.unwrap(),
        h3.await.unwrap(),
        h4.await.unwrap(),
    ];
    for entry in results {
        let (status, body) = entry;
        assert_ne!(
            status, 500,
            "membership InProgress must not surface 500: {body}"
        );
    }
}
