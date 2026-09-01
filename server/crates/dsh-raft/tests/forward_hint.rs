//! 契约：learner 上 client_write 返回 ForwardToLeader 且携带 leader 的 http_addr（login 转发依赖）。
use std::sync::{Arc, RwLock};
use std::time::Duration;

use dsh_core::command::Command;
use dsh_core::StateMachine;
use dsh_raft::*;
use dsh_storage::RedbStorage;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn learner_forward_hint() {
    let network = NetworkFactory::new();
    let dir = std::env::temp_dir().join("dsh-fwd-hint");
    let _ = std::fs::remove_dir_all(&dir);
    let dir2 = std::env::temp_dir().join("dsh-fwd-hint2");
    let _ = std::fs::remove_dir_all(&dir2);

    let storage = RedbStorage::open(&dir.display().to_string()).unwrap();
    let db = storage.raw_db();
    let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage))));
    let sm_store = Arc::new(StateMachineStore::new(sm.clone(), db.clone()));
    let log_store = LogStore::new(db.clone());
    let n1 = NodeInfo {
        grpc_addr: "127.0.0.1:8001".into(),
        http_addr: "127.0.0.1:9001".into(),
        raft_addr: "127.0.0.1:7001".into(),
    };
    let raft1 = new_raft_node(1, n1.clone(), log_store, sm_store, &network, dev_config())
        .await
        .unwrap();
    network.register(1, raft1.clone());
    initialize_single(&raft1, 1, n1.clone()).await.unwrap();
    assert!(wait_for_leader(&raft1, Duration::from_secs(5))
        .await
        .is_some());

    let storage2 = RedbStorage::open(&dir2.display().to_string()).unwrap();
    let db2 = storage2.raw_db();
    let sm2 = Arc::new(RwLock::new(StateMachine::new(Box::new(storage2))));
    let sm_store2 = Arc::new(StateMachineStore::new(sm2.clone(), db2.clone()));
    let log_store2 = LogStore::new(db2.clone());
    let n2 = NodeInfo {
        grpc_addr: "127.0.0.1:8002".into(),
        http_addr: "127.0.0.1:9002".into(),
        raft_addr: "127.0.0.1:7002".into(),
    };
    let raft2 = new_raft_node(2, n2.clone(), log_store2, sm_store2, &network, dev_config())
        .await
        .unwrap();
    network.register(2, raft2.clone());
    raft1.add_learner(2, n2.clone(), false).await.unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;

    // learner 上 client_write → ForwardToLeader 且携带 leader 的 http_addr（login 转发依赖此契约）
    let r = try_client_write(
        &raft2,
        Command::ProjectCreate {
            name: "x".into(),
            operator: String::new(),
            ts: 0,
            clone_from: None,
        },
    )
    .await;
    match r {
        Err(WriteError::ForwardToLeader {
            leader_id: Some(1),
            http_addr: Some(addr),
        }) => {
            assert_eq!(addr, "127.0.0.1:9001");
        }
        other => panic!("expected ForwardToLeader with hint, got {other:?}"),
    }
    assert_eq!(leader_http_addr(&raft2).as_deref(), Some("127.0.0.1:9001"));

    drop(raft1);
    drop(raft2);
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&dir2);
}
