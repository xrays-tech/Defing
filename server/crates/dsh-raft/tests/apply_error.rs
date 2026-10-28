//! P1 契约（0.10 responder）：RaftStateMachine::apply 的错误路径即所有已消费条目's responder
//! 已 respond——Raft::client_write 的 must return（never hang）。
//! 注入：StateMachineStore.set_fail_last_applied（第 N write_last_applied → IO Err）。
//! 0.9-style early `?` 即 dropped pending responders → client_write 的 would hang forever.
//!
//! 实测（0.10.0-alpha.37）：storage IO 错误 during apply 即 RaftCore 的 caches a StorageError and
//! subsequent client_write 的 return it fast-fail（non-hanging, retry-able by upper layers）。

use std::sync::{Arc, RwLock};
use std::time::Duration;

use dsh_core::command::Command;
use dsh_core::StateMachine;
use dsh_raft::*;
use dsh_storage::RedbStorage;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_error_path_returns_to_client() {
    let network = NetworkFactory::new();
    let dir = std::env::temp_dir().join("dsh-apply-err");
    let _ = std::fs::remove_dir_all(&dir);

    let storage = RedbStorage::open(&dir.display().to_string()).unwrap();
    let db = storage.raw_db();
    let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage))));
    let sm_store = Arc::new(StateMachineStore::new(sm.clone(), db.clone(), dir.clone()));
    let sm_inject = sm_store.clone();
    let log_store = LogStore::new(db.clone());
    let n1 = NodeInfo {
        grpc_addr: "127.0.0.1:8111".into(),
        http_addr: "127.0.0.1:9111".into(),
        raft_addr: "127.0.0.1:7111".into(),
    };
    let raft1 = new_raft_node(1, n1.clone(), log_store, sm_store, &network, dev_config())
        .await
        .unwrap();
    network.register(1, raft1.clone());
    initialize_single(&raft1, 1, n1.clone()).await.unwrap();
    assert!(wait_for_leader(&raft1, Duration::from_secs(5))
        .await
        .is_some());

    // warmup write（baseline success）
    match client_write(&raft1, Command::SessionLogout, Duration::from_secs(5)).await {
        Ok(Ok(_)) => {}
        other => panic!("warmup write must succeed, got {other:?}"),
    }

    // arm: next write_last_applied → IO Err。client_write 的 must RETURN（responder delivered
    // or fast-fail StorageError）——guard: 3 s timeout would catch the 0.9-style hang。
    sm_inject.set_fail_last_applied(1);
    let outcome = tokio::time::timeout(
        Duration::from_secs(3),
        try_client_write(&raft1, Command::SessionLogout),
    )
    .await;
    let returned = match outcome {
        Err(_) => panic!("client_write HUNG — responder lost（P1 regression）"),
        Ok(r) => r,
    };
    match returned {
        Ok(Err(e)) => {
            // responder path：client 看到 dsh-level persist error
            assert!(
                e.message.contains("last_applied persist failed"),
                "responder must carry persist error, got: {e}",
            );
        }
        Err(WriteError::Other(msg)) => {
            // fast-fail path：RaftError 的 carries the storage IO failure（no hang）
            assert!(
                msg.contains("injected write_last_applied IO failure"),
                "raft-layer error must carry the injected IO failure, got: {msg}",
            );
        }
        other => panic!("unexpected non-error outcome: {other:?}"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}
