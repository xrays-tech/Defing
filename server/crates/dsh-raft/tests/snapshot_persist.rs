//! B5 快照持久化：build_snapshot 落盘 → 重启（同 data-dir 重新打开）→ get_current_snapshot 读盘返回。
//! 依赖：snapshots 表（dsh-storage）。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use dsh_core::command::Command;
use dsh_core::StateMachine;
use dsh_raft::*;
use dsh_storage::RedbStorage;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("dsh-snap-{tag}-{}-{n}", std::process::id()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_persists_across_restart() {
    let dir = tmpdir("persist");
    let _ = std::fs::remove_dir_all(&dir);

    // 1) 首启：写入状态 + 构建快照（落盘）
    {
        let storage = RedbStorage::open(&dir.display().to_string()).unwrap();
        let db = storage.raw_db();
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage))));
        sm.write()
            .unwrap()
            .apply(
                &Command::ProjectCreate {
                    name: "p".into(),
                    operator: String::new(),
                    ts: 0,
                    clone_from: None,
                },
                1,
            )
            .unwrap();
        let mut sm_store = StateMachineStore::new(sm.clone(), db.clone());
        let mut builder = sm_store.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.expect("build snapshot");
        assert!(
            !snap.snapshot.into_inner().is_empty(),
            "snapshot data should be non-empty"
        );
        drop(builder);
        drop(sm_store);
        drop(sm);
        drop(db);
    }

    // 2) 重启：同 data-dir 重新打开，内存无快照 → 应从盘恢复
    {
        let storage = RedbStorage::open(&dir.display().to_string()).unwrap();
        let db = storage.raw_db();
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage))));
        let mut s = StateMachineStore::new(sm.clone(), db);
        let snap = s
            .get_current_snapshot()
            .await
            .expect("get_current_snapshot should not error");
        let snap = snap.expect("snapshot should persist across restart (B5)");
        let data = snap.snapshot.into_inner();
        let pairs: Vec<(Vec<u8>, Vec<u8>)> =
            serde_json::from_slice(&data).expect("snapshot data decodable");
        assert!(!pairs.is_empty(), "restored snapshot should carry state");
        // 快照内容可恢复为状态机
        let restored = StateMachine::new(Box::new(dsh_core::InMemoryStore::new()));
        restored.restore_all(&pairs).expect("restore_all");
        assert!(
            restored
                .list_projects()
                .unwrap()
                .iter()
                .any(|p| p.name == "p"),
            "restored state should contain project p"
        );
        drop(s);
        drop(sm);
    }

    let _ = std::fs::remove_dir_all(&dir);
}
