//! (P3a-A) `{data_dir}/snapshots/` startup sweep（store.rs `sweep_snapshot_payloads`,hooked
//! into StateMachineStore ctor after prepare_snap_dir）:
//! - `*.part`（crashed-write residue）removed;
//! - canonical `*.snap` not referenced by registry `file` key removed;
//! - referenced payload + foreign files kept;
//! - registry read/parse failure aborts the sweep (nothing deleted).

use std::fs;
use std::sync::{Arc, RwLock};

use dsh_core::StateMachine;
use dsh_raft::*;
use dsh_storage::{RedbStorage, TBL_SNAPSHOTS};

static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("dsh-sweep-{tag}-{}-{n}", std::process::id()))
}

#[test]
fn sweep_removes_parts_and_orphans() {
    let dir = tmpdir("orphan");
    let _ = std::fs::remove_dir_all(&dir);
    let storage = RedbStorage::open(&dir.display().to_string()).unwrap();
    let db = storage.raw_db();
    let snap_dir = dir.join("snapshots"); // SNAP_SUBDIR
    fs::create_dir_all(&snap_dir).unwrap();

    // registry: single `file` key = JSON-encoded PathBuf（store.rs registry format）
    let keep = snap_dir.join("current.snap");
    fs::write(keep.clone(), b"payload").unwrap();
    let raw = serde_json::to_vec(&keep).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TBL_SNAPSHOTS).unwrap();
        table.insert(b"file".as_slice(), raw.as_slice()).unwrap();
    }
    txn.commit().unwrap();

    fs::write(snap_dir.join("old.snap"), b"stale payload").unwrap();
    fs::write(snap_dir.join("crashed.snap.part"), b"partial").unwrap();
    fs::write(snap_dir.join("notes.txt"), b"operator drop").unwrap();

    let (parts, orphans) = sweep_snapshot_payloads(&db, snap_dir.clone()).unwrap();
    assert_eq!(parts, 1);
    assert_eq!(orphans, 1);
    assert!(fs::exists(keep.clone()).unwrap_or(false));
    assert!(!fs::exists(snap_dir.join("old.snap")).unwrap_or(false));
    assert!(!fs::exists(snap_dir.join("crashed.snap.part")).unwrap_or(false));
    assert!(fs::exists(snap_dir.join("notes.txt")).unwrap_or(false));

    // idempotent: second sweep is a no-op
    let (p2, o2) = sweep_snapshot_payloads(&db, snap_dir).unwrap();
    assert_eq!((p2, o2), (0, 0));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn sweep_aborts_on_corrupt_registry() {
    let dir = tmpdir("corrupt");
    let _ = std::fs::remove_dir_all(&dir);
    let storage = RedbStorage::open(&dir.display().to_string()).unwrap();
    let db = storage.raw_db();
    let snap_dir = dir.join("snapshots");
    fs::create_dir_all(&snap_dir).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TBL_SNAPSHOTS).unwrap();
        table
            .insert(b"file".as_slice(), b"not-json".as_slice())
            .unwrap();
    }
    txn.commit().unwrap();

    fs::write(snap_dir.join("payload.snap"), b"x").unwrap();
    fs::write(snap_dir.join("residue.snap.part"), b"y").unwrap();

    assert!(sweep_snapshot_payloads(&db, snap_dir.clone()).is_err());
    // abort before any deletion（registry parsed before enumeration）
    assert!(fs::exists(snap_dir.join("payload.snap")).unwrap_or(false));
    assert!(fs::exists(snap_dir.join("residue.snap.part")).unwrap_or(false));

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn store_ctor_sweeps_snapshots_dir() {
    let dir = tmpdir("ctor");
    let _ = std::fs::remove_dir_all(&dir);
    let storage = RedbStorage::open(&dir.display().to_string()).unwrap();
    let db = storage.raw_db();
    let snap_dir = dir.join("snapshots");
    fs::create_dir_all(&snap_dir).unwrap();

    // no registry rows: every canonical payload is garbage
    fs::write(snap_dir.join("legacy.snap"), b"x").unwrap();
    fs::write(snap_dir.join("crashed.snap.part"), b"y").unwrap();
    fs::write(snap_dir.join("keepme.txt"), b"z").unwrap();

    let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage))));
    let _store = StateMachineStore::new(sm, db, dir.clone());

    assert!(!fs::exists(snap_dir.join("legacy.snap")).unwrap_or(false));
    assert!(!fs::exists(snap_dir.join("crashed.snap.part")).unwrap_or(false));
    assert!(fs::exists(snap_dir.join("keepme.txt")).unwrap_or(false));

    let _ = std::fs::remove_dir_all(&dir);
}
