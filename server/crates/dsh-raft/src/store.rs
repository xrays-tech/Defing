//! Raft 日志/状态机/快照存储（模块 03 §5）。
//! - LogStore：raft-log / raft-meta 表（经 RedbStorage::raw_db）
#![allow(clippy::result_large_err, clippy::type_complexity)] // openraft IOFlushed/Responder 的 Err 变体较大（上游类型）

use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

use dsh_core::error::Error as DshError;
use dsh_core::model::PublishEvent;
use dsh_core::StateMachine;
use dsh_storage::{TBL_RAFT_LOG, TBL_RAFT_META, TBL_SNAPSHOTS};
use futures_util::Stream;
use futures_util::StreamExt;
use openraft::impls::leader_id_adv::LeaderId;
use openraft::storage::EntryResponder;
use openraft::storage::{
    ApplyResponder, IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder,
    RaftStateMachine,
};
use openraft::type_config::alias::{
    EntryOf as EntryAlias, LogIdOf as LogIdAlias, SnapshotMetaOf, SnapshotOf, StoredMembershipOf,
    VoteOf,
};
use openraft::{EntryPayload, LogId, OptionalSend, Snapshot, SnapshotMeta, StoredMembership};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata};
use serde::{Deserialize, Serialize};

use crate::types::{NodeId, NodeInfo, TypeConfig};

// 0.10：storage Err is std::io::Error（0.9 StorageError<C> enum 已 gone）
type ErrOf = io::Error;

pub type EntryOf = EntryAlias<TypeConfig>;
pub type LogIdOf = LogIdAlias<TypeConfig>;

// 公共存储句柄：共享 redb（dsh_storage::RedbStorage::raw_db）
pub type DbHandle = std::sync::Arc<redb::Database>;

/// write_last_applied 故障注入 sentinel：关闭默认（usize 上限，u64 域）。
const NO_FAULT: u64 = 18446744073370008100;

/// (P3a-C1) AppendEntries 网络层批量预算（`limited_get_log_entries` 短读上限，stored bytes）。
/// Release measurements (compat §13): msgpack wire ≈ stored-JSON bytes (mp/json=1.00);
/// 64 fat entries (60 KiB values) = 3.85 MB stored / 3.67 MiB wire, redb append+fsync 5.3 ms
/// — so `max_payload_entries=64` stays the practical bound at current entry sizes and this
/// 6 MiB byte cap only chops pathological batches (e.g. 1 MiB-value entries: 64 RPC entries
/// would be 64 MiB wire → cap keeps ~6 per RPC). Contract honored: non-empty range ⇒
/// non-empty batch (first oversized entry returned alone — openraft stream_state.rs:249).
pub const MAX_BATCH_BYTES: u64 = 6 << 20;

// ---------------- meta 键（raft-meta 表） ----------------

const META_VOTE: &[u8] = b"vote";
const META_LAST_PURGED: &[u8] = b"last_purged";
const META_LAST_APPLIED: &[u8] = b"last_applied";
const META_MEMBERSHIP: &[u8] = b"membership";

// ---------------- snapshot persistence (snapshots table registry; B5; P2a file-backed) ----------------
//
// P2a（dev_docs/research/openraft-010-compat.md §12）: SnapshotData = PathBuf.
// Payload bytes live in `{data_dir}/snapshots/` files (anchor RedbStorage::base_dir());
// redb `snapshots` table only keeps a registry: `meta` (SnapshotMeta JSON) + `file`
// (current payload file path). Old 0.9/P1 inline key `data` (JSON pairs blob) is migrated
// on first read: materialize to file, write `file`, remove `data`.
// File writes go through `{name}.part` + atomic rename; every failure path removes its `.part`.

const SNAP_META_KEY: &[u8] = b"meta";
const SNAP_FILE_KEY: &[u8] = b"file";
const SNAP_DATA_KEY: &[u8] = b"data"; // legacy inline payload (0.9/P1), migrated on first read

/// `{data_dir}/snapshots/` subdir name (pairs with RedbStorage::base_dir()).
pub const SNAP_SUBDIR: &str = "snapshots";

/// Per-process monotonic suffix for snapshot payload file names (unique across rebuilds).
static SNAP_SEQ: AtomicU64 = AtomicU64::new(0);

/// Per-process nonce：文件名里的进程随机位。
///
/// 只有 `SNAP_SEQ` 时，进程重启后 seq 从 0 重新计数，同名 meta 会生成与**已持久化 registry**
/// 完全相同的文件名 —— 落盘时会覆盖 registry 指向的 payload；再叠加 `delete_old_snapshot_file`
/// 就会把刚登记的文件删掉（P3 复核 blocker）。加上非零 nonce 后进程间不重名。
fn process_nonce() -> &'static str {
    static NONCE: OnceLock<String> = OnceLock::new();
    NONCE.get_or_init(|| {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        format!("{:x}{:x}", std::process::id(), nanos & 0xffff_ffff)
    })
}

fn snap_file_name(meta: &SnapshotMetaOf<TypeConfig>) -> String {
    let seq = SNAP_SEQ.fetch_add(1, Ordering::Relaxed);
    match meta.last_log_id {
        Some(id) => format!(
            "snap-t{}-n{}-i{}-{}-{}.snap",
            id.leader_id.term,
            id.leader_id.node_id,
            id.index,
            seq,
            process_nonce()
        ),
        None => format!("snap-initial-{}-{}.snap", seq, process_nonce()),
    }
}

/// Create `{data_dir}/snapshots/` (idempotent; used at store construction and before each write).
/// Per-name `.part` pre-clear/failure cleanup lives in write_payload_file; the process-wide
/// startup sweep is `sweep_snapshot_payloads` (P3a-A, run at StateMachineStore construction).
fn prepare_snap_dir(dir: PathBuf) -> Result<(), ErrOf> {
    fs::create_dir_all(dir).map_err(io_write)?;
    Ok(())
}

/// (P3a-A) startup sweep of `{data_dir}/snapshots/`:
/// - every `*.part` (crashed write residue from any name) is removed;
/// - every canonical `*.snap` the registry's `file` key does not reference is removed
///   (install/legacy-migration renames replace the previous payload; old files are garbage);
/// - foreign names (not `*.part` / `*.snap`) are left untouched (operator debug drops).
///   Registry read/parse failure aborts the sweep (never mass-delete on a corrupt read).
///   Returns `(parts_removed, orphans_removed)`.
pub fn sweep_snapshot_payloads(db: &DbHandle, dir: PathBuf) -> Result<(u64, u64), ErrOf> {
    if !fs::exists(&dir).unwrap_or(false) {
        return Ok((0, 0));
    }
    // canonical referenced path: the single `file` registry key (JSON-encoded PathBuf).
    // `file` absent (no snapshot, or legacy inline `data` not yet migrated) = no keep.
    let ref_raw = {
        let txn = db.begin_read().map_err(io_read)?;
        let table = txn.open_table(TBL_SNAPSHOTS).map_err(io_read)?;
        table
            .get(SNAP_FILE_KEY)
            .map_err(io_read)?
            .map(|guard| guard.value().to_vec())
    };
    let keep: PathBuf = match ref_raw {
        Some(raw) => de(&raw)?,
        None => PathBuf::new(),
    };
    let mut parts: u64 = 0;
    let mut orphans: u64 = 0;
    let listing = fs::read_dir(&dir).map_err(io_read)?;
    for entry in listing {
        match entry {
            Ok(item) => {
                let path = item.path();
                let name = path.display().to_string();
                if name.ends_with(".part") {
                    match fs::remove_file(&path) {
                        Ok(()) => {
                            parts += 1;
                        }
                        Err(e) => tracing::warn!("sweep remove {}: {e}", name),
                    }
                } else if name.ends_with(".snap") {
                    if name == keep.display().to_string() {
                        continue;
                    }
                    match fs::remove_file(&path) {
                        Ok(()) => {
                            orphans += 1;
                        }
                        Err(e) => tracing::warn!("sweep remove {}: {e}", name),
                    }
                }
            }
            Err(e) => {
                // enumeration IO error: stop sweep (partial sweep done so far is fine,
                // remaining residue is transient—next boot retries)
                tracing::warn!("sweep snapshots dir: {e}");
                break;
            }
        }
    }
    if parts > 0 || orphans > 0 {
        tracing::info!(
            "snapshots dir sweep: parts={parts} orphans={orphans} ({})",
            dir.display()
        );
    }
    Ok((parts, orphans))
}

/// Atomic payload file write: bytes → `{name}.part`, then rename → `{name}`.
/// Any failure path removes the `.part` (never leaves a half-written file).
fn write_payload_file(dir: PathBuf, name: String, bytes: &[u8]) -> Result<PathBuf, ErrOf> {
    fs::create_dir_all(&dir).map_err(io_write)?;
    let part_path = dir.join(format!("{}.part", name));
    let final_path = dir.join(name);
    // Clear a stale temp for this name before starting (crashed earlier write of same name).
    let _ = fs::remove_file(&part_path);
    let done = match fs::write(&part_path, bytes) {
        Err(e) => Err(io_write(e)),
        Ok(()) => match fs::rename(&part_path, &final_path) {
            Err(e) => Err(io_write(e)),
            Ok(()) => Ok(()),
        },
    };
    if done.is_err() {
        // failed write/rename: the (possibly half-written) temp must not linger
        let _ = fs::remove_file(&part_path);
    }
    done?;
    Ok(final_path)
}

/// Read a payload file (install-side validation happens in the caller before any state change).
fn read_payload_file(path: PathBuf) -> Result<Vec<u8>, ErrOf> {
    fs::read(path).map_err(io_read)
}

/// P3c：流式解析 payload 文件成 `(key, value)` 列表 —— 只驻留解析结果一份；
/// 与 `read_payload_file` 的「整份读 + 再解析」相比，省掉一份 4xx MB 的全量拷贝。
fn read_payload_pairs(path: &std::path::Path) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ErrOf> {
    let f = fs::File::open(path).map_err(io_read)?;
    serde_json::from_reader(std::io::BufReader::new(f)).map_err(io_read)
}

/// P3c：把收到的 payload 落成本节点 canonical snapshot 文件。
///
/// 用**文件级 copy**（`fs::copy`，内核零拷贝路径），不做整份 `Vec<u8>` 中转，也不 rename
/// 输入文件。原因是输入可能是本节点 registry 自己的 payload（openraft `restore_from_snapshot`
/// 启动路径会把 registry 路径直接传给 `install_snapshot`），rename 采纳会把 registry 指向的
/// 文件移走，再叠加 `delete_old_snapshot_file` 就直接删掉刚登记的快照；copy 失败时输入
/// 保持原样，registry 仍指向可用文件。名字带进程 nonce + 自增 seq（见 `snap_file_name`），
/// 不会覆盖既有 canonical 文件。
fn copy_payload_file(dir: PathBuf, name: String, src: &std::path::Path) -> Result<PathBuf, ErrOf> {
    fs::create_dir_all(&dir).map_err(io_write)?;
    let part_path = dir.join(format!("{name}.part"));
    let final_path = dir.join(&name);
    let _ = fs::remove_file(&part_path);
    if let Err(e) = fs::copy(src, &part_path) {
        let _ = fs::remove_file(&part_path);
        return Err(io_read(e));
    }
    match fs::rename(&part_path, &final_path) {
        Ok(()) => Ok(final_path),
        Err(e) => {
            let _ = fs::remove_file(&part_path);
            Err(io_write(e))
        }
    }
}

/// Registry read: return the current snapshot as (meta, payload file path).
///
/// - `file` key present → payload is that file (missing/unreadable file = stale registry:
///   warn, treat as no snapshot — the leader re-sends on the next replication round).
/// - legacy 0.9/P1 layout (`meta` + inline `data`, no `file`) → one-shot migration:
///   validate the inline JSON parses, materialize it to a file, then in one write txn
///   insert `file` and remove `data` (registry switch is atomic; payload file written first).
fn load_snapshot_registry(
    db: &DbHandle,
    dir: PathBuf,
) -> Result<Option<(SnapshotMetaOf<TypeConfig>, PathBuf)>, ErrOf> {
    let (meta_raw, file_raw, data_raw) = {
        let txn = db.begin_read().map_err(io_read)?;
        let table = txn.open_table(TBL_SNAPSHOTS).map_err(io_read)?;
        (
            table
                .get(SNAP_META_KEY)
                .map_err(io_read)?
                .map(|g| g.value().to_vec()),
            table
                .get(SNAP_FILE_KEY)
                .map_err(io_read)?
                .map(|g| g.value().to_vec()),
            table
                .get(SNAP_DATA_KEY)
                .map_err(io_read)?
                .map(|g| g.value().to_vec()),
        )
    };
    let meta: SnapshotMetaOf<TypeConfig> = match meta_raw {
        Some(raw) => de(&raw)?,
        None => return Ok(None),
    };
    if let Some(raw) = file_raw {
        // `file` value = JSON-encoded PathBuf (serde_json handles PathBuf natively)
        let path: PathBuf = de(&raw)?;
        if fs::exists(&path).unwrap_or(false) {
            return Ok(Some((meta, path)));
        }
        tracing::warn!(
            "snapshot registry points at missing file {}; treat as none",
            path.display()
        );
        return Ok(None);
    }
    // legacy inline blob (0.9 redb `data` key): materialize once
    if data_raw.is_none() {
        return Ok(None);
    }
    let raw_data = data_raw.unwrap();
    let parsed: Result<Vec<(Vec<u8>, Vec<u8>)>, _> = serde_json::from_slice(&raw_data);
    if parsed.is_err() {
        tracing::warn!("legacy snapshot data key unparsable; leaving it for inspection");
        return Ok(None);
    }
    let path = write_payload_file(dir, snap_file_name(&meta), &raw_data)?;
    let file_val = ser(&path)?;
    let txn = db.begin_write().map_err(io_write)?;
    {
        let mut table = txn.open_table(TBL_SNAPSHOTS).map_err(io_write)?;
        table
            .insert(SNAP_FILE_KEY, file_val.as_slice())
            .map_err(io_write)?;
        table.remove(SNAP_DATA_KEY).map_err(io_write)?;
    }
    if let Err(e) = txn.commit() {
        // registry switch failed → the materialized file is an orphan: drop it, no half-state
        let _ = fs::remove_file(&path);
        return Err(io_write(e));
    }
    tracing::info!(
        "migrated legacy inline snapshot blob to file {}",
        path.display()
    );
    Ok(Some((meta, path)))
}

/// Persist the registry: meta + current file path; also removes any legacy inline `data`
/// key (same txn). Returns the previous file path (if any) for caller-side cleanup after commit.
fn persist_snapshot_registry(
    db: &DbHandle,
    meta: &SnapshotMetaOf<TypeConfig>,
    path: PathBuf,
) -> Result<Option<PathBuf>, ErrOf> {
    let meta_raw = ser(meta)?;
    let file_val = ser(&path)?;
    let old: Option<PathBuf> = {
        let txn = db.begin_read().map_err(io_read)?;
        let table = txn.open_table(TBL_SNAPSHOTS).map_err(io_read)?;
        match table.get(SNAP_FILE_KEY).map_err(io_read)? {
            Some(guard) => Some(de(guard.value())?),
            None => None,
        }
    };
    let txn = db.begin_write().map_err(io_write)?;
    {
        let mut table = txn.open_table(TBL_SNAPSHOTS).map_err(io_write)?;
        table
            .insert(SNAP_META_KEY, meta_raw.as_slice())
            .map_err(io_write)?;
        table
            .insert(SNAP_FILE_KEY, file_val.as_slice())
            .map_err(io_write)?;
        table.remove(SNAP_DATA_KEY).map_err(io_write)?;
    }
    txn.commit().map_err(io_write)?;
    Ok(old)
}

/// Delete the superseded payload file after a newer registry has committed.
/// Best-effort: IO failure only warns (registry no longer references the file).
fn delete_old_snapshot_file(old: Option<PathBuf>) {
    if let Some(p) = &old {
        if let Err(e) = fs::remove_file(p) {
            tracing::warn!("old snapshot file remove failed: {e}");
        }
    }
}

/// P2a public file helpers (network bridge + HTTP server-side materialization):
/// - write_snapshot_payload: received wire bytes → canonical file in this node's snapshots dir
///   (`.part` + rename, cleanup on failure);
/// - read_snapshot_payload: payload file → Vec for the JSON single-RPC bridge
///   (P1 transport kept; P2b will replace both sides with streaming binary frames).
pub fn write_snapshot_payload(
    snap_dir: PathBuf,
    meta: &SnapshotMetaOf<TypeConfig>,
    bytes: &[u8],
) -> Result<PathBuf, ErrOf> {
    write_payload_file(snap_dir, snap_file_name(meta), bytes)
}

/// P2b3 streaming materialization helpers. The payload file name must be minted
/// EXACTLY ONCE per snapshot (SNAP_SEQ advances per call), then both the `.part`
/// and final paths derive from that single name:
/// - open `.part` when the Header frame arrives (ChunkWriter.create),
/// - finalize_payload_part: flush done -> rename `.part` -> canonical,
/// - abort_payload_part: any protocol/IO failure path removes `.part` (best-effort).
pub fn snapshot_file_name(meta: &SnapshotMetaOf<TypeConfig>) -> String {
    snap_file_name(meta)
}

pub fn payload_part_path(dir: PathBuf, name: String) -> PathBuf {
    dir.join(format!("{}.part", name))
}

pub fn finalize_payload_part(dir: PathBuf, name: String) -> Result<PathBuf, ErrOf> {
    let part_path = payload_part_path(dir.clone(), name.clone());
    let final_path = dir.join(name);
    match fs::rename(&part_path, &final_path) {
        Ok(()) => Ok(final_path),
        Err(e) => Err(io_write(e)),
    }
}

pub fn abort_payload_part(dir: PathBuf, name: String) {
    let part_path = payload_part_path(dir, name);
    let _ = fs::remove_file(&part_path);
}

pub fn read_snapshot_payload(path: PathBuf) -> Result<Vec<u8>, ErrOf> {
    read_payload_file(path)
}

fn log_key(index: u64) -> [u8; 8] {
    index.to_be_bytes()
}

fn log_index_from_key(key: &[u8]) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&key[..8]);
    u64::from_be_bytes(b)
}

fn ser<T: Serialize>(v: &T) -> Result<Vec<u8>, ErrOf> {
    serde_json::to_vec(v).map_err(|e| io_write(e.to_string()))
}

fn de<T: for<'de> Deserialize<'de>>(raw: &[u8]) -> Result<T, ErrOf> {
    serde_json::from_slice(raw).map_err(|e| io_read(e.to_string()))
}

// 0.10：storage Err is std::io::Error（0.9 StorageIOError{subject,verb,AnyError} 已 gone）
fn io_write(msg: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("write: {msg}"))
}

fn io_read(msg: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("read: {msg}"))
}

fn storage_err(e: DshError) -> ErrOf {
    io_write(e.to_string())
}

// ---------------- raft-meta 表读写（LogStore / StateMachineStore 共用） ----------------

fn meta_get(db: &DbHandle, key: &[u8]) -> Result<Option<Vec<u8>>, ErrOf> {
    let txn = db.begin_read().map_err(io_read)?;
    let table = txn.open_table(TBL_RAFT_META).map_err(io_read)?;
    match table.get(key).map_err(io_read)? {
        Some(guard) => Ok(Some(guard.value().to_vec())),
        None => Ok(None),
    }
}

/// 写 raft-meta 单键（单事务提交；Durability 默认 Immediate，commit 即 fsync）。
fn meta_put(db: &DbHandle, key: &[u8], value: &[u8]) -> Result<(), ErrOf> {
    let txn = db.begin_write().map_err(io_write)?;
    let mut table = txn.open_table(TBL_RAFT_META).map_err(io_write)?;
    table.insert(key, value).map_err(io_write)?;
    drop(table); // 表句柄先于 commit 释放（commit 消费事务）
    txn.commit().map_err(io_write)
}

/// 共享的日志读取逻辑（LogReader 与 LogStore 复用）：[start, end] 闭区间。
fn read_entries(db: &DbHandle, start: u64, end: u64) -> Result<Vec<EntryOf>, ErrOf> {
    let txn = db.begin_read().map_err(io_read)?;
    let table = txn.open_table(TBL_RAFT_LOG).map_err(io_read)?;
    let start_key = log_key(start);
    let mut out = Vec::new();
    for row in table.range(start_key.as_slice()..).map_err(io_read)? {
        let (k, v) = row.map_err(io_read)?;
        if log_index_from_key(k.value()) > end {
            break;
        }
        out.push(de(v.value())?);
    }
    Ok(out)
}

/// Byte-capped `read_entries` for `limited_get_log_entries` (P3a-C1).
/// Accumulates **stored row bytes** (JSON-encoded entries; msgpack wire ≈1.00× per compat
/// §9.5/§13, so capping stored bytes bounds the wire batch with margin). Short reads are fine
/// (openraft replication continues from the last returned index); an empty result for a
/// non-empty range is not — the first oversized entry is returned alone.
fn read_entries_limited(
    db: &DbHandle,
    start: u64,
    end: u64,
    cap: u64,
) -> Result<Vec<EntryOf>, ErrOf> {
    let txn = db.begin_read().map_err(io_read)?;
    let table = txn.open_table(TBL_RAFT_LOG).map_err(io_read)?;
    let start_key = log_key(start);
    let mut out = Vec::new();
    let mut acc = 0u64;
    for row in table.range(start_key.as_slice()..).map_err(io_read)? {
        let (k, v) = row.map_err(io_read)?;
        if log_index_from_key(k.value()) > end {
            break;
        }
        let size = v.value().len() as u64;
        if !out.is_empty() && acc + size > cap {
            break;
        }
        out.push(de(v.value())?);
        acc += size;
    }
    Ok(out)
}

/// 最后一条日志（升序表尾部）。空表返回 None。
fn last_log_entry(db: &DbHandle) -> Result<Option<EntryOf>, ErrOf> {
    let txn = db.begin_read().map_err(io_read)?;
    let table = txn.open_table(TBL_RAFT_LOG).map_err(io_read)?;
    // 先绑定局部再匹配：避免尾表达式的 AccessGuard 临时值活得比 table 久
    let last = table.last().map_err(io_read)?;
    match last {
        Some((_, v)) => Ok(Some(de(v.value())?)),
        None => Ok(None),
    }
}

/// 在写事务内收集 range 命中的日志索引（BE u64 key → u64）。
/// 表句柄随本函数结束 drop——之后可在同一事务重新 open_table 执行删除
/// （同一事务内同名表二次 open_table 会报 TableAlreadyOpen，设计 §3.1）。
fn collect_log_indexes<'k, R>(txn: &redb::WriteTransaction, range: R) -> Result<Vec<u64>, ErrOf>
where
    R: std::ops::RangeBounds<&'k [u8]> + 'k,
{
    let table = txn.open_table(TBL_RAFT_LOG).map_err(io_write)?;
    let mut out = Vec::new();
    for row in table.range(range).map_err(io_write)? {
        let (k, _) = row.map_err(io_write)?;
        out.push(log_index_from_key(k.value()));
    }
    Ok(out)
}

/// 在写事务内逐条删除日志（调用前须已 drop 收集阶段的表句柄，见 collect_log_indexes）。
fn remove_log_indexes(txn: &redb::WriteTransaction, indexes: Vec<u64>) -> Result<(), ErrOf> {
    let mut table = txn.open_table(TBL_RAFT_LOG).map_err(io_write)?;
    for idx in indexes {
        table.remove(log_key(idx).as_slice()).map_err(io_write)?;
    }
    Ok(())
}

// ---------------- LogStore ----------------

/// Raft 日志存储（raft-log 表：key = 8B BE index）。
pub struct LogStore {
    db: DbHandle,
}

impl LogStore {
    pub fn new(db: DbHandle) -> Self {
        Self { db }
    }
}

/// 日志读取器（只读，独立于 LogStore 的写路径）。
pub struct LogReader {
    db: DbHandle,
}

impl LogReader {
    pub fn new(db: DbHandle) -> Self {
        Self { db }
    }
}

impl RaftLogReader<TypeConfig> for LogReader {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf>, ErrOf> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(i) => *i + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(i) => *i - 1,
            std::ops::Bound::Unbounded => u64::MAX,
        };
        read_entries(&self.db, start, end)
    }

    // (P3a-C1) 0.10 replication 路径（stream_state.rs limited read）: end-exclusive;
    // short reads OK (replication resumes at last returned index + 1), but a non-empty
    // range must never yield an empty batch — first oversized entry ships alone.
    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<EntryOf>, ErrOf> {
        if start >= end {
            Ok(Vec::new())
        } else {
            read_entries_limited(&self.db, start, end - 1, MAX_BATCH_BYTES)
        }
    }

    // 0.10：read_vote 在 RaftLogReader（0.9 的 LogStorage 位置 gone）
    async fn read_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, ErrOf> {
        meta_get(&self.db, META_VOTE)?
            .map(|raw| de(&raw))
            .transpose()
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf>, ErrOf> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(i) => *i + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(i) => *i - 1,
            std::ops::Bound::Unbounded => u64::MAX,
        };
        read_entries(&self.db, start, end)
    }

    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<EntryOf>, ErrOf> {
        if start >= end {
            Ok(Vec::new())
        } else {
            read_entries_limited(&self.db, start, end - 1, MAX_BATCH_BYTES)
        }
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, ErrOf> {
        meta_get(&self.db, META_VOTE)?
            .map(|raw| de(&raw))
            .transpose()
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = LogReader;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, ErrOf> {
        let last_purged = meta_get(&self.db, META_LAST_PURGED)?
            .map(|raw| de::<u64>(&raw))
            .transpose()?
            .map(|idx| LogId {
                leader_id: LeaderId {
                    term: 0,
                    node_id: NodeId::MAX,
                },
                index: idx,
            });
        let last_log = last_log_entry(&self.db)?.map(|e| e.log_id);
        Ok(LogState {
            // 0.10 契约（`storage/log_state.rs`）：日志为空时 `last_log_id` 必须等于
            // `last_purged_log_id`；0.9 允许 None，但 0.10 `get_initial_state` 会做
            // `last_log_id < last_purged_log_id` 的硬校验（helper.rs "inverted log order"），
            // 而 `None < Some(_)` 恒真。快照追平的 follower 会在 install 后 purge 到快照 index
            // （日志清空），此刻重启 = 节点永久起不来。所以空日志必须回落成 purge 位置。
            last_log_id: last_log.or(last_purged),
            last_purged_log_id: last_purged,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        LogReader::new(self.db.clone())
    }

    async fn save_vote(&mut self, vote: &VoteOf<TypeConfig>) -> Result<(), ErrOf> {
        let raw = ser(vote)?;
        meta_put(&self.db, META_VOTE, &raw)
    }

    // 0.10：read_vote 在 RaftLogReader（not on RaftLogStorage）——see LogReader impl above

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<TypeConfig>) -> Result<(), ErrOf>
    where
        I: IntoIterator<Item = EntryOf> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        // 单写事务批量 insert 后一次 commit：等价原 WriteBatch+flush_wal(true)
        // （WriteTransaction 默认 Durability::Immediate，commit 返回即已 fsync）
        let txn = self.db.begin_write().map_err(io_write)?;
        {
            let mut table = txn.open_table(TBL_RAFT_LOG).map_err(io_write)?;
            for e in entries {
                let raw = ser(&e)?;
                table
                    .insert(log_key(e.log_id.index).as_slice(), raw.as_slice())
                    .map_err(io_write)?;
            }
        }
        txn.commit().map_err(io_write)?;
        // 0.10：IOFlushed callback（0.9 LogFlushed 已 renamed; log_io_completed → io_completed）
        callback.io_completed(Ok(()));
        Ok(())
    }

    // 0.10：truncate(log_id) 改名 truncate_after(last_log_id)（删除 index > last_log_id.index）
    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf>) -> Result<(), ErrOf> {
        let txn = self.db.begin_write().map_err(io_write)?;
        let from_index = match last_log_id {
            None => 0,
            Some(id) => id.index + 1,
        };
        let from = log_key(from_index);
        let indexes = collect_log_indexes(&txn, from.as_slice()..)?;
        remove_log_indexes(&txn, indexes)?;
        txn.commit().map_err(io_write)
    }

    async fn purge(&mut self, log_id: LogIdOf) -> Result<(), ErrOf> {
        // BE 定宽 key：..=idx 的字典序范围恰为索引 <= idx 的全部日志
        let txn = self.db.begin_write().map_err(io_write)?;
        let upto = log_key(log_id.index);
        let indexes = collect_log_indexes(&txn, ..=upto.as_slice())?;
        remove_log_indexes(&txn, indexes)?;
        // 日志清理与 last_purged 同一事务原子提交（redb 多表事务原生支持）
        let mut meta_table = txn.open_table(TBL_RAFT_META).map_err(io_write)?;
        meta_table
            .insert(META_LAST_PURGED, ser(&log_id.index)?.as_slice())
            .map_err(io_write)?;
        drop(meta_table); // 表句柄先于 commit 释放（commit 消费事务）
        txn.commit().map_err(io_write)
    }
}

// ---------------- StateMachineStore ----------------

/// 状态机 + 快照（Clone 共享同一实例）。
#[derive(Clone)]
pub struct StateMachineStore {
    /// 状态机（共享：apply 与读共用锁）
    pub sm: Arc<RwLock<StateMachine>>,
    db: DbHandle,
    /// 本节点 data 目录（RedbStorage::base_dir 传入）；P2a 快照文件在 `{data_dir}/snapshots/`
    data_dir: PathBuf,
    /// 内存中的当前快照（meta + payload 文件 PathBuf；P2a: PathBuf 拷贝即 cheap，no blob clone）
    current_snapshot: Arc<tokio::sync::Mutex<Option<(SnapshotMetaOf<TypeConfig>, PathBuf)>>>,
    /// 发布事件广播（集群 watch：apply 时向本地订阅者推送）
    events: tokio::sync::broadcast::Sender<PublishEvent>,
    /// 主密钥轮换钩子：apply 到 `Command::RotateMasterKey` 成功时调用（更新本地 Cipher keyring + 持久化 ring 文件）。
    /// 入参为整个命令（F7b：新命令 kek_enc 自加密、旧日志 kek 明文），解密由钩子实现方负责。
    /// 状态机本身不落数据（确定性），此钩子是集群一致的密钥轮换副作用出口。
    rotation_hook: Option<Arc<dyn Fn(&dsh_core::command::Command) + Send + Sync>>,
    /// P1 测试注入：第 N（≥1）`write_last_applied` 调用返回 IO error（0/SENTINEL = off）。
    /// Relaxed atomic 读/增量 per entry（negligible vs redb 事务），仅测试路径非默认。
    fail_last_applied_at: Arc<AtomicU64>,
    last_applied_calls: Arc<AtomicU64>,
}

impl StateMachineStore {
    pub fn new(sm: Arc<RwLock<StateMachine>>, db: DbHandle, data_dir: PathBuf) -> Self {
        Self::new_with_rotation(sm, db, None, data_dir)
    }

    /// 构造状态机存储并挂载主密钥轮换钩子（集群模式由 dsh-cli 传入；dev-single 走 handler 本地逻辑，不挂）。
    pub fn new_with_rotation(
        sm: Arc<RwLock<StateMachine>>,
        db: DbHandle,
        hook: Option<Arc<dyn Fn(&dsh_core::command::Command) + Send + Sync>>,
        data_dir: PathBuf,
    ) -> Self {
        let (tx, _rx) = tokio::sync::broadcast::channel(1024);
        // P2a: prepare `{data_dir}/snapshots/`（best-effort warn，实际写入路径再 ensure_dir）
        let snap_dir = data_dir.join(SNAP_SUBDIR);
        if let Err(e) = prepare_snap_dir(snap_dir.clone()) {
            tracing::warn!("prepare snapshots dir failed: {e}");
        }
        // P3a-A: startup sweep — 删除 `.part` 残留与未 referenced canonical payload files
        // (registry 仍 points at its `file` key; corrupt registry read aborts the sweep).
        match sweep_snapshot_payloads(&db, snap_dir) {
            Ok(_) => {}
            Err(e) => tracing::warn!("snapshots dir startup sweep skipped: {e}"),
        };
        Self {
            sm,
            db,
            data_dir,
            current_snapshot: Arc::new(tokio::sync::Mutex::new(None)),
            events: tx,
            rotation_hook: hook,
            fail_last_applied_at: Arc::new(AtomicU64::new(NO_FAULT)),
            last_applied_calls: Arc::new(AtomicU64::new(0)),
        }
    }

    /// P1 测试注入开关（doc-hidden）：第 `n`（≥1）write_last_applied 调用 IO-fail；`0`/`NO_FAULT` 关闭。
    #[doc(hidden)]
    pub fn set_fail_last_applied(&self, n: u64) {
        self.fail_last_applied_at.store(n, Ordering::Relaxed);
        self.last_applied_calls.store(0, Ordering::Relaxed);
    }

    /// 订阅发布事件（集群 watch 用）。
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<PublishEvent> {
        self.events.subscribe()
    }

    /// 重启恢复判断（dsh-cli）：raft-meta 表非空即存在已落盘的 Raft 状态
    /// （vote / last_applied / last_purged / membership 任一存在即 true）。
    pub fn has_persisted_state(&self) -> bool {
        match self.raft_meta_is_empty() {
            Ok(empty) => !empty,
            Err(e) => {
                // 读失败按「无持久化状态」处理并告警——该方法仅用于重启恢复判断
                tracing::warn!("has_persisted_state: {e}");
                false
            }
        }
    }

    /// 读取持久化成员表（dsh-cli A2：seed 与集群现实比对用）。
    /// 返回 `None` = 尚无已持久化成员表（崩溃于追平前，resume 后会经复制追平）——
    /// 调用方应跳过比对，避免瞬态误报。
    pub fn persisted_membership(
        &self,
    ) -> Result<Option<std::collections::BTreeMap<NodeId, NodeInfo>>, String> {
        let map: std::collections::BTreeMap<NodeId, NodeInfo> = self
            .read_membership()
            .map_err(|e| format!("read membership: {e:?}"))?
            .membership()
            .nodes()
            .map(|(id, n)| (*id, n.clone()))
            .collect();
        if map.is_empty() {
            Ok(None)
        } else {
            Ok(Some(map))
        }
    }

    fn raft_meta_is_empty(&self) -> Result<bool, ErrOf> {
        let txn = self.db.begin_read().map_err(io_read)?;
        let table = txn.open_table(TBL_RAFT_META).map_err(io_read)?;
        table.is_empty().map_err(io_read)
    }

    fn read_last_applied(&self) -> Result<Option<LogIdOf>, ErrOf> {
        meta_get(&self.db, META_LAST_APPLIED)?
            .map(|raw| de(&raw))
            .transpose()
    }

    fn write_last_applied(&self, id: &LogIdOf) -> Result<(), ErrOf> {
        // P1 注入：第 N 调用 fail（测试错误路径：responder 必须已 respond 前 apply 返回 Err）
        let call = self.last_applied_calls.fetch_add(1, Ordering::Relaxed) + 1;
        let fail_at = self.fail_last_applied_at.load(Ordering::Relaxed);
        if fail_at != 0 && call >= fail_at {
            return Err(io_write("injected write_last_applied IO failure"));
        }
        // 每条独立事务（设计 §8.7：重启重放边界语义优先，不合并为批末单写）
        meta_put(&self.db, META_LAST_APPLIED, &ser(id)?)
    }

    fn read_membership(&self) -> Result<StoredMembershipOf<TypeConfig>, ErrOf> {
        match meta_get(&self.db, META_MEMBERSHIP)? {
            Some(raw) => de(&raw),
            None => Ok(StoredMembership::default()),
        }
    }

    fn write_membership(&self, m: &StoredMembershipOf<TypeConfig>) -> Result<(), ErrOf> {
        meta_put(&self.db, META_MEMBERSHIP, &ser(m)?)
    }
    #[doc(hidden)]
    /// apply 一个条目：状态机变更 + per-entry last_applied 落盘（§8.7 独立事务）。
    /// 返回 (client 响应, 事件, hard)；hard!=None 即 IO/锁错误——调用方已 respond 该 entry（P1:
    /// responder 契约），stop consuming the stream, flush, then return Err.
    fn apply_entry(
        &self,
        entry: EntryOf,
    ) -> (
        Result<crate::types::WriteAck, DshError>,
        Vec<PublishEvent>,
        Option<ErrOf>,
    ) {
        let log_id = entry.log_id;
        match self
            .sm
            .write()
            .map_err(|e| io_write(format!("state lock unavailable: {e}")))
        {
            Err(e) => (
                Err(DshError::internal(format!("state lock unavailable: {e}"))),
                Vec::new(),
                Some(e),
            ),
            Ok(mut sm) => {
                let mut events: Vec<PublishEvent> = Vec::new();
                let mut resp: Result<crate::types::WriteAck, DshError> =
                    Ok(crate::types::WriteAck::default());
                let mut hard: Option<ErrOf> = None;
                match entry.payload {
                    EntryPayload::Membership(m) => {
                        let stored = StoredMembership::new(Some(log_id), m.clone());
                        if let Err(e) = self.write_membership(&stored) {
                            resp = Err(DshError::internal(format!(
                                "membership persist failed: {e}"
                            )));
                            hard = Some(e);
                        }
                    }
                    EntryPayload::Blank => {}
                    EntryPayload::Normal(cmd) => {
                        // deterministic time from log index (D16)
                        let now_ms = log_id.index as i64;
                        match sm.apply(&cmd, now_ms) {
                            Ok(applied) => {
                                if matches!(cmd, dsh_core::command::Command::RotateMasterKey { .. })
                                {
                                    if let Some(h) = &self.rotation_hook {
                                        h(&cmd);
                                    }
                                }
                                events.extend(applied.iter().cloned());
                                resp = Ok(crate::types::WriteAck {
                                    version: events.first().map(|e| e.version).unwrap_or(0),
                                    events: applied,
                                });
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "apply command failed (logged but state unchanged): {e}"
                                );
                                resp = Err(e);
                            }
                        }
                    }
                }
                if hard.is_none() {
                    if let Err(e) = self.write_last_applied(&log_id) {
                        resp = Err(DshError::internal(format!(
                            "last_applied persist failed: {e}"
                        )));
                        hard = Some(e);
                    }
                }
                (resp, events, hard)
            }
        }
    }
}

pub struct SnapshotBuilder {
    inner: Arc<StateMachineStore>,
}

impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    // 0.10：SnapshotData 在 state machine/network/builder（not in declare_raft_types）；P2a: PathBuf
    type SnapshotData = PathBuf;

    async fn build_snapshot(&mut self) -> Result<SnapshotOf<TypeConfig, PathBuf>, ErrOf> {
        let pairs = {
            let sm = self.inner.sm.read().map_err(|e| io_read(e.to_string()))?;
            sm.dump_all().map_err(storage_err)?
        };
        let last_applied = self.inner.read_last_applied()?;
        let membership = self.inner.read_membership()?;
        let data = serde_json::to_vec(&pairs).map_err(|e| io_write(e.to_string()))?;
        // 0.10：SnapshotMeta.snapshot_id 已 gone（wire 兼容 slot 保留 but always empty，see compat doc）
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
        };
        // P2a: payload 落盘文件（`{name}.part` + rename，失败清理）——Snapshot 只 ever carries PathBuf
        let path = write_payload_file(
            self.inner.data_dir.join(SNAP_SUBDIR),
            snap_file_name(&meta),
            &data,
        )?;
        // registry 落盘（meta + file key；同事务 removes legacy inline data key）；旧文件清理 after commit
        let old = persist_snapshot_registry(&self.inner.db, &meta, path.clone())?;
        delete_old_snapshot_file(old);
        let snapshot = Snapshot {
            meta: meta.clone(),
            snapshot: path.clone(),
        };
        *self.inner.current_snapshot.lock().await = Some((meta, path));
        Ok(snapshot)
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = SnapshotBuilder;
    type SnapshotData = PathBuf;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf>, StoredMembershipOf<TypeConfig>), ErrOf> {
        let last_applied = self.read_last_applied()?;
        let membership = self.read_membership()?;
        Ok((last_applied, membership))
    }

    // 0.10：apply<I: IntoIterator> 已 gone —— 消费 Stream<EntryResponder>，响应经 responder.send(C::R)
    //（C::R = Result<WriteAck, dsh_core::Error>，见 types.rs declare_raft_types）。
    // 写锁 per-entry（RwLock write guard 跨 await 条目读取会破坏 OptionalSend 要求）。
    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), ErrOf>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, ErrOf>> + Unpin + OptionalSend,
    {
        // perf 方案③：写锁作用域化（per-entry）——events 收集后 unlock 再广播（锁外 send），
        // 缩短写锁持有时间（fsync 已由方案①收敛；send 移到锁外不阻塞读）。
        let mut pending_events: Vec<PublishEvent> = Vec::new();
        let mut pending_responses: Vec<(
            ApplyResponder<TypeConfig>,
            Result<crate::types::WriteAck, DshError>,
        )> = Vec::new();
        let mut fatal: Option<ErrOf> = None;
        loop {
            let next = entries.next().await;
            if next.is_none() {
                break;
            }
            match next.unwrap() {
                Err(e) => {
                    // 0.10 log reader stream 的 Err（IO/read failure）：stop，collected flushed below
                    fatal = Some(e);
                    break;
                }
                Ok((entry, responder)) => {
                    let (resp, events, hard) = self.apply_entry(entry);
                    pending_events.extend(events);
                    if let Some(r) = responder {
                        // 0.10：ApplyResponder<C>::send(self, C::R)——collect 后锁释放再 send（perf ③
                        // 思路：响应与事件广播均在锁外；P1: the failing entry gets Err resp here）
                        pending_responses.push((r, resp));
                    }
                    if let Some(h) = hard {
                        fatal = Some(h);
                        break;
                    }
                }
            }
        }
        if fatal.is_some() {
            // P1 drain：openraft 已 created remaining responders（client may await）——abort-respond
            // with Err, so their client_write returns; otherwise never returns（upstream contract）。
            loop {
                let more = entries.next().await;
                if more.is_none() {
                    break;
                }
                match more.unwrap() {
                    Err(e) => tracing::warn!("apply abort drain stream error: {e}"),
                    Ok((_, responder)) => {
                        if let Some(r) = responder {
                            r.send(Err(DshError::internal("apply aborted (storage failure)")));
                        }
                    }
                }
            }
        }
        // 锁外广播（watch）：所有节点本地 apply 后推送，语义一致。
        // 行为说明：解锁后广播 → "状态先于事件可见"窗口（读可先见新状态、watch 事件后到），
        // SSE watch 通知语义 + after_version 重放可回补，接受。
        for e in &pending_events {
            let _ = self.events.send(e.clone());
        }
        // 统一 flush（happy path 不变 + P1 error path）：every consumed entry has a response.
        for (responder, resp) in pending_responses {
            responder.send(resp);
        }
        if let Some(f) = fatal {
            return Err(f);
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            inner: Arc::new(StateMachineStore {
                sm: self.sm.clone(),
                db: self.db.clone(),
                data_dir: self.data_dir.clone(),
                current_snapshot: Arc::new(tokio::sync::Mutex::new(None)),
                events: self.events.clone(),
                rotation_hook: self.rotation_hook.clone(),
                fail_last_applied_at: self.fail_last_applied_at.clone(),
                last_applied_calls: self.last_applied_calls.clone(),
            }),
        }
    }

    // 0.10：begin_receiving_snapshot 已 gone —— install_snapshot 直接接受 SnapshotData（PathBuf）

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<TypeConfig>,
        snapshot: Self::SnapshotData,
    ) -> Result<(), ErrOf> {
        // 顺序（P3d）：先把 payload 落成 canonical 文件 + 解析校验，**再**动状态机。
        // 这样 copy/解析失败时状态机和 last_applied 都没有前进（旧顺序先 restore 再落盘，
        // 落盘失败会留下「last_applied 已前进但没有 registry 快照」的窗口）。
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = read_payload_pairs(&snapshot)?;
        let path = copy_payload_file(
            self.data_dir.join(SNAP_SUBDIR),
            snap_file_name(meta),
            &snapshot,
        )?;
        {
            let sm = self.sm.write().map_err(|e| io_write(e.to_string()))?;
            sm.restore_all(&pairs).map_err(storage_err)?;
        }
        if let Some(last_log_id) = &meta.last_log_id {
            self.write_last_applied(last_log_id)?;
        }
        self.write_membership(&meta.last_membership)?;
        let old = persist_snapshot_registry(&self.db, meta, path.clone())?;
        // P3d：old 与 path 相同时**不能删**（名字复用/同名 meta 会命中，删掉就等于删掉刚登记的快照）
        if old.as_deref() != Some(path.as_path()) {
            delete_old_snapshot_file(old);
        }
        *self.current_snapshot.lock().await = Some((meta.clone(), path));
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<TypeConfig, PathBuf>>, ErrOf> {
        let guard = self.current_snapshot.lock().await;
        if let Some((meta, path)) = guard.as_ref() {
            // P2a: PathBuf 拷贝 —— no whole-payload blob clone（P1 每次 clone 整个 Vec）
            return Ok(Some(Snapshot {
                meta: meta.clone(),
                snapshot: path.clone(),
            }));
        }
        drop(guard);
        // 重启后内存为空 → 读盘（B5：快照跨重启持久化；P2a: registry meta+file, legacy data 迁移）
        match load_snapshot_registry(&self.db, self.data_dir.join(SNAP_SUBDIR))? {
            Some((meta, path)) => Ok(Some(Snapshot {
                meta,
                snapshot: path,
            })),
            None => Ok(None),
        }
    }
}

// ---------------- 内嵌单测（设计 §6 N4a：范围翻译+多表事务最易错路径） ----------------

#[cfg(test)]
mod tests {
    use super::*;
    use dsh_storage::RedbStorage;
    use futures_util::stream;
    use openraft::storage::RaftLogStorageExt;
    use std::sync::Mutex;

    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    // 0.10：apply 消费 Stream<EntryResponder>；测试 entries 无 client responder（None）
    fn entry_stream(
        entries: Vec<EntryOf>,
    ) -> Vec<Result<(EntryOf, Option<ApplyResponder<TypeConfig>>), ErrOf>> {
        entries.into_iter().map(|e| Ok((e, None))).collect()
    }

    fn tmpdir(tag: &str) -> std::path::PathBuf {
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!("dsh-raft-store-{tag}-{}-{n}", std::process::id()))
    }

    fn open_storage(dir: &std::path::Path) -> RedbStorage {
        RedbStorage::open(&dir.display().to_string()).unwrap()
    }

    fn blank(index: u64) -> EntryOf {
        EntryOf {
            log_id: LogId {
                leader_id: LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index,
            },
            payload: EntryPayload::Blank,
        }
    }

    fn truncate_at(index: u64) -> LogIdOf {
        LogId {
            leader_id: LeaderId {
                term: 1,
                node_id: 1,
            },
            index,
        }
    }

    /// (last_purged, last_log) 索引对。
    async fn log_state_of(store: &mut LogStore) -> (Option<u64>, Option<u64>) {
        let st = store.get_log_state().await.unwrap();
        (
            st.last_purged_log_id.map(|l| l.index),
            st.last_log_id.map(|l| l.index),
        )
    }

    async fn indexes_of(store: &mut LogStore, range: std::ops::RangeInclusive<u64>) -> Vec<u64> {
        store
            .try_get_log_entries(range)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.log_id.index)
            .collect()
    }

    // (P3a-C1) 60 KiB-value entry（P0 §9.5 budget repro size）
    fn fat(index: u64) -> EntryOf {
        EntryOf {
            log_id: LogId {
                leader_id: LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index,
            },
            payload: EntryPayload::Normal(dsh_core::command::Command::DraftUpdate {
                project: dsh_core::model::ProjectId("c1-cap".to_string()),
                branch: dsh_core::model::BranchName("dev".to_string()),
                updates: [dsh_core::command::DraftUpdateItem {
                    group: "g".to_string(),
                    key: "k".to_string(),
                    value: dsh_core::model::Value::String("x".repeat(60_000)),
                }]
                .to_vec(),
                deletes: [].to_vec(),
                shared_bindings: [].to_vec(),
                operator: "admin".to_string(),
                ts: index as i64,
                expected_draft_rev: None,
            }),
        }
    }

    #[tokio::test]
    async fn limited_get_log_entries_byte_cap_contract() {
        let dir = tmpdir("c1cap");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let mut store = LogStore::new(storage.raw_db());
        store
            .blocking_append([fat(1), fat(2), fat(3)])
            .await
            .unwrap();

        // default cap (6 MiB) over the whole range: full batch; trait end-exclusive contract
        let all = store.limited_get_log_entries(1, 4).await.unwrap();
        assert_eq!(
            all.into_iter()
                .map(|e| e.log_id.index)
                .collect::<Vec<u64>>(),
            vec![1, 2, 3]
        );
        let one = store.limited_get_log_entries(2, 3).await.unwrap();
        assert_eq!(
            one.into_iter()
                .map(|e| e.log_id.index)
                .collect::<Vec<u64>>(),
            vec![2]
        );
        assert!(store
            .limited_get_log_entries(3, 3)
            .await
            .unwrap()
            .is_empty());

        // cap 1 KiB (< single 60 KiB row): first entry alone — never an empty batch
        let small = read_entries_limited(&store.db, 1, 3, 1024).unwrap();
        assert_eq!(
            small
                .into_iter()
                .map(|e| e.log_id.index)
                .collect::<Vec<u64>>(),
            vec![1]
        );

        // cap 122 KiB: two fat rows (≈120.3 KiB) fit, third chopped
        let mid = read_entries_limited(&store.db, 1, 3, 122_000).unwrap();
        assert_eq!(
            mid.into_iter()
                .map(|e| e.log_id.index)
                .collect::<Vec<u64>>(),
            vec![1, 2]
        );

        // cap 1 byte: still returns the first entry
        let tiny = read_entries_limited(&store.db, 2, 2, 1).unwrap();
        assert_eq!(
            tiny.into_iter()
                .map(|e| e.log_id.index)
                .collect::<Vec<u64>>(),
            vec![2]
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn append_truncate_get_log_state_consistent() {
        let dir = tmpdir("trunc");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let mut store = LogStore::new(storage.raw_db());

        store
            .blocking_append([blank(1), blank(2), blank(3)])
            .await
            .unwrap();
        assert_eq!(log_state_of(&mut store).await, (None, Some(3)));
        assert_eq!(indexes_of(&mut store, 1..=3).await, vec![1, 2, 3]);

        // 截断 index >= 2（0.10：truncate_after —— 删除 > last_log_id.index）
        store.truncate_after(Some(truncate_at(1))).await.unwrap();
        assert_eq!(log_state_of(&mut store).await, (None, Some(1)));
        assert_eq!(indexes_of(&mut store, 1..=3).await, vec![1]);

        // 截到头：last_log_id 变 None（0.10：truncate_after(None) → 删除全部）
        store.truncate_after(None).await.unwrap();
        assert_eq!(log_state_of(&mut store).await, (None, None));
        assert!(indexes_of(&mut store, 1..=3).await.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn purge_updates_last_purged_and_clears_prefix() {
        let dir = tmpdir("purge");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let mut store = LogStore::new(storage.raw_db());

        store.blocking_append((1..=5).map(blank)).await.unwrap();
        store.purge(truncate_at(3)).await.unwrap();

        // last_purged=3；日志仅剩 4、5
        assert_eq!(log_state_of(&mut store).await, (Some(3), Some(5)));
        assert_eq!(indexes_of(&mut store, 1..=u64::MAX).await, vec![4, 5]);

        // 重复 purge（空 range）幂等
        store.purge(truncate_at(3)).await.unwrap();
        assert_eq!(log_state_of(&mut store).await, (Some(3), Some(5)));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 0.10 LogState 契约 + 重启可启动性。
    ///
    /// 快照追平的 follower 在 install 后被 openraft 无条件 purge 到快照 index（日志清空）。
    /// 0.10 `get_initial_state` 有 0.9 没有的硬校验 `last_log_id >= last_purged_log_id`
    /// （`None < Some(_)` 恒真）→ 空日志若仍返回 `last_log_id = None`，节点重启即报
    /// "inverted log order in log store ... corrupted"，且状态已持久化，永久起不来。
    #[tokio::test]
    async fn purge_to_empty_then_restart_can_boot_raft() {
        let dir = tmpdir("purge-empty-boot");
        let _ = std::fs::remove_dir_all(&dir);
        {
            let storage = open_storage(&dir);
            let db = storage.raw_db();
            let mut store = LogStore::new(db.clone());
            store
                .blocking_append([blank(1), blank(2), blank(3)])
                .await
                .unwrap();
            store.purge(truncate_at(3)).await.unwrap();
            assert!(indexes_of(&mut store, 1..=u64::MAX).await.is_empty());
            // 契约：日志空 + purged=3 ⇒ last_log_id 必须回落成 3
            assert_eq!(log_state_of(&mut store).await, (Some(3), Some(3)));
        }

        // 「重启」：drop 掉全部 handle 后重新打开同一 data dir
        let storage = open_storage(&dir);
        let db = storage.raw_db();
        let mut store = LogStore::new(db.clone());
        assert_eq!(log_state_of(&mut store).await, (Some(3), Some(3)));

        // 真正用这份 data dir 起 Raft —— 0.10 的 inverted-order 校验就在 get_initial_state 里
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage))));
        let sm_store = Arc::new(StateMachineStore::new(sm, db.clone(), dir.clone()));
        let net = crate::NetworkFactory::new();
        let info = crate::NodeInfo {
            grpc_addr: "127.0.0.1:0".into(),
            http_addr: "http://127.0.0.1:0".into(),
            raft_addr: "127.0.0.1:0".into(),
        };
        let raft = crate::new_raft_node(
            1,
            info,
            LogStore::new(db),
            sm_store,
            &net,
            crate::dev_config(),
        )
        .await
        .expect("purged-empty log store 必须能在 openraft 0.10 上启动（空日志 last_log_id 回落）");
        // 关键断言是上面的 `Raft::new` 成功：0.10 的 inverted-order 校验就在 get_initial_state
        // 里跑，空日志回落不生效时这里会 Err。metrics 是异步填充的，不做即时断言。
        let _ = raft.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn snapshot_persist_load_roundtrip() {
        let dir = tmpdir("snap");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);

        // 安装快照（install_snapshot 内部落盘到 snapshots 表）
        let data = serde_json::to_vec(&vec![(b"k".to_vec(), b"v".to_vec())]).unwrap();
        let meta: SnapshotMetaOf<TypeConfig> = SnapshotMeta {
            last_log_id: Some(LogId {
                leader_id: LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 5,
            }),
            last_membership: StoredMembership::default(),
            // 0.10：SnapshotMeta.snapshot_id 已 gone（serde wire slot 保留，always empty）
        };
        {
            let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));
            let mut store = StateMachineStore::new(sm, storage.raw_db(), dir.clone());
            let payload_path = write_snapshot_payload(dir.join(SNAP_SUBDIR), &meta, &data).unwrap();
            store.install_snapshot(&meta, payload_path).await.unwrap();
        }

        // 「重启」：全新 StateMachineStore（内存快照为空）→ 从盘恢复
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));
        let mut revived = StateMachineStore::new(sm, storage.raw_db(), dir.clone());
        let snap = revived
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("snapshot should persist (B5)");
        assert_eq!(snap.meta.last_log_id, meta.last_log_id);
        // P2a: SnapshotData 即 PathBuf——payload 落盘文件,not inline Vec
        assert!(
            std::fs::exists(&snap.snapshot).unwrap_or(false),
            "snapshot payload file must exist"
        );
        assert_eq!(read_snapshot_payload(snap.snapshot).unwrap(), data);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn has_persisted_state_tracks_raft_meta() {
        let dir = tmpdir("meta");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));
        let mut store = StateMachineStore::new(sm, storage.raw_db(), dir.clone());

        assert!(
            !store.has_persisted_state(),
            "fresh store: no persisted state"
        );
        // apply 一条空白日志 → last_applied 落盘（per-entry 独立事务）
        store
            .apply(stream::iter(entry_stream([blank(1)].to_vec())))
            .await
            .unwrap();
        assert!(store.has_persisted_state());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn rotation_hook_fires_on_rotate_command_only() {
        let dir = tmpdir("rot");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));

        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
        let hook_calls = calls.clone();
        let hook_received = received.clone();
        let mut store = StateMachineStore::new_with_rotation(
            sm.clone(),
            storage.raw_db(),
            Some(Arc::new(move |cmd: &dsh_core::command::Command| {
                hook_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                // 记录命令携带的明文 KEK（旧日志路径 kek；kek_enc 解密由钩子实现方负责）
                if let dsh_core::command::Command::RotateMasterKey { kek, .. } = cmd {
                    hook_received.lock().unwrap().push(kek.clone());
                }
            })),
            dir.clone(),
        );

        // RotateMasterKey 条目 → 钩子被调用且收到正确 kek（旧日志明文路径）
        let kek = vec![1u8; 32];
        let rotate = EntryOf {
            log_id: LogId {
                leader_id: LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 1,
            },
            payload: EntryPayload::Normal(dsh_core::command::Command::RotateMasterKey {
                kek: kek.clone(),
                kek_enc: vec![],
            }),
        };
        store
            .apply(stream::iter(entry_stream([rotate].to_vec())))
            .await
            .unwrap();
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "hook must fire exactly once for RotateMasterKey"
        );
        assert_eq!(
            *received.lock().unwrap(),
            vec![kek],
            "hook must receive the exact new KEK"
        );

        // 普通命令 → 钩子不再被调用
        let normal = EntryOf {
            log_id: LogId {
                leader_id: LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 2,
            },
            payload: EntryPayload::Normal(dsh_core::command::Command::SessionLogout),
        };
        store
            .apply(stream::iter(entry_stream([normal].to_vec())))
            .await
            .unwrap();
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "hook must NOT fire for non-rotate commands"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- P1 responder 错误路径（0.10 RaftStateMachine::apply 的 contract：
    // ApplyResponder::send 的必须 called，否则 client_write 永不 return） ----

    #[tokio::test]
    async fn apply_mid_stream_error_flushes_collected_entries() {
        // Stream 的 Err 即 apply 的 stop，已收集条目仍 responded（collected flush）+ Err return。
        // 0.9-style early `?` 即 dropped pending responders（client_write hang）。
        let dir = tmpdir("apply-err-mid");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));
        let mut store = StateMachineStore::new(sm.clone(), storage.raw_db(), dir.clone());

        let mut items = entry_stream([blank(1), blank(2)].to_vec());
        items.push(Err(io_write("injected mid-stream log read failure")));
        let r = store.apply(stream::iter(items)).await;
        assert!(r.is_err(), "mid-stream Err must propagate to openraft");

        // 前条目 flush 前 Err return：last_applied 即 2（no lost responder、no rollback）
        let last = store.read_last_applied().unwrap();
        assert!(
            last.is_some() && last.unwrap().index == 2,
            "collected entries must flush before Err"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn apply_last_applied_failure_responds_then_recovers() {
        // write_last_applied 的 IO 的 Err：该 entry's responder 已 get Err(...) resp（push 前 return）
        // + apply 的 Err；store 的 remain usable（replay 的 succeed）——0.9-style `?` 即 responder lost。
        let dir = tmpdir("apply-err-la");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));
        let mut store = StateMachineStore::new(sm.clone(), storage.raw_db(), dir.clone());

        store.set_fail_last_applied(1); // 第 1 write_last_applied 即 IO 的 Err
        let r = store
            .apply(stream::iter(entry_stream([blank(7)].to_vec())))
            .await;
        assert!(r.is_err(), "persist failure must surface as apply Err");
        assert!(
            store.read_last_applied().unwrap().is_none(),
            "failed persist must not record last_applied"
        );

        store.set_fail_last_applied(0); // 关闭注入
        store
            .apply(stream::iter(entry_stream([blank(7)].to_vec())))
            .await
            .unwrap();
        let last = store.read_last_applied().unwrap();
        assert!(
            last.is_some() && last.unwrap().index == 7,
            "replay after failure must succeed"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn legacy_inline_snapshot_key_migrates_to_file_on_read() {
        // 0.9/P1 即 inline `data` key（no file key）: first read must materialize the payload
        // file, rewrite the registry (meta + file key) and REMOVE the legacy data key.
        let dir = tmpdir("legacy-snap");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let db = storage.raw_db();
        let meta: SnapshotMetaOf<TypeConfig> = SnapshotMeta {
            last_log_id: Some(LogId {
                leader_id: LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 5,
            }),
            last_membership: StoredMembership::default(),
        };
        let data = serde_json::to_vec(&vec![(b"k".to_vec(), b"v".to_vec())]).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TBL_SNAPSHOTS).unwrap();
            t.insert(SNAP_META_KEY, ser(&meta).unwrap().as_slice())
                .unwrap();
            t.insert(SNAP_DATA_KEY, data.as_slice()).unwrap();
        }
        txn.commit().unwrap();

        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));
        let mut revived = StateMachineStore::new(sm.clone(), db.clone(), dir.clone());
        let snap = revived
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("legacy inline snapshot must load (migrated)");
        assert!(
            std::fs::exists(&snap.snapshot).unwrap_or(false),
            "payload file must be materialized"
        );
        assert_eq!(read_snapshot_payload(snap.snapshot).unwrap(), data);

        // registry rewritten exactly once: file key present, legacy data key gone
        let read_txn = db.begin_read().unwrap();
        let t = read_txn.open_table(TBL_SNAPSHOTS).unwrap();
        assert!(
            t.get(SNAP_FILE_KEY).unwrap().is_some(),
            "file key must exist post-migration"
        );
        assert!(
            t.get(SNAP_DATA_KEY).unwrap().is_none(),
            "legacy data key must be removed"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn install_snapshot_rejects_corrupt_payload_keeps_registry_untouched() {
        // install 即 validation-first: corrupt payload file 即 Err,registry untouched
        // (no half-install state, no registry entry pointing at bad payload).
        let dir = tmpdir("snap-bad");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));
        let mut store = StateMachineStore::new(sm.clone(), storage.raw_db(), dir.clone());
        let meta: SnapshotMetaOf<TypeConfig> = SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
        };
        let bad = write_snapshot_payload(dir.clone().join(SNAP_SUBDIR), &meta, b"not-json-bytes")
            .unwrap();
        let r = store.install_snapshot(&meta, bad).await;
        assert!(r.is_err(), "corrupt payload must be rejected");
        let cur = store.get_current_snapshot().await.unwrap();
        assert!(cur.is_none(), "rejected install must not touch registry");
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn snap_meta(index: u64) -> SnapshotMetaOf<TypeConfig> {
        SnapshotMeta {
            last_log_id: Some(LogId {
                leader_id: LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index,
            }),
            last_membership: StoredMembership::default(),
        }
    }

    fn snap_part_count(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir.join(SNAP_SUBDIR))
            .map(|it| {
                it.filter_map(|e| e.ok())
                    .filter(|e| e.file_name().to_string_lossy().ends_with(".part"))
                    .count()
            })
            .unwrap_or(0)
    }

    fn snap_snap_count(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir.join(SNAP_SUBDIR))
            .map(|it| {
                it.filter_map(|e| e.ok())
                    .filter(|e| e.file_name().to_string_lossy().ends_with(".snap"))
                    .count()
            })
            .unwrap_or(0)
    }

    /// P3d 回归（架构复核 blocker）：输入路径本身就是 registry 登记的 payload
    /// （openraft `restore_from_snapshot` 启动路径正是这样调用 install_snapshot）。
    /// 旧实现 rename「采纳」+ 随后 delete_old 会把刚登记的文件删掉 → 重启后没有快照。
    #[tokio::test]
    async fn reinstall_from_registry_path_keeps_payload_file() {
        let dir = tmpdir("snap-reinstall");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));
        let mut store = StateMachineStore::new(sm, storage.raw_db(), dir.clone());

        let data = serde_json::to_vec(&vec![(b"k".to_vec(), b"v".to_vec())]).unwrap();
        let meta = snap_meta(6);
        let payload = write_snapshot_payload(dir.join(SNAP_SUBDIR), &meta, &data).unwrap();
        store.install_snapshot(&meta, payload).await.unwrap();

        let snap = store
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("snapshot");
        assert!(std::fs::exists(&snap.snapshot).unwrap());

        // 第二次：输入 = registry 自己的路径
        store
            .install_snapshot(&snap.meta, snap.snapshot.clone())
            .await
            .unwrap();
        let snap2 = store
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("snapshot after reinstall");
        assert!(
            std::fs::exists(&snap2.snapshot).unwrap(),
            "registry 指向的 payload 必须存在（不能被 adopt/delete 掉）"
        );
        assert_eq!(snap_part_count(&dir), 0, "不得残留 .part");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 覆盖装：旧 registry 文件应被清理，只保留新的 canonical 文件，registry 指向它。
    /// 输入 payload 放在 staging 目录（生产中是传输层 materialize / sender 目录），
    /// store 只做 canonical copy，不删调用方的输入文件。
    #[tokio::test]
    async fn reinstall_over_existing_replaces_registry_and_files() {
        let dir = tmpdir("snap-replace");
        let _ = std::fs::remove_dir_all(&dir);
        let staging = dir.join("incoming");
        std::fs::create_dir_all(&staging).unwrap();

        let storage = open_storage(&dir);
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));
        let mut store = StateMachineStore::new(sm, storage.raw_db(), dir.clone());

        let m1 = snap_meta(3);
        let d1 = serde_json::to_vec(&vec![(b"a".to_vec(), b"1".to_vec())]).unwrap();
        let p1 = staging.join("payload-1.json");
        std::fs::write(&p1, &d1).unwrap();
        store.install_snapshot(&m1, p1.clone()).await.unwrap();
        assert!(
            std::fs::exists(&p1).unwrap(),
            "staging 输入不应被 store 删除"
        );

        let m2 = snap_meta(7);
        let d2 = serde_json::to_vec(&vec![(b"b".to_vec(), b"2".to_vec())]).unwrap();
        let p2 = staging.join("payload-2.json");
        std::fs::write(&p2, &d2).unwrap();
        store.install_snapshot(&m2, p2.clone()).await.unwrap();
        assert!(
            std::fs::exists(&p2).unwrap(),
            "staging 输入不应被 store 删除"
        );

        let snap = store
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("snapshot");
        assert_eq!(snap.meta.last_log_id, m2.last_log_id);
        assert!(std::fs::exists(&snap.snapshot).unwrap());
        let pairs: Vec<(Vec<u8>, Vec<u8>)> =
            serde_json::from_slice(&std::fs::read(&snap.snapshot).unwrap()).unwrap();
        assert_eq!(
            pairs,
            vec![(b"b".to_vec(), b"2".to_vec())],
            "registry 必须指向新内容"
        );
        assert_eq!(
            snap_snap_count(&dir),
            1,
            "旧 canonical 文件应被清理，只留 registry 那一份"
        );
        assert_eq!(snap_part_count(&dir), 0, "不得残留 .part");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
