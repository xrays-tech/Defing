//! Raft 日志/状态机/快照存储（模块 03 §5）。
//! - LogStore：raft-log / raft-meta 表（经 RedbStorage::raw_db）
#![allow(clippy::result_large_err, clippy::type_complexity)] // openraft IOFlushed/Responder 的 Err 变体较大（上游类型）

use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

// ---------------- P4-A restore-in-progress 标记 ----------------
//
// install_snapshot 的 restore 是**分块提交**（`Store::write_batch`，64MiB/块），崩溃/IO 错误
// 落在中间会留下「部分新 + 部分旧」的混合状态。P4-A 用 raft-meta 里的一个标记把窗口变成可重放：
//   1. payload 已 fsync 落到 canonical 文件后、动状态机之前写 `META_RESTORE_BEGIN{meta,payload}`；
//   2. registry 提交（+ 旧 payload 清理）之后删除标记；
//   3. boot 见到标记 → 用同一 payload 重放一次全量 restore（幂等），再清标记。
// 顺序不变量（设计 §7 A1/A4/A5）：
//   - 标记必须晚于 payload 的 fsync（copy_payload_file/write_payload_file 内部已 fsync 文件 + 目录）；
//   - 标记指向的 payload 就是 registry 即将指向的路径（同一个 `path`）；
//   - 启动清扫必须把 marker payload 并入 keep-set，否则会在重放前把它当 orphan 删掉（A1 blocker）。
const META_RESTORE_BEGIN: &[u8] = b"restore_begin";

/// `META_RESTORE_BEGIN` 的值（raft-meta 表，JSON；与 registry 同库同事务语义）。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RestoreBegin {
    /// 正在安装的快照元数据：重放后原样写回 last_applied / membership / registry。
    meta: SnapshotMetaOf<TypeConfig>,
    /// 已 fsync 的 canonical payload 文件（`{data_dir}/snapshots/` 下）。
    payload: PathBuf,
}

/// kill-switch：`DSH_RAFT_RESTORE_MARKER=0` **只关闭写 marker**；已存在的 marker 在 boot 时
/// 仍必须重放（设计 §7 A2）——否则会以混合状态启动。
const ENV_RESTORE_MARKER: &str = "DSH_RAFT_RESTORE_MARKER";
/// 唯一的「忽略已有 marker」逃生门，必须显式 `=1`/`true`（大声 WARN），供运维在 payload
/// 确认不可用时人工放行；默认关闭。
const ENV_RESTORE_FORCE: &str = "DSH_RAFT_RESTORE_FORCE";

fn env_true(name: &str) -> bool {
    matches!(
        std::env::var(name).as_deref(),
        Ok("1") | Ok("true") | Ok("yes") | Ok("on")
    )
}

fn restore_marker_enabled_from_env() -> bool {
    !matches!(
        std::env::var(ENV_RESTORE_MARKER).as_deref(),
        Ok("0") | Ok("false") | Ok("off") | Ok("no")
    )
}

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
/// - every canonical `*.snap` not referenced by the registry `file` key **or by an in-flight
///   restore marker** is removed (install/legacy-migration renames replace the previous payload;
///   old files are garbage);
/// - foreign names (not `*.part` / `*.snap`) are left untouched (operator debug drops).
///   Registry read/parse failure or a corrupt restore marker aborts the sweep (never mass-delete
///   on a corrupt read; P4-A: a marker we cannot parse means boot recovery will fail-fast, and
///   deleting its payload first would turn that into a bricked node).
///   Returns `(parts_removed, orphans_removed)`.
pub fn sweep_snapshot_payloads(db: &DbHandle, dir: PathBuf) -> Result<(u64, u64), ErrOf> {
    if !fs::exists(&dir).unwrap_or(false) {
        return Ok((0, 0));
    }
    // canonical referenced paths: the `file` registry key (JSON-encoded PathBuf) and, when an
    // install is in flight, the restore marker's payload (P4-A A1). `file` absent (no snapshot,
    // or legacy inline `data` not yet migrated) = no registry keep.
    let ref_raw = {
        let txn = db.begin_read().map_err(io_read)?;
        let table = txn.open_table(TBL_SNAPSHOTS).map_err(io_read)?;
        table
            .get(SNAP_FILE_KEY)
            .map_err(io_read)?
            .map(|guard| guard.value().to_vec())
    };
    let mut keep: Vec<PathBuf> = Vec::new();
    if let Some(raw) = ref_raw {
        keep.push(de(&raw)?);
    }
    if let Some(raw) = meta_get(db, META_RESTORE_BEGIN)? {
        match de::<RestoreBegin>(&raw) {
            Ok(marker) => keep.push(marker.payload),
            Err(e) => {
                return Err(io_read(format!(
                    "restore marker is corrupt ({e}); refusing to sweep the snapshots dir \
                     (boot recovery must fail-fast instead of losing the marker payload)"
                )))
            }
        }
    }
    let keep_canon: Vec<PathBuf> = keep
        .iter()
        .filter_map(|p| match fs::canonicalize(p) {
            Ok(c) => Some(c),
            Err(_) => {
                // missing payload is recover's problem (fail-fast), not the sweep's: nothing to keep.
                tracing::warn!("sweep keep-set path missing: {}", p.display());
                None
            }
        })
        .collect();
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
                    // P3f：按 canonicalize 后比较，而不是 display() 字符串。`--data-dir data`
                    // 与 `./data` / 绝对路径只是拼写不同，字符串比较会把 registry 正在使用的
                    // payload 判成 orphan 删掉（重启后没有快照）。
                    let is_keep = match fs::canonicalize(&path) {
                        Ok(p) => keep_canon.contains(&p),
                        // canonicalize 失败（罕见：权限/竞态）→ 退回按原始路径字符串比较
                        Err(_) => keep.contains(&path),
                    };
                    if is_keep {
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
        Ok(()) => match fs::File::open(&part_path).and_then(|f| f.sync_all()) {
            Err(e) => Err(io_write(e)),
            Ok(()) => match fs::rename(&part_path, &final_path) {
                Err(e) => Err(io_write(e)),
                Ok(()) => fsync_dir(&dir),
            },
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
/// P4a1：payload 头（读端兼容层）。
/// 布局（8B）：`MAGIC(6B)="DSHSNP" | version(u8) | flags(u8)`。
/// version=1 表示 body 仍是 JSON（即现状）；写端在 P4a2 之前**不写头**，读端两者都接受。
/// P4a2 起 version=2 = msgpack body（见 dev_docs/design/openraft-010-p4-payload-msgpack.md §7）。
pub const PAYLOAD_MAGIC: &[u8; 6] = b"DSHSNP";
pub const PAYLOAD_HEADER_LEN: usize = 8;
const PAYLOAD_VERSION_JSON: u8 = 1;

fn read_payload_pairs(path: &std::path::Path) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ErrOf> {
    use std::io::Read as _;
    let mut f = fs::File::open(path).map_err(io_read)?;
    let mut head = [0u8; PAYLOAD_HEADER_LEN];
    let mut n = 0usize;
    while n < head.len() {
        match f.read(&mut head[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) => return Err(io_read(e)),
        }
    }
    if n == PAYLOAD_HEADER_LEN && &head[..PAYLOAD_MAGIC.len()] == PAYLOAD_MAGIC {
        let version = head[PAYLOAD_MAGIC.len()];
        if version != PAYLOAD_VERSION_JSON {
            return Err(io_read(format!(
                "unsupported snapshot payload version {version}（本 binary 只认 v1=JSON；P4a2 才写 v2）"
            )));
        }
        return serde_json::from_reader(std::io::BufReader::new(f)).map_err(io_read);
    }
    // 无头（旧 payload）：把已读到的前缀接回去，从字节 0 解析。
    let prefix = std::io::Cursor::new(head[..n].to_vec());
    serde_json::from_reader(std::io::BufReader::new(prefix.chain(f))).map_err(io_read)
}

/// P3c：把收到的 payload 落成本节点 canonical snapshot 文件。
///
/// 用**文件级 copy**（`fs::copy`，内核零拷贝路径），不做整份 `Vec<u8>` 中转，也不 rename
/// 输入文件。原因是输入可能是本节点 registry 自己的 payload（openraft `restore_from_snapshot`
/// 启动路径会把 registry 路径直接传给 `install_snapshot`），rename 采纳会把 registry 指向的
/// 文件移走，再叠加 `delete_old_snapshot_file` 就直接删掉刚登记的快照；copy 失败时输入
/// 保持原样，registry 仍指向可用文件。名字带进程 nonce + 自增 seq（见 `snap_file_name`），
/// 不会覆盖既有 canonical 文件。
/// rename 之后 fsync 目录，保证「registry 指向新文件」在掉电后仍成立。
/// 某些平台/文件系统不允许打开目录：降级为 warn（不阻断安装），但记录。
fn fsync_dir(dir: &std::path::Path) -> Result<(), ErrOf> {
    match fs::File::open(dir) {
        Ok(f) => f.sync_all().map_err(io_write),
        Err(e) => {
            tracing::warn!("fsync snapshots dir {}: {e}", dir.display());
            Ok(())
        }
    }
}

fn copy_payload_file(dir: PathBuf, name: String, src: &std::path::Path) -> Result<PathBuf, ErrOf> {
    fs::create_dir_all(&dir).map_err(io_write)?;
    let part_path = dir.join(format!("{name}.part"));
    let final_path = dir.join(&name);
    let _ = fs::remove_file(&part_path);
    if let Err(e) = fs::copy(src, &part_path) {
        let _ = fs::remove_file(&part_path);
        return Err(io_read(e));
    }
    // 先 fsync 数据再 rename，最后 fsync 目录：避免「registry 已提交、payload 却是截断/未落盘」
    // （复核 major：power loss 下 registry→truncated payload）。
    fs::File::open(&part_path)
        .and_then(|f| f.sync_all())
        .map_err(io_write)?;
    match fs::rename(&part_path, &final_path) {
        Ok(()) => {
            fsync_dir(&dir)?;
            Ok(final_path)
        }
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
/// 删除一个 meta 键（幂等）。P4-A 的 `META_RESTORE_BEGIN` 生命周期用得到。
fn meta_del(db: &DbHandle, key: &[u8]) -> Result<(), ErrOf> {
    let txn = db.begin_write().map_err(io_write)?;
    {
        let mut table = txn.open_table(TBL_RAFT_META).map_err(io_write)?;
        table.remove(key).map_err(io_write)?;
    }
    txn.commit().map_err(io_write)
}

fn meta_put(db: &DbHandle, key: &[u8], value: &[u8]) -> Result<(), ErrOf> {
    let txn = db.begin_write().map_err(io_write)?;
    let mut table = txn.open_table(TBL_RAFT_META).map_err(io_write)?;
    table.insert(key, value).map_err(io_write)?;
    drop(table); // 表句柄先于 commit 释放（commit 消费事务）
    txn.commit().map_err(io_write)
}

/// P4-A：last_applied / membership 的自由函数形态——`install_snapshot` 与 boot 重放
/// （`recover_interrupted_restore`，没有 StateMachineStore 实例）共用同一份落盘语义。
fn put_last_applied(db: &DbHandle, id: &LogIdOf) -> Result<(), ErrOf> {
    meta_put(db, META_LAST_APPLIED, &ser(id)?)
}

fn put_membership(db: &DbHandle, m: &StoredMembershipOf<TypeConfig>) -> Result<(), ErrOf> {
    meta_put(db, META_MEMBERSHIP, &ser(m)?)
}

/// 共享的日志读取逻辑（LogReader 与 LogStore 复用）：[start, end] 闭区间。
fn read_entries(db: &DbHandle, start: u64, end: u64) -> Result<Vec<EntryOf>, ErrOf> {
    let txn = db.begin_read().map_err(io_read)?;
    let table = txn.open_table(TBL_RAFT_LOG).map_err(io_read)?;
    let start_key = log_key(start);
    let mut out: Vec<EntryOf> = Vec::new();
    // last_purged 与日志 range 必须同一个 read txn：分两个 txn 时，purge 在两者之间提交会让
    // expected 落后于日志快照，误报 "log hole"（openraft 会把读日志失败升级为 Fatal::StorageError）。
    let first_expected = {
        let mt = txn.open_table(TBL_RAFT_META).map_err(io_read)?;
        let purged = match mt.get(META_LAST_PURGED).map_err(io_read)? {
            None => None,
            Some(g) => {
                let raw = g.value().to_vec();
                Some(match de::<LogIdOf>(&raw) {
                    Ok(id) => id.index,
                    Err(_) => de::<u64>(&raw)?,
                })
            }
        };
        purged.map(|v| v + 1).unwrap_or(start).max(start)
    };
    let mut expected: Option<u64> = Some(first_expected);
    for row in table.range(start_key.as_slice()..).map_err(io_read)? {
        let (k, v) = row.map_err(io_read)?;
        let idx = log_index_from_key(k.value());
        if idx > end {
            break;
        }
        // P3f：openraft 契约要求中间空洞报错（只有 purge 造成的前缀缺失可容忍）。
        if let Some(exp) = expected {
            if idx != exp {
                return Err(io_read(format!(
                    "log store hole: expected index {exp}, found {idx}"
                )));
            }
        }
        expected = Some(idx + 1);
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
    let mut out: Vec<EntryOf> = Vec::new();
    let mut acc = 0u64;
    let first_expected = {
        let mt = txn.open_table(TBL_RAFT_META).map_err(io_read)?;
        let purged = match mt.get(META_LAST_PURGED).map_err(io_read)? {
            None => None,
            Some(g) => {
                let raw = g.value().to_vec();
                Some(match de::<LogIdOf>(&raw) {
                    Ok(id) => id.index,
                    Err(_) => de::<u64>(&raw)?,
                })
            }
        };
        purged.map(|v| v + 1).unwrap_or(start).max(start)
    };
    let mut expected: Option<u64> = Some(first_expected);
    for row in table.range(start_key.as_slice()..).map_err(io_read)? {
        let (k, v) = row.map_err(io_read)?;
        let idx = log_index_from_key(k.value());
        if idx > end {
            break;
        }
        // P3f：中间空洞必须报错（短读只能由 cap 触发，不能是缺失）。
        if let Some(exp) = expected {
            if idx != exp {
                return Err(io_read(format!(
                    "log store hole: expected index {exp}, found {idx}"
                )));
            }
        }
        expected = Some(idx + 1);
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
        let last_applied: Option<LogIdOf> = meta_get(&self.db, META_LAST_APPLIED)?
            .map(|raw| de(&raw))
            .transpose()?;
        let last_purged: Option<LogIdOf> = match meta_get(&self.db, META_LAST_PURGED)? {
            None => None,
            Some(raw) => match de::<LogIdOf>(&raw) {
                Ok(id) => Some(id),
                Err(_) => {
                    // 兼容旧格式（只存 index）：同 index 时直接用 last_applied 的 LogId，
                    // 保证「日志清空 + purge 到 last_applied」这一生产状态在 0.10 下可启动。
                    let idx = de::<u64>(&raw)?;
                    match last_applied {
                        Some(a) if a.index == idx => Some(a),
                        _ => Some(LogId {
                            leader_id: LeaderId {
                                term: 0,
                                node_id: NodeId::MAX,
                            },
                            index: idx,
                        }),
                    }
                }
            },
        };
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
        // 存**完整 LogId**（旧格式只存 index）：openraft 的 get_initial_state 除了
        // last_log_id >= last_purged_log_id，还比较 last_log_id 与 last_applied 的 LogId 顺序；
        // 只存 index 会合成 T0-NMAX.N，与 install 写入的 last_applied=T4-N5.N 同 index 但更小
        // → 仍判 "inverted log order"，快照追平后的节点重启起不来。
        meta_table
            .insert(META_LAST_PURGED, ser(&log_id)?.as_slice())
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
    /// P4-A kill-switch：是否写 `META_RESTORE_BEGIN`（`DSH_RAFT_RESTORE_MARKER=0` 只关写）。
    restore_marker: Arc<AtomicBool>,
    /// P4-A A3 fail-stop：restore 失败（非进程死亡）后置位。置位期间 apply / 快照构建 /
    /// `get_current_snapshot` 全部拒绝，dsh-api 据此对 HTTP 读写返回 503——不能带着
    /// 「部分新 + 部分旧」的混合状态继续服务。解除方式：一次完整成功的 install_snapshot，
    /// 或重启走 boot 重放。
    poisoned: Arc<AtomicBool>,
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
        let restore_marker = restore_marker_enabled_from_env();
        if !restore_marker {
            tracing::warn!(
                "DSH_RAFT_RESTORE_MARKER disabled: snapshot installs will NOT write the \
                 restore-in-progress marker, so a crash mid-restore cannot be replayed \
                 (release-period escape hatch only). Existing markers are still replayed on boot."
            );
        }
        Self {
            sm,
            db,
            data_dir,
            current_snapshot: Arc::new(tokio::sync::Mutex::new(None)),
            events: tx,
            rotation_hook: hook,
            fail_last_applied_at: Arc::new(AtomicU64::new(NO_FAULT)),
            last_applied_calls: Arc::new(AtomicU64::new(0)),
            restore_marker: Arc::new(AtomicBool::new(restore_marker)),
            poisoned: Arc::new(AtomicBool::new(false)),
        }
    }

    /// P4-A kill-switch 的进程内覆盖（测试用；生产走 `DSH_RAFT_RESTORE_MARKER`）。
    #[doc(hidden)]
    pub fn set_restore_marker(&self, enabled: bool) {
        self.restore_marker.store(enabled, Ordering::Relaxed);
    }

    /// P4-A：fail-stop 标记句柄（dsh-cli 交给 dsh-api，置位后 HTTP 读写 503）。
    pub fn poison_flag(&self) -> Arc<AtomicBool> {
        self.poisoned.clone()
    }

    /// P4-A：restore 失败后是否处于 fail-stop。
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::SeqCst)
    }

    /// P4-A A3：restore 错误 → 进入 fail-stop（只记第一次，避免刷日志）。
    fn poison(&self, why: impl std::fmt::Display) {
        if !self.poisoned.swap(true, Ordering::SeqCst) {
            tracing::error!(
                "state machine POISONED (fail-stop): {why}. Refusing apply/snapshot/read paths \
                 until a full snapshot install succeeds or the node restarts (boot replay)."
            );
        }
    }

    /// P4-A：install_snapshot 的「状态机已可能被改写」区段（标记已写之后）。
    /// 返回被替换的旧 registry 文件路径（交给调用方在提交后清理）。
    fn restore_and_persist_inner(
        &self,
        meta: &SnapshotMetaOf<TypeConfig>,
        pairs: &[(Vec<u8>, Vec<u8>)],
        path: &std::path::Path,
    ) -> Result<Option<PathBuf>, ErrOf> {
        {
            let sm = self.sm.write().map_err(|e| io_write(e.to_string()))?;
            sm.restore_all(pairs).map_err(storage_err)?;
        }
        if let Some(last_log_id) = &meta.last_log_id {
            self.write_last_applied(last_log_id)?;
        }
        self.write_membership(&meta.last_membership)?;
        persist_snapshot_registry(&self.db, meta, path.to_path_buf())
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
        put_last_applied(&self.db, id)
    }

    fn read_membership(&self) -> Result<StoredMembershipOf<TypeConfig>, ErrOf> {
        match meta_get(&self.db, META_MEMBERSHIP)? {
            Some(raw) => de(&raw),
            None => Ok(StoredMembership::default()),
        }
    }

    fn write_membership(&self, m: &StoredMembershipOf<TypeConfig>) -> Result<(), ErrOf> {
        put_membership(&self.db, m)
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
        if self.inner.is_poisoned() {
            return Err(io_write(
                "state machine poisoned: refusing to build a snapshot from a mixed state",
            ));
        }
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
        // P4-A A3：fail-stop。restore 已失败 → 本地状态机可能是「部分新 + 部分旧」，
        // 继续 apply 会把日志条目叠在混合状态上（静默分叉）。这里必须拒绝，但 openraft
        // 已为条目创建 responder（P1 教训：不 respond 会让 client_write 永久挂起）——
        // 先把整条流 drain 并逐条 respond Err，再返回 Err。
        if self.is_poisoned() {
            const WHY: &str =
                "state machine poisoned: a snapshot restore failed; restart to replay";
            while let Some(item) = entries.next().await {
                match item {
                    Ok((_, Some(responder))) => {
                        responder.send(Err(DshError::internal(WHY)));
                    }
                    Ok((_, None)) => {}
                    Err(e) => tracing::warn!("poisoned apply drain stream error: {e}"),
                }
            }
            return Err(io_write(WHY));
        }
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
                restore_marker: self.restore_marker.clone(),
                poisoned: self.poisoned.clone(),
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
        // P4-A：payload 已 fsync（copy_payload_file 内含文件 + 目录 fsync）、restore 之前写标记。
        // 单事务（Durability=Immediate）→ 崩溃点可判定：见 META_RESTORE_BEGIN 注释。
        // 标记写失败时状态机未动，直接返回 Err（不 poison）。
        if self.restore_marker.load(Ordering::Relaxed) {
            let marker = RestoreBegin {
                meta: meta.clone(),
                payload: path.clone(),
            };
            meta_put(&self.db, META_RESTORE_BEGIN, &ser(&marker)?)?;
        }
        // 从这里开始状态机可能被改写：任何错误都必须 fail-stop（A3），
        // 带着「部分新 + 部分旧」继续 apply / 对外读会静默分叉。
        let old = match self.restore_and_persist_inner(meta, &pairs, path.as_path()) {
            Ok(old) => old,
            Err(e) => {
                self.poison(format!("install_snapshot restore failed: {e}"));
                return Err(e);
            }
        };
        // P3d：old 与 path 相同时**不能删**（名字复用/同名 meta 会命中，删掉就等于删掉刚登记的快照）
        if old.as_deref() != Some(path.as_path()) {
            delete_old_snapshot_file(old);
        }
        // registry 已提交：标记删除是幂等收尾。清理失败只 warn——boot 重放同一 payload 是幂等的。
        if let Err(e) = meta_del(&self.db, META_RESTORE_BEGIN) {
            tracing::warn!("clear restore marker after successful install: {e}");
        }
        // 一次完整成功的 install 已经把状态机整体替换成 payload 内容 → 解除 fail-stop。
        self.poisoned.store(false, Ordering::SeqCst);
        *self.current_snapshot.lock().await = Some((meta.clone(), path));
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<TypeConfig, PathBuf>>, ErrOf> {
        if self.is_poisoned() {
            return Err(io_read(
                "state machine poisoned: snapshot restore failed; restart to replay",
            ));
        }
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

/// P4-A：boot 期重放被中断的快照安装。
///
/// 返回 `Some(meta)` = 确实重放过一次（调用方应记日志）；`None` = 没有残留标记。
///
/// 顺序固定（设计 §7 A1/A4/A5，store 构造时的 sweep 已把 marker payload 并入 keep-set）：
/// 读 marker → 校验 payload 存在且可解析 → 全量 restore（分块写，幂等替换）→
/// 写 last_applied / membership → registry 指向同一 payload（+ 清理被替换的旧文件）→ 清 marker →
/// 末端再清扫一次（此时 registry 与 marker 都指向该 payload，其余 canonical 文件是孤儿）。
///
/// 失败语义：marker 损坏、payload 缺失/不可解析、重放写失败 → **Err**，调用方必须拒绝启动
/// （fail-fast）。静默降级成「无快照」会丢数据；自动向 leader 要快照需要网络与成员表，超出
/// boot 期职责。
pub fn recover_interrupted_restore(
    sm: &Arc<RwLock<StateMachine>>,
    db: &DbHandle,
    data_dir: PathBuf,
) -> Result<Option<SnapshotMetaOf<TypeConfig>>, ErrOf> {
    recover_interrupted_restore_with(sm, db, data_dir, env_true(ENV_RESTORE_FORCE))
}

/// `recover_interrupted_restore` 的显式参数版本（单测用；`force_ignore` 对应
/// `DSH_RAFT_RESTORE_FORCE=1` 的人工逃生门）。
#[doc(hidden)]
pub fn recover_interrupted_restore_with(
    sm: &Arc<RwLock<StateMachine>>,
    db: &DbHandle,
    data_dir: PathBuf,
    force_ignore: bool,
) -> Result<Option<SnapshotMetaOf<TypeConfig>>, ErrOf> {
    let Some(raw) = meta_get(db, META_RESTORE_BEGIN)? else {
        return Ok(None);
    };
    let marker: RestoreBegin = match de(&raw) {
        Ok(m) => m,
        Err(e) => {
            return Err(io_read(format!(
                "interrupted snapshot restore marker is corrupt ({e}); refusing to start \
                 (mixed state). Restore the node from backup, or clear the data volume to rejoin."
            )))
        }
    };
    if force_ignore {
        // 唯一允许「忽略 marker」的路径：运维确认 payload 不可用且接受混合状态。
        tracing::error!(
            "DSH_RAFT_RESTORE_FORCE set: IGNORING interrupted-restore marker \
             (last_log_id={:?}, payload={}). State machine may be a MIX of old and new data; \
             consistency must be verified manually.",
            marker.meta.last_log_id,
            marker.payload.display()
        );
        meta_del(db, META_RESTORE_BEGIN)?;
        return Ok(None);
    }
    if !fs::exists(&marker.payload).unwrap_or(false) {
        return Err(io_read(format!(
            "interrupted snapshot restore: payload {} is missing; refusing to start. \
             Restore the node from backup, or clear the data volume to rejoin.",
            marker.payload.display()
        )));
    }
    let pairs = read_payload_pairs(&marker.payload).map_err(|e| {
        io_read(format!(
            "interrupted snapshot restore: payload {} is unreadable/corrupt ({e}); refusing to start.",
            marker.payload.display()
        ))
    })?;
    let started = std::time::Instant::now();
    {
        let sm = sm
            .write()
            .map_err(|e| io_write(format!("state lock unavailable: {e}")))?;
        sm.restore_all(&pairs).map_err(storage_err)?;
    }
    if let Some(last_log_id) = &marker.meta.last_log_id {
        put_last_applied(db, last_log_id)?;
    }
    put_membership(db, &marker.meta.last_membership)?;
    let old = persist_snapshot_registry(db, &marker.meta, marker.payload.clone())?;
    if old.as_deref() != Some(marker.payload.as_path()) {
        delete_old_snapshot_file(old);
    }
    meta_del(db, META_RESTORE_BEGIN)?;
    tracing::warn!(
        "restore replayed on boot: last_log_id={:?} payload={} pairs={} elapsed={:?}",
        marker.meta.last_log_id,
        marker.payload.display(),
        pairs.len(),
        started.elapsed()
    );
    // 设计 §7 A1 的最后一步：registry 已指向该 payload，清扫其余 canonical 残留。
    // 清扫失败不阻断启动（registry/marker 都已一致；残留文件下次 boot 再清）。
    if let Err(e) = sweep_snapshot_payloads(db, data_dir.join(SNAP_SUBDIR)) {
        tracing::warn!("post-recovery snapshots sweep skipped: {e}");
    }
    Ok(Some(marker.meta))
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
    pub(super) fn entry_stream(
        entries: Vec<EntryOf>,
    ) -> Vec<Result<(EntryOf, Option<ApplyResponder<TypeConfig>>), ErrOf>> {
        entries.into_iter().map(|e| Ok((e, None))).collect()
    }

    pub(super) fn tmpdir(tag: &str) -> std::path::PathBuf {
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!("dsh-raft-store-{tag}-{}-{n}", std::process::id()))
    }

    pub(super) fn open_storage(dir: &std::path::Path) -> RedbStorage {
        RedbStorage::open(&dir.display().to_string()).unwrap()
    }

    pub(super) fn blank(index: u64) -> EntryOf {
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
        // 落盘格式必须是**完整 LogId**（term/node 不能丢）：只存 index 时，快照追平后的
        // 空日志状态会合成 T0-NMAX.N，与 last_applied 同 index 但更小 → 0.10 判 inverted。
        let raw = meta_get(&storage.raw_db(), META_LAST_PURGED)
            .unwrap()
            .expect("last_purged persisted");
        let stored: LogIdOf = de(&raw).expect("last_purged 必须是完整 LogId 格式");
        assert_eq!(stored.index, 3);
        assert_eq!(
            stored.leader_id.term, 1,
            "term 必须保留（index-only 会丢成 0）"
        );
        assert_eq!(stored.leader_id.node_id, 1, "node_id 必须保留");
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
            // 生产状态是「install 设了 last_applied → openraft purge 到快照 index → 日志清空」。
            // 复核 blocker：只断言 purge 而不设 last_applied 测不到 openraft 的第二个 inverted 检查
            // （last_log_id 与 last_applied 的 LogId 顺序）。
            meta_put(&store.db, META_LAST_APPLIED, &ser(&truncate_at(3)).unwrap()).unwrap();
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

    pub(super) fn snap_meta(index: u64) -> SnapshotMetaOf<TypeConfig> {
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

    /// 架构复核 blocker 的兼容面：旧数据目录里 `last_purged` 只存了 index（无 leader_id）。
    /// 同 index 且已有 `last_applied` 时必须归一成 last_applied 的 LogId，否则 0.10 仍判
    /// inverted（综合成 T0-NMAX.N < Tx-Ny.N）→ 节点重启起不来。
    #[tokio::test]
    async fn legacy_index_only_purged_normalizes_to_last_applied_and_boots() {
        let dir = tmpdir("legacy-purged");
        let _ = std::fs::remove_dir_all(&dir);
        {
            let storage = open_storage(&dir);
            let db = storage.raw_db();
            let mut store = LogStore::new(db.clone());
            store
                .blocking_append([blank(1), blank(2), blank(3)])
                .await
                .unwrap();
            meta_put(&db, META_LAST_APPLIED, &ser(&truncate_at(3)).unwrap()).unwrap();
            // 手工写回「旧格式：只有 index」
            meta_put(&db, META_LAST_PURGED, &ser(&3u64).unwrap()).unwrap();
            // 清空日志（模拟 purge 后的空日志）
            store.purge(truncate_at(3)).await.unwrap();
            // 再写回旧格式（purge 现在会写完整 LogId）
            meta_put(&db, META_LAST_PURGED, &ser(&3u64).unwrap()).unwrap();
        }

        let storage = open_storage(&dir);
        let db = storage.raw_db();
        let mut store = LogStore::new(db.clone());
        let st = store.get_log_state().await.unwrap();
        let purged = st.last_purged_log_id.expect("purged");
        let applied: LogIdOf = meta_get(&db, META_LAST_APPLIED)
            .unwrap()
            .map(|raw| de(&raw).unwrap())
            .expect("last_applied");
        assert_eq!(
            purged, applied,
            "旧格式同 index 必须归一成 last_applied 的 LogId"
        );
        assert_eq!(
            st.last_log_id,
            Some(applied),
            "空日志回落值必须与 last_applied 同序"
        );

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
        .expect("旧格式 purged + 空日志必须能在 0.10 上启动");
        let _ = raft.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
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

    /// P3f 回归：registry 正在使用的 payload 不能被启动清扫删掉。
    /// 复核场景：`--data-dir data` / `./data` / 绝对路径 拼写不同 → 旧实现用 display()
    /// 字符串比较，把同一文件判成 orphan；这里用带 `.` 的非规范拼写扫同一目录。
    #[tokio::test]
    async fn sweep_keeps_registry_payload_under_noncanonical_dir_spelling() {
        let dir = tmpdir("sweep-canon");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));
        let mut store = StateMachineStore::new(sm, storage.raw_db(), dir.clone());

        let meta = snap_meta(9);
        let data = serde_json::to_vec(&vec![(b"k".to_vec(), b"v".to_vec())]).unwrap();
        // 输入放 staging：snapshots 目录里只留 registry 的 canonical 文件，
        // 否则「输入 payload 是 orphan」会混进本测试的断言。
        let staging = dir.join("incoming");
        std::fs::create_dir_all(&staging).unwrap();
        let payload = staging.join("payload.json");
        std::fs::write(&payload, &data).unwrap();
        store.install_snapshot(&meta, payload).await.unwrap();
        let snap = store
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("snapshot");

        // 非规范拼写扫同一目录（read_dir 出来的路径带 "./"）
        let (parts, orphans) =
            sweep_snapshot_payloads(&storage.raw_db(), dir.join("snapshots/.")).unwrap();
        assert_eq!(
            (parts, orphans),
            (0, 0),
            "registry 正在用的 payload 不能被当成 orphan 删掉"
        );
        assert!(
            std::fs::exists(&snap.snapshot).unwrap(),
            "registry 指向的 payload 必须仍然存在"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// P3f 回归：日志中间空洞必须报错（openraft 契约），不能静默跳过。
    #[tokio::test]
    async fn read_entries_rejects_interior_hole() {
        let dir = tmpdir("log-hole");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let db = storage.raw_db();
        let mut store = LogStore::new(db.clone());
        store
            .blocking_append([blank(1), blank(2), blank(3)])
            .await
            .unwrap();
        // 人为删掉中间 index=2 的日志行（模拟存储损坏/外部篡改）
        {
            let txn = db.begin_write().unwrap();
            {
                let mut t = txn.open_table(TBL_RAFT_LOG).unwrap();
                t.remove(log_key(2).as_slice()).unwrap();
            }
            txn.commit().unwrap();
        }
        let got = store.try_get_log_entries(1..=3).await;
        assert!(
            got.is_err(),
            "中间空洞必须报错，实际返回 {:?}",
            got.map(|v| v.len())
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 复核 minor 回归：空洞检测必须把「purged 前缀」算进期望起点。
    /// 调用方从已 purge 的 index 开始读是合法的；但 purged 之后一旦缺 index 就必须报错。
    #[tokio::test]
    async fn hole_detection_accounts_for_purged_prefix() {
        let dir = tmpdir("hole-purged");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let db = storage.raw_db();
        let mut store = LogStore::new(db.clone());
        store.blocking_append((1..=5).map(blank)).await.unwrap();
        store.purge(truncate_at(2)).await.unwrap();
        // 从 1 开始读（1、2 已 purge）→ 合法，返回 3、4、5
        assert_eq!(indexes_of(&mut store, 1..=5).await, vec![3, 4, 5]);
        // 人为删掉 4 → 3 之后的空洞必须报错（不能因起点 1 != 3 而误报/漏报）
        {
            let txn = db.begin_write().unwrap();
            {
                let mut t = txn.open_table(TBL_RAFT_LOG).unwrap();
                t.remove(log_key(4).as_slice()).unwrap();
            }
            txn.commit().unwrap();
        }
        let r = store.try_get_log_entries(1..=5).await;
        assert!(
            r.is_err(),
            "purged 前缀之后的空洞必须报错，实际 {:?}",
            r.map(|v| v.len())
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// P4a1：读端必须同时接受「无头旧 payload」与「带 MAGIC 头的 v1 payload」，
    /// 并对未知 version 明确报错（不能静默按 JSON 解析）。
    #[tokio::test]
    async fn payload_reader_accepts_legacy_and_magic_json_but_rejects_unknown_version() {
        let dir = tmpdir("payload-header");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));
        let store = StateMachineStore::new(sm, storage.raw_db(), dir.clone());
        let pairs = vec![(b"k".to_vec(), b"v".to_vec())];
        let body = serde_json::to_vec(&pairs).unwrap();

        // 1) 无头（旧格式）可读
        let legacy = dir.join("legacy.json");
        std::fs::write(&legacy, &body).unwrap();
        assert_eq!(read_payload_pairs(&legacy).unwrap(), pairs);

        // 2) 带 MAGIC 头 + version=1 可读
        let mut framed = Vec::new();
        framed.extend_from_slice(PAYLOAD_MAGIC);
        framed.push(PAYLOAD_VERSION_JSON);
        framed.push(0);
        framed.extend_from_slice(&body);
        let with_head = dir.join("framed.bin");
        std::fs::write(&with_head, &framed).unwrap();
        assert_eq!(read_payload_pairs(&with_head).unwrap(), pairs);

        // 3) 未知版本必须报错
        let mut bad = framed.clone();
        bad[PAYLOAD_MAGIC.len()] = 9;
        let bad_path = dir.join("v9.bin");
        std::fs::write(&bad_path, &bad).unwrap();
        assert!(
            read_payload_pairs(&bad_path).is_err(),
            "未知 payload 版本必须报错"
        );

        // 4) 用带头 payload 走完整 install（另一份 store 实例）
        let meta = snap_meta(11);
        let storage2 = open_storage(&dir.join("n2"));
        let sm2 = Arc::new(RwLock::new(StateMachine::new(Box::new(storage2.clone()))));
        let mut store2 = StateMachineStore::new(sm2, storage2.raw_db(), dir.join("n2"));
        store2.install_snapshot(&meta, with_head).await.unwrap();
        let snap = store2
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("snapshot");
        assert!(std::fs::exists(&snap.snapshot).unwrap());
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// meta_del：写后删必须读回 None，且重复删除幂等（P4-A marker 生命周期依赖它）。
    #[tokio::test]
    async fn meta_del_removes_key_and_is_idempotent() {
        let dir = tmpdir("meta-del");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let db = storage.raw_db();
        meta_put(&db, b"k", b"v").unwrap();
        assert_eq!(meta_get(&db, b"k").unwrap().as_deref(), Some(&b"v"[..]));
        meta_del(&db, b"k").unwrap();
        assert!(meta_get(&db, b"k").unwrap().is_none());
        meta_del(&db, b"k").unwrap(); // 幂等
        let _ = std::fs::remove_dir_all(&dir);
    }
}

// ---------------- P4-A：restore-in-progress 标记 + boot 重放 + fail-stop ----------------

#[cfg(test)]
mod p4a_tests {
    use super::tests::{blank, entry_stream, open_storage, snap_meta, tmpdir};
    use super::*;
    use futures_util::stream;

    fn pairs_of(items: &[(&str, &str)]) -> Vec<(Vec<u8>, Vec<u8>)> {
        items
            .iter()
            .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
            .collect()
    }

    fn json_payload(pairs: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
        serde_json::to_vec(&pairs).unwrap()
    }

    fn write_marker(db: &DbHandle, meta: &SnapshotMetaOf<TypeConfig>, payload: &std::path::Path) {
        let marker = RestoreBegin {
            meta: meta.clone(),
            payload: payload.to_path_buf(),
        };
        meta_put(db, META_RESTORE_BEGIN, &ser(&marker).unwrap()).unwrap();
    }

    fn state_pairs(sm: &Arc<RwLock<StateMachine>>) -> Vec<(Vec<u8>, Vec<u8>)> {
        sm.read().unwrap().dump_all().unwrap()
    }

    /// 交接测试 #1：marker 存在时，启动清扫必须保留其 payload；去掉 marker 后同一文件
    /// 才算 orphan（证明「保留」确实来自 marker keep-set，而不是清扫失效）。
    #[tokio::test]
    async fn sweep_keeps_restore_marker_payload() {
        let dir = tmpdir("p4a-sweep-marker");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let db = storage.raw_db();
        let snap_dir = dir.join(SNAP_SUBDIR);

        let meta = snap_meta(4);
        let data = json_payload(&pairs_of(&[("k", "v")]));
        let payload = write_snapshot_payload(snap_dir.clone(), &meta, &data).unwrap();
        let orphan = write_snapshot_payload(snap_dir.clone(), &snap_meta(3), &data).unwrap();
        write_marker(&db, &meta, &payload);

        let (parts, orphans) = sweep_snapshot_payloads(&db, snap_dir.clone()).unwrap();
        assert_eq!((parts, orphans), (0, 1), "只应删掉真正的 orphan");
        assert!(fs::exists(&payload).unwrap(), "marker payload 必须保留");
        assert!(!fs::exists(&orphan).unwrap());

        // 反证：清掉 marker 再扫，同一 payload 变成 orphan
        meta_del(&db, META_RESTORE_BEGIN).unwrap();
        let (_, orphans2) = sweep_snapshot_payloads(&db, snap_dir.clone()).unwrap();
        assert_eq!(orphans2, 1);
        assert!(
            !fs::exists(&payload).unwrap(),
            "无 marker 时 payload 是 orphan"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 坏 marker：清扫必须 Err 且**不删任何文件**（不能把待重放 payload 当 orphan 删掉）；
    /// boot 重放同样 Err（fail-fast）。
    #[tokio::test]
    async fn corrupt_restore_marker_aborts_sweep_and_recovery() {
        let dir = tmpdir("p4a-corrupt-marker");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let db = storage.raw_db();
        let snap_dir = dir.join(SNAP_SUBDIR);
        fs::create_dir_all(&snap_dir).unwrap();

        meta_put(&db, META_RESTORE_BEGIN, b"not-a-marker").unwrap();
        let payload = snap_dir.join("maybe-good.snap");
        fs::write(&payload, b"{}").unwrap();
        let part = snap_dir.join("residue.snap.part");
        fs::write(&part, b"x").unwrap();

        assert!(
            sweep_snapshot_payloads(&db, snap_dir.clone()).is_err(),
            "坏 marker 时清扫必须 fail-fast"
        );
        assert!(fs::exists(&payload).unwrap(), "清扫不得删除任何文件");
        assert!(fs::exists(&part).unwrap());

        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));
        let e = recover_interrupted_restore_with(&sm, &db, dir.clone(), false).unwrap_err();
        assert!(
            e.to_string().contains("corrupt"),
            "坏 marker 必须报 corrupt：{e}"
        );
        assert!(
            meta_get(&db, META_RESTORE_BEGIN).unwrap().is_some(),
            "fail-fast 时保留 marker 现场，供人工判断"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 交接测试 #2：正常 install 成功后 marker 不残留。
    #[tokio::test]
    async fn install_snapshot_clears_restore_marker() {
        let dir = tmpdir("p4a-install-marker");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let db = storage.raw_db();
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));
        let mut store = StateMachineStore::new(sm, db.clone(), dir.clone());

        let meta = snap_meta(5);
        let data = json_payload(&pairs_of(&[("a", "1")]));
        let payload = write_snapshot_payload(dir.join(SNAP_SUBDIR), &meta, &data).unwrap();
        store.install_snapshot(&meta, payload).await.unwrap();
        assert!(
            meta_get(&db, META_RESTORE_BEGIN).unwrap().is_none(),
            "成功 install 必须清 marker"
        );
        assert!(!store.is_poisoned());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 交接测试 #4 + 设计 §7 A1 的验收：**开着清扫**模拟一次崩溃点
    /// （payload 已落盘 + marker 已写 + 状态机仍是旧内容），重启后
    /// ctor 清扫保留 payload → recover 重放 → 状态/registry/marker 一致。
    #[tokio::test]
    async fn boot_replay_after_interrupted_restore_with_sweep_on() {
        let dir = tmpdir("p4a-boot-replay");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let db = storage.raw_db();
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));
        // 崩溃前状态机内容（旧）
        sm.write()
            .unwrap()
            .restore_all(&pairs_of(&[("old", "1")]))
            .unwrap();

        // 崩溃现场：新 payload 已落盘、marker 已写，restore 未开始
        let meta = snap_meta(9);
        let new_pairs = pairs_of(&[("new", "2"), ("other", "3")]);
        let payload =
            write_snapshot_payload(dir.join(SNAP_SUBDIR), &meta, &json_payload(&new_pairs))
                .unwrap();
        write_marker(&db, &meta, &payload);

        // 「重启」：先构造 store（内部跑清扫），再走 dsh-cli 的 boot 重放入口
        let _store = StateMachineStore::new(sm.clone(), db.clone(), dir.clone());
        assert!(
            fs::exists(&payload).unwrap(),
            "ctor 清扫不得删掉 marker payload"
        );
        let out = recover_interrupted_restore(&sm, &db, dir.clone()).unwrap();
        assert_eq!(
            out.expect("应返回重放的 meta").last_log_id,
            meta.last_log_id
        );

        assert_eq!(state_pairs(&sm), new_pairs, "状态机必须是 payload 内容");
        let (reg_meta, reg_file) = load_snapshot_registry(&db, dir.join(SNAP_SUBDIR))
            .unwrap()
            .expect("registry");
        assert_eq!(reg_meta.last_log_id, meta.last_log_id);
        assert_eq!(reg_file, payload, "registry 必须指向 marker 的 payload");
        assert_eq!(
            meta_get(&db, META_LAST_APPLIED).unwrap(),
            Some(ser(&meta.last_log_id.unwrap()).unwrap()),
            "重放必须补写 last_applied"
        );
        assert!(
            meta_get(&db, META_RESTORE_BEGIN).unwrap().is_none(),
            "重放成功必须清 marker"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 崩溃于「registry 已提交、marker 未清」之间：重放幂等，不得删掉 registry 自己的 payload。
    #[tokio::test]
    async fn recover_after_registry_commit_is_idempotent() {
        let dir = tmpdir("p4a-replay-idempotent");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let db = storage.raw_db();
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));

        let meta = snap_meta(7);
        let new_pairs = pairs_of(&[("k", "v")]);
        let payload =
            write_snapshot_payload(dir.join(SNAP_SUBDIR), &meta, &json_payload(&new_pairs))
                .unwrap();
        persist_snapshot_registry(&db, &meta, payload.clone()).unwrap();
        write_marker(&db, &meta, &payload);

        let out = recover_interrupted_restore_with(&sm, &db, dir.clone(), false).unwrap();
        assert!(out.is_some());
        assert!(fs::exists(&payload).unwrap(), "registry payload 不得被删");
        let (_, reg_file) = load_snapshot_registry(&db, dir.join(SNAP_SUBDIR))
            .unwrap()
            .expect("registry");
        assert_eq!(reg_file, payload);
        assert!(meta_get(&db, META_RESTORE_BEGIN).unwrap().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 交接测试 #3：marker 在、payload 被删 → 启动必须返回明确错误（不静默继续）。
    #[tokio::test]
    async fn recover_refuses_missing_payload() {
        let dir = tmpdir("p4a-missing-payload");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let db = storage.raw_db();
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));
        let meta = snap_meta(11);
        let ghost = dir.join(SNAP_SUBDIR).join("gone.snap");
        write_marker(&db, &meta, &ghost);

        let e = recover_interrupted_restore_with(&sm, &db, dir.clone(), false).unwrap_err();
        assert!(e.to_string().contains("missing"), "{e}");
        assert!(
            e.to_string().contains("backup"),
            "错误信息必须给出恢复路径：{e}"
        );
        assert!(
            meta_get(&db, META_RESTORE_BEGIN).unwrap().is_some(),
            "fail-fast 不清 marker"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 交接测试 #5：`DSH_RAFT_RESTORE_FORCE`（显式逃生门）忽略 marker —— 只清标记，不重放。
    #[tokio::test]
    async fn recover_force_ignores_marker_without_replay() {
        let dir = tmpdir("p4a-force");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let db = storage.raw_db();
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));
        sm.write()
            .unwrap()
            .restore_all(&pairs_of(&[("old", "1")]))
            .unwrap();

        let meta = snap_meta(13);
        let payload = write_snapshot_payload(
            dir.join(SNAP_SUBDIR),
            &meta,
            &json_payload(&pairs_of(&[("new", "2")])),
        )
        .unwrap();
        write_marker(&db, &meta, &payload);

        let out = recover_interrupted_restore_with(&sm, &db, dir.clone(), true).unwrap();
        assert!(out.is_none(), "force 路径不重放");
        assert_eq!(
            state_pairs(&sm),
            pairs_of(&[("old", "1")]),
            "状态机保持原样"
        );
        assert!(
            meta_get(&db, META_RESTORE_BEGIN).unwrap().is_none(),
            "force 清 marker"
        );
        assert!(fs::exists(&payload).unwrap(), "payload 不被 force 删除");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// kill-switch：`DSH_RAFT_RESTORE_MARKER=0` 只关「写 marker」。restore 失败时没有标记可重放
    /// ——这正是它只能当发布期逃生门的原因（测试用 set_restore_marker 覆盖，避免 env 竞态）。
    #[tokio::test]
    async fn kill_switch_disables_marker_write() {
        let dir = tmpdir("p4a-kill-switch");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let db = storage.raw_db();
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));
        let mut store = StateMachineStore::new(sm.clone(), db.clone(), dir.clone());
        store.set_restore_marker(false);
        store.set_fail_last_applied(1);

        let meta = snap_meta(15);
        let payload = write_snapshot_payload(
            dir.join(SNAP_SUBDIR),
            &meta,
            &json_payload(&pairs_of(&[("k", "v")])),
        )
        .unwrap();
        assert!(store.install_snapshot(&meta, payload).await.is_err());
        assert!(
            meta_get(&db, META_RESTORE_BEGIN).unwrap().is_none(),
            "关闭后不得写 marker"
        );

        // A2 的另一半：开关只关「写」，**已存在**的 marker 仍必须重放——recover 不接
        // store 句柄、也不读该开关，这里手工放一个 marker 验证它照样被重放。
        let meta2 = snap_meta(16);
        let pairs2 = pairs_of(&[("from", "marker")]);
        let payload2 =
            write_snapshot_payload(dir.join(SNAP_SUBDIR), &meta2, &json_payload(&pairs2)).unwrap();
        write_marker(&db, &meta2, &payload2);
        let out = recover_interrupted_restore_with(&sm, &db, dir.clone(), false).unwrap();
        assert_eq!(
            out.expect("已有 marker 必须重放").last_log_id,
            meta2.last_log_id
        );
        assert_eq!(state_pairs(&sm), pairs2);
        assert!(meta_get(&db, META_RESTORE_BEGIN).unwrap().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 设计 §7 A3：restore 中段 IO 错误（restore_all 已成功、last_applied 写失败）→
    /// 节点进入 fail-stop：apply / 快照构建 / get_current_snapshot 全拒绝，marker 保留待重放；
    /// 一次完整成功的 install 才解除。
    #[tokio::test]
    async fn restore_error_poisons_until_next_successful_install() {
        let dir = tmpdir("p4a-poison");
        let _ = std::fs::remove_dir_all(&dir);
        let storage = open_storage(&dir);
        let db = storage.raw_db();
        let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage.clone()))));
        let mut store = StateMachineStore::new(sm.clone(), db.clone(), dir.clone());

        let meta = snap_meta(21);
        let pairs = pairs_of(&[("a", "1"), ("b", "2")]);
        let payload =
            write_snapshot_payload(dir.join(SNAP_SUBDIR), &meta, &json_payload(&pairs)).unwrap();

        // 注入：install 内部的 write_last_applied（restore 之后）失败
        store.set_fail_last_applied(1);
        assert!(store
            .install_snapshot(&meta, payload.clone())
            .await
            .is_err());
        assert!(store.is_poisoned(), "restore 错误必须 fail-stop");
        assert!(
            meta_get(&db, META_RESTORE_BEGIN).unwrap().is_some(),
            "失败 install 必须留下 marker 供 boot 重放"
        );

        // apply 必须拒绝（并 drain responder，不能挂客户端）
        let items = entry_stream(vec![blank(22)]);
        assert!(
            store.apply(stream::iter(items)).await.is_err(),
            "poisoned apply 必须返回 Err"
        );
        // 快照构建 / 读取拒绝
        assert!(store
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .is_err());
        assert!(store.get_current_snapshot().await.is_err());

        // 解除注入后重试同一 payload → 成功即解除 fail-stop、清 marker
        store.set_fail_last_applied(0);
        store.install_snapshot(&meta, payload).await.unwrap();
        assert!(!store.is_poisoned());
        assert!(meta_get(&db, META_RESTORE_BEGIN).unwrap().is_none());
        assert_eq!(state_pairs(&sm), pairs);
        assert!(store.write_last_applied(&meta.last_log_id.unwrap()).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
