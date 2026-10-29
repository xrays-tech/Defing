//! redb 存储实现（模块 02）：多表布局 + Store trait 实现 + 文件级备份。
//!
//! rocksdb → redb 迁移（dev_docs/design/storage-redb-migration.md §3.1/3.2/3.4）：
//! - 单文件 `{data_dir}/dsh.redb`，4 张 `&[u8]→&[u8]` 表（替代 4 列族）；
//! - 表定义在本 crate pub 导出、dsh-raft 跨 crate 引用（单一来源，N5）；
//! - 备份 = 持有未提交写事务互斥写入 + 文件 copy（redb 4.x 无官方 backup API）。

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use dsh_core::error::{Error, ErrorKind};
use dsh_core::store::Store;
use redb::{Database, ReadableDatabase, TableDefinition};
use tracing::warn;

/// 数据库文件名（位于 data_dir 下）。
const DB_FILE: &str = "dsh.redb";

/// 全部表定义（open 时 eager 预建，见 [`RedbStorage::open`]）。
const ALL_TABLES: [TableDefinition<&[u8], &[u8]>; 4] =
    [TBL_STATE, TBL_RAFT_LOG, TBL_RAFT_META, TBL_SNAPSHOTS];

/// 状态机 KV（p/、sh/、idx/、sess/、audit/，Store trait 落点）。
pub const TBL_STATE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("state");
/// Raft 日志（key = 日志索引 big-endian u64）。
pub const TBL_RAFT_LOG: TableDefinition<&[u8], &[u8]> = TableDefinition::new("raft-log");
/// Raft 元数据（hard_state / last_applied 等）。
pub const TBL_RAFT_META: TableDefinition<&[u8], &[u8]> = TableDefinition::new("raft-meta");
/// 快照持久化（SnapshotMeta + chunk）。
pub const TBL_SNAPSHOTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("snapshots");

/// redb 错误 → dsh-core Storage 错误。
/// orphan 规则禁止跨 crate `impl From<redb::Error> for dsh_core::Error`，统一经此转换。
fn redb_error(context: &str, err: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::Storage, format!("{context}: {err}"))
}

/// redb 存储（4 表单库；Clone 共享同一 Database 句柄）。
#[derive(Clone)]
pub struct RedbStorage {
    db: Arc<Database>,
    /// 备份时 copy 的源文件路径（redb 4.1 无 `Database::path()`）。
    db_path: PathBuf,
}

impl RedbStorage {
    /// 打开/初始化 `{dir}/dsh.redb` 并 eager 预建 4 张表。
    ///
    /// - `create` 对已存在合法库等价 open，首次运行初始化（两种情况一次覆盖）；
    /// - 预建表是硬要求（B1）：`ReadTransaction::open_table` 对不存在的表报
    ///   `TableDoesNotExist`，不预建则全新 data_dir 下读路径在首写前全部报错；
    /// - 文件损坏自动修复（repair callback 记日志）；仅当修复发生时才追加
    ///   `check_integrity` 校验（该调用较慢且需 `&mut`，故在 Arc 包装前执行）。
    pub fn open(dir: &str) -> Result<Self, Error> {
        std::fs::create_dir_all(dir)
            .map_err(|e| Error::new(ErrorKind::Storage, format!("create dir: {e}")))?;
        let db_path = Path::new(dir).join(DB_FILE);

        let repair_triggered = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&repair_triggered);
        let mut db = Database::builder()
            .set_repair_callback(move |session| {
                warn!(
                    progress = %session.progress(),
                    "redb database file needs repair"
                );
                flag.store(true, Ordering::Release);
            })
            .create(&db_path)
            .map_err(|e| redb_error("open redb", e))?;

        let txn = db.begin_write().map_err(|e| redb_error("begin write", e))?;
        for table in ALL_TABLES {
            txn.open_table(table)
                .map_err(|e| redb_error("create table", e))?;
        }
        txn.commit()
            .map_err(|e| redb_error("commit table creation", e))?;

        if repair_triggered.load(Ordering::Acquire) {
            let intact = db
                .check_integrity()
                .map_err(|e| redb_error("check_integrity", e))?;
            if !intact {
                return Err(Error::new(
                    ErrorKind::Storage,
                    "integrity check failed: database repaired with possible data loss",
                ));
            }
        }

        // F7a：数据库文件含全部密文/密码哈希/会话哈希 —— 权限收紧 0600
        //（含存量 0644 文件一并修复，对齐 crypto::save_ring 的 ring 文件处理）。
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&db_path, std::fs::Permissions::from_mode(0o600))
                .map_err(|e| redb_error("set db file permissions", e))?;
        }

        Ok(Self {
            db: Arc::new(db),
            db_path,
        })
    }

    /// 供 Raft 日志/元数据/快照直接访问（模块 03 使用）。
    pub fn raw_db(&self) -> Arc<Database> {
        Arc::clone(&self.db)
    }

    /// 数据目录（`open` 的 dir 的 arg，即 `{dir}/dsh.redb` 的 parent）。
    /// P2 快照文件目录 = `{data_dir}/snapshots`（file-backed SnapshotData）。
    pub fn base_dir(self) -> PathBuf {
        match self.db_path.parent() {
            Some(p) => p.to_path_buf(),
            None => PathBuf::new(),
        }
    }

    /// 创建备份：copy `{data_dir}/dsh.redb` → `{dest_dir}/dsh.redb`，返回备份文件路径。
    ///
    /// 实现方式：持有未提交的写事务贯穿整个 copy 窗口——redb 单写者语义下其他
    /// `begin_write` 全部阻塞，即进程内所有写（含 raft 日志/状态机）被互斥，
    /// copy 得到某个精确一致提交点的文件快照；guard drop 即 abort（纯内存，零 IO）。
    ///
    /// **调用方应放 `tokio::task::spawn_blocking`**（begin_write 与文件 copy 均为
    /// 阻塞调用）；备份窗口会阻塞所有写（含 raft 心跳/日志复制）。
    pub fn create_backup(&self, dest_dir: &Path) -> Result<PathBuf, Error> {
        std::fs::create_dir_all(dest_dir)
            .map_err(|e| Error::new(ErrorKind::Storage, format!("create backup dir: {e}")))?;
        let write_guard = self
            .db
            .begin_write()
            .map_err(|e| redb_error("acquire backup write lock", e))?;
        let dest = dest_dir.join(DB_FILE);
        std::fs::copy(&self.db_path, &dest)
            .map_err(|e| Error::new(ErrorKind::Storage, format!("copy database file: {e}")))?;
        drop(write_guard); // abort：未写入任何数据，纯内存回滚
        Ok(dest)
    }
}

impl Store for RedbStorage {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| redb_error("begin read", e))?;
        let table = txn
            .open_table(TBL_STATE)
            .map_err(|e| redb_error("open state table", e))?;
        Ok(table
            .get(key)
            .map_err(|e| redb_error("get", e))?
            .map(|value| value.value().to_vec()))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_error("begin write", e))?;
        let mut table = txn
            .open_table(TBL_STATE)
            .map_err(|e| redb_error("open state table", e))?;
        table.insert(key, value).map_err(|e| redb_error("put", e))?;
        drop(table);
        txn.commit().map_err(|e| redb_error("commit put", e))
    }

    fn delete(&self, key: &[u8]) -> Result<(), Error> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_error("begin write", e))?;
        let mut table = txn
            .open_table(TBL_STATE)
            .map_err(|e| redb_error("open state table", e))?;
        table.remove(key).map_err(|e| redb_error("delete", e))?;
        drop(table);
        txn.commit().map_err(|e| redb_error("commit delete", e))
    }

    fn get_prefix_keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| redb_error("begin read", e))?;
        let table = txn
            .open_table(TBL_STATE)
            .map_err(|e| redb_error("open state table", e))?;
        let mut keys = Vec::new();
        let iter = table
            .range::<&[u8]>(prefix..)
            .map_err(|e| redb_error("prefix range", e))?;
        for entry in iter {
            let (key, _value) = entry.map_err(|e| redb_error("prefix scan", e))?;
            let k = key.value();
            if !k.starts_with(prefix) {
                break;
            }
            keys.push(k.to_vec());
        }
        Ok(keys)
    }

    fn get_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| redb_error("begin read", e))?;
        let table = txn
            .open_table(TBL_STATE)
            .map_err(|e| redb_error("open state table", e))?;
        let mut rows = Vec::new();
        let iter = table
            .range::<&[u8]>(prefix..)
            .map_err(|e| redb_error("prefix range", e))?;
        for entry in iter {
            let (key, value) = entry.map_err(|e| redb_error("prefix scan", e))?;
            if !key.value().starts_with(prefix) {
                break;
            }
            rows.push((key.value().to_vec(), value.value().to_vec()));
        }
        Ok(rows)
    }

    fn put_batch(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Error> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_error("begin write", e))?;
        let mut table = txn
            .open_table(TBL_STATE)
            .map_err(|e| redb_error("open state table", e))?;
        for (key, value) in pairs {
            table
                .insert(key.as_slice(), value.as_slice())
                .map_err(|e| redb_error("batch insert", e))?;
        }
        drop(table);
        txn.commit().map_err(|e| redb_error("commit batch", e))
    }

    fn write_batch(&self, puts: &[(Vec<u8>, Vec<u8>)], deletes: &[Vec<u8>]) -> Result<(), Error> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_error("begin write", e))?;
        let mut table = txn
            .open_table(TBL_STATE)
            .map_err(|e| redb_error("open state table", e))?;
        // 先删后插：同一事务内 remove+insert 同 key 无冲突（redb 单写者事务内自洽）
        for key in deletes {
            table
                .remove(key.as_slice())
                .map_err(|e| redb_error("batch remove", e))?;
        }
        for (key, value) in puts {
            table
                .insert(key.as_slice(), value.as_slice())
                .map_err(|e| redb_error("batch insert", e))?;
        }
        drop(table);
        txn.commit()
            .map_err(|e| redb_error("commit write_batch", e))
    }

    fn flush(&self) -> Result<(), Error> {
        // redb 写事务默认 Durability::Immediate（commit 返回即 fsync），无需额外落盘
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    fn tmpdir(tag: &str) -> PathBuf {
        let n = SEQ.fetch_add(1, AtomicOrdering::Relaxed);
        std::env::temp_dir().join(format!("dsh-storage-{tag}-{}-{n}", std::process::id()))
    }

    #[test]
    fn open_put_get_prefix() {
        let dir = tmpdir("basic");
        let _ = std::fs::remove_dir_all(&dir);
        let store = RedbStorage::open(&dir.display().to_string()).unwrap();
        // F7a：数据库文件权限必须为 0600（仅 Unix 断言）
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(dir.join("dsh.redb"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600, "db 文件权限必须为 0600");
        }
        // B1 回归：空库未写入前，读路径不得报 TableDoesNotExist
        assert!(store.get_prefix(b"").unwrap().is_empty());
        assert!(store.get(b"missing").unwrap().is_none());

        store.put(b"p/x", b"1").unwrap();
        store.put(b"p/x/struct", b"2").unwrap();
        store.put(b"p/y", b"3").unwrap();
        assert_eq!(store.get(b"p/x").unwrap().unwrap(), b"1");
        // 前缀边界：p/x 与 p/x/struct 命中，p/y 排除
        let rows = store.get_prefix(b"p/x").unwrap();
        assert_eq!(rows.len(), 2);
        // 不存在前缀 → 空
        assert!(store.get_prefix(b"zzz").unwrap().is_empty());

        store
            .put_batch(&[
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
            ])
            .unwrap();
        assert_eq!(store.get(b"a").unwrap().unwrap(), b"1");
        // 空前缀 = 全表
        assert_eq!(store.get_prefix(b"").unwrap().len(), 5);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn crash_reopen_keeps_data() {
        let dir = tmpdir("crash");
        let _ = std::fs::remove_dir_all(&dir);
        {
            let store = RedbStorage::open(&dir.display().to_string()).unwrap();
            store.put(b"k", b"v").unwrap();
            store.flush().unwrap();
        }
        {
            let store = RedbStorage::open(&dir.display().to_string()).unwrap();
            assert_eq!(store.get(b"k").unwrap().unwrap(), b"v");
            store.put(b"k2", b"v2").unwrap();
        }
        {
            let store = RedbStorage::open(&dir.display().to_string()).unwrap();
            assert_eq!(store.get(b"k").unwrap().unwrap(), b"v");
            assert_eq!(store.get(b"k2").unwrap().unwrap(), b"v2");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn backup_file_copy() {
        let dir = tmpdir("bk");
        let dest = tmpdir("bk-out");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dest);
        let store = RedbStorage::open(&dir.display().to_string()).unwrap();
        store.put(b"k", b"v").unwrap();
        store.put(b"k2", b"v2").unwrap();
        store.flush().unwrap();
        let backup_path = store.create_backup(&dest).unwrap();
        assert_eq!(backup_path, dest.join("dsh.redb"));

        // 备份可被 Database::open 独立打开且数据完整
        let backup_db = Database::open(&backup_path).unwrap();
        let txn = backup_db.begin_read().unwrap();
        let table = txn.open_table(TBL_STATE).unwrap();
        assert_eq!(table.get(b"k".as_slice()).unwrap().unwrap().value(), b"v");
        assert_eq!(table.get(b"k2".as_slice()).unwrap().unwrap().value(), b"v2");
        drop(table);
        drop(txn);
        drop(backup_db);

        // 备份后原库仍可正常写
        store.put(b"k3", b"v3").unwrap();
        assert_eq!(store.get(b"k3").unwrap().unwrap(), b"v3");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dest);
    }

    #[test]
    fn put_batch_success() {
        let dir = tmpdir("batch");
        let _ = std::fs::remove_dir_all(&dir);
        let store = RedbStorage::open(&dir.display().to_string()).unwrap();
        store.put(b"old", b"0").unwrap();
        store
            .put_batch(&[
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"old".to_vec(), b"9".to_vec()), // 同批覆盖既有 key
            ])
            .unwrap();
        assert_eq!(store.get(b"a").unwrap().unwrap(), b"1");
        assert_eq!(store.get(b"b").unwrap().unwrap(), b"2");
        assert_eq!(store.get(b"old").unwrap().unwrap(), b"9");
        assert_eq!(store.get_prefix(b"").unwrap().len(), 3);
        // 空批不报错
        store.put_batch(&[]).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_batch_puts_and_deletes_atomically() {
        let dir = tmpdir("writebatch");
        let _ = std::fs::remove_dir_all(&dir);
        let store = RedbStorage::open(&dir.display().to_string()).unwrap();
        store.put(b"del", b"1").unwrap();
        store.put(b"keep", b"2").unwrap();
        store.put(b"same", b"old").unwrap();
        // 混合：删 2 个 + 插 2 个 + 覆盖 1 个（同批内先删后插自洽）
        store
            .write_batch(
                &[
                    (b"new1".to_vec(), b"v1".to_vec()),
                    (b"same".to_vec(), b"new".to_vec()),
                    (b"new2".to_vec(), b"v2".to_vec()),
                ],
                &[b"del".to_vec(), b"same".to_vec()],
            )
            .unwrap();
        assert_eq!(store.get(b"del").unwrap(), None);
        assert_eq!(store.get(b"keep").unwrap().unwrap(), b"2");
        // 同一事务内先删后插 → same 最终值为 new（redb 单写者事务内自洽）
        assert_eq!(store.get(b"same").unwrap().unwrap(), b"new");
        assert_eq!(store.get(b"new1").unwrap().unwrap(), b"v1");
        assert_eq!(store.get(b"new2").unwrap().unwrap(), b"v2");
        assert_eq!(store.get_prefix(b"").unwrap().len(), 4);
        // 空批不报错
        store.write_batch(&[], &[]).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn multi_table_isolation() {
        let dir = tmpdir("tables");
        let _ = std::fs::remove_dir_all(&dir);
        let store = RedbStorage::open(&dir.display().to_string()).unwrap();

        // raft 表写入（模拟 dsh-raft 的直接表访问；key = 日志索引 big-endian u64）
        let log_key: &[u8] = b"\0\0\0\0\0\0\0\x01";
        let db = store.raw_db();
        let txn = db.begin_write().unwrap();
        {
            let mut log = txn.open_table(TBL_RAFT_LOG).unwrap();
            log.insert(log_key, b"log-entry-1".as_slice()).unwrap();
            let mut meta = txn.open_table(TBL_RAFT_META).unwrap();
            meta.insert(b"hard_state".as_slice(), b"term=1".as_slice())
                .unwrap();
        }
        txn.commit().unwrap();

        // state 表（Store 路径）看不到 raft 表数据
        assert!(store.get(b"hard_state").unwrap().is_none());
        assert!(store.get_prefix(b"").unwrap().is_empty());

        // 反向：state 写入不污染 raft 表
        store.put(b"p/x", b"1").unwrap();
        let txn = db.begin_read().unwrap();
        let log = txn.open_table(TBL_RAFT_LOG).unwrap();
        assert_eq!(log.get(log_key).unwrap().unwrap().value(), b"log-entry-1");
        assert!(log.get(b"p/x".as_slice()).unwrap().is_none());
        let meta = txn.open_table(TBL_RAFT_META).unwrap();
        assert_eq!(
            meta.get(b"hard_state".as_slice()).unwrap().unwrap().value(),
            b"term=1"
        );
        let state = txn.open_table(TBL_STATE).unwrap();
        assert_eq!(state.get(b"p/x".as_slice()).unwrap().unwrap().value(), b"1");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn get_prefix_keys_matches_full_scan_keys_without_values() {
        let dir = tmpdir("prefix-keys");
        let _ = std::fs::remove_dir_all(&dir);
        let store = RedbStorage::open(&dir.display().to_string()).unwrap();
        store.put(b"a/1", b"v1").unwrap();
        store.put(b"a/2", b"v2").unwrap();
        store.put(b"b/1", b"v3").unwrap();

        let keys = store.get_prefix_keys(b"a/").unwrap();
        assert_eq!(
            keys,
            vec![b"a/1".to_vec(), b"a/2".to_vec()],
            "只返回前缀内 key，按字典序"
        );

        let full: Vec<Vec<u8>> = store
            .get_prefix(b"a/")
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(keys, full, "与 get_prefix 的 key 序列一致");

        assert!(store.get_prefix_keys(b"zzz/").unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
