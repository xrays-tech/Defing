//! 存储抽象（模块 02 的应用层入口）：Store trait + 内存实现。

use std::collections::BTreeMap;
use std::sync::RwLock;

use crate::error::{Error, ErrorKind};

/// 前缀扫描结果：(key, value) 列表。
pub type KeyValuePairs = Vec<(Vec<u8>, Vec<u8>)>;

/// 应用层 KV 存储接口（redb 由 dsh-storage 实现；单命名空间）。
pub trait Store: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error>;
    fn delete(&self, key: &[u8]) -> Result<(), Error>;
    /// 前缀扫描（按 key 字典序）。
    fn get_prefix(&self, prefix: &[u8]) -> Result<KeyValuePairs, Error>;
    /// 前缀 key 扫描（不 materialize value）。
    ///
    /// 快照安装要「清空全部旧 key」，只需要 key：旧实现用 `get_prefix_merged(b"")` 拿全量
    /// (k,v)，在「本来就有大状态」的节点上会额外驻留一份 payload（复核 major）。默认实现
    /// 丢弃 value（内存实现足够）；redb 覆盖为只读 key 的 range 扫描。
    fn get_prefix_keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        Ok(self
            .get_prefix(prefix)?
            .into_iter()
            .map(|(k, _)| k)
            .collect())
    }
    /// 批量写（单次事务提交语义）。
    fn put_batch(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Error>;
    /// 批量写 + 批量删（单事务，原子提交；redb 一次 fsync；内存实现先删后插）。
    /// perf 方案①：状态机命令末统一落盘，替代逐 key 独立事务（每次 fsync）。
    fn write_batch(&self, puts: &[(Vec<u8>, Vec<u8>)], deletes: &[Vec<u8>]) -> Result<(), Error>;
    /// 强制落盘（崩溃恢复测试用）；内存实现为空操作。
    fn flush(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// 内存实现（测试与 dev-single 默认）。
#[derive(Default)]
pub struct InMemoryStore {
    inner: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Store for InMemoryStore {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.inner.read().expect("store lock").get(key).cloned())
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.inner
            .write()
            .expect("store lock")
            .insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.inner.write().expect("store lock").remove(key);
        Ok(())
    }

    fn get_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        let guard = self.inner.read().expect("store lock");
        let start = prefix.to_vec();
        Ok(guard
            .range(start..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    fn put_batch(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Error> {
        let mut guard = self.inner.write().expect("store lock");
        for (k, v) in pairs {
            guard.insert(k.clone(), v.clone());
        }
        Ok(())
    }

    fn write_batch(&self, puts: &[(Vec<u8>, Vec<u8>)], deletes: &[Vec<u8>]) -> Result<(), Error> {
        let mut guard = self.inner.write().expect("store lock");
        for k in deletes {
            guard.remove(k);
        }
        for (k, v) in puts {
            guard.insert(k.clone(), v.clone());
        }
        Ok(())
    }
}

/// 存储错误包装。
pub fn storage_err(msg: impl Into<String>) -> Error {
    Error::new(ErrorKind::Storage, msg)
}
