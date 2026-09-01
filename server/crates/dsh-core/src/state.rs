//! 确定性状态机（模块 01/04）：命令 apply + 读取。
//! 约定：apply 不读墙钟/不 IO/不日志（D16）；时间戳由调用方注入 now_ms。
//! M1 范围：项目/分支 CRUD、结构草稿与结构发布、值草稿、值发布、GetConfig（版本快照全量存储）。

use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::command::Command;
use crate::diff::compute_diff;
use crate::error::{Error, ErrorKind};
use crate::keys::*;
use crate::limits::*;
use crate::model::*;
use crate::store::{KeyValuePairs, Store};
use crate::validator;

/// GetConfig 返回的配置快照。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigSnapshot {
    pub project: String,
    pub branch: String,
    pub version: u64,
    /// 解析出的版本号语义别名（G3/D27）：数据面读取时 = 服务端按身份 resolve 的结果
    /// （稳定 = active_version；灰度命中 = gray_seq）；管理面显式版本请求 = version 本身。
    #[serde(default)]
    pub resolved_version: u64,
    pub structure_version: u64,
    pub groups: BTreeMap<String, BTreeMap<String, Value>>,
    /// 灰度标记（G3/D27）：true = 本次返回的是灰度快照内容（客户端可见自己在灰度）。
    #[serde(default)]
    pub gray: bool,
}

/// 客户端身份（G2 灰度解析输入；G3 数据面由 HTTP 头/gRPC 字段/对端地址注入）。
/// D18：instance_id 优先（容器重建不变）> labels > IP（兜底）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClientCtx {
    pub instance_id: String,
    pub labels: BTreeMap<String, String>,
    pub ip: Option<std::net::IpAddr>,
}

/// 灰度解析结果（G3/D24：消除 gray_seq 与 active_version 数值巧合的分流歧义）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedVersion {
    /// 读稳定版 v（= active_version）。
    Stable(u64),
    /// 读灰度快照 gray-snap/{seq}（= gray_seq）。
    Gray(u64),
}

/// 版本存储 checkpoint 间隔（perf 方案② D3）：每 N 版本存 full 快照，其余存 diff。
/// 与 design-modules/04-publish.md §8 一致；改小可降低重建成本但增加存储，改大反之。
pub const CHECKPOINT_INTERVAL: u64 = 100;

/// apply 结果：成功产出的事件列表（确定性副作用，供 watch 扇出）。
pub type ApplyOutcome = Result<Vec<PublishEvent>, Error>;

fn load<T: DeserializeOwned>(store: &dyn Store, key: &str) -> Result<Option<T>, Error> {
    match store.get(key.as_bytes())? {
        Some(raw) => serde_json::from_slice(&raw)
            .map(Some)
            .map_err(|e| Error::internal(format!("corrupt value at {key}: {e}"))),
        None => Ok(None),
    }
}

fn save<T: Serialize>(store: &dyn Store, key: &str, value: &T) -> Result<(), Error> {
    let raw = serde_json::to_vec(value).map_err(|e| Error::internal(format!("serialize: {e}")))?;
    store.put(key.as_bytes(), &raw)
}

/// 项目名合法性（[a-z0-9][a-z0-9-]{0,127}）。
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_PROJECT_NAME_BYTES
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && name.as_bytes()[0] != b'-'
        && name.as_bytes()[name.len() - 1] != b'-'
}

/// 分支名合法性（[a-z0-9][a-z0-9-]{0,63}）。
fn valid_branch(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && name.as_bytes()[0] != b'-'
        && name.as_bytes()[name.len() - 1] != b'-'
}

/// 命令级写缓冲操作（perf 方案①）：统一序列保证"最后一次操作决定"语义。
#[derive(Debug, Clone)]
enum PendingOp {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

/// 确定性状态机。
pub struct StateMachine {
    store: Box<dyn Store>,
    /// 命令级写缓冲（perf 方案①：apply 期间收集写操作，命令末统一 write_batch 单事务提交）。
    /// apply 开始清空、命令成功 flush、失败 abort。非 apply 路径（快照安装/后台任务）不使用。
    pending_ops: Vec<PendingOp>,
}

impl StateMachine {
    pub fn new(store: Box<dyn Store>) -> Self {
        Self {
            store,
            pending_ops: Vec::new(),
        }
    }

    // ---------------- 命令级写缓冲（perf 方案①） ----------------

    /// 写缓冲 put：apply 期间收集；无 pending（非 apply 路径）时直写 store。
    fn put_pending(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.pending_ops
            .push(PendingOp::Put(key.to_vec(), value.to_vec()));
        Ok(())
    }

    /// 写缓冲 delete：apply 期间收集。
    fn delete_pending(&mut self, key: &[u8]) -> Result<(), Error> {
        self.pending_ops.push(PendingOp::Delete(key.to_vec()));
        Ok(())
    }

    /// 读合并 get：pending 逆序找 key（最后一次操作决定），miss 走 store。
    fn get_merged(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        for op in self.pending_ops.iter().rev() {
            match op {
                PendingOp::Put(k, v) if k.as_slice() == key => return Ok(Some(v.clone())),
                PendingOp::Delete(k) if k.as_slice() == key => return Ok(None),
                _ => {}
            }
        }
        self.store.get(key)
    }

    /// 读合并 get_prefix：store 结果 + pending 操作（按序应用），BTreeMap 保字典序。
    fn get_prefix_merged(&self, prefix: &[u8]) -> Result<KeyValuePairs, Error> {
        let mut out: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
            self.store.get_prefix(prefix)?.into_iter().collect();
        for op in &self.pending_ops {
            match op {
                PendingOp::Put(k, v) => {
                    if k.starts_with(prefix) {
                        out.insert(k.clone(), v.clone());
                    }
                }
                PendingOp::Delete(k) => {
                    if k.starts_with(prefix) {
                        out.remove(k);
                    }
                }
            }
        }
        Ok(out.into_iter().collect())
    }

    /// 命令末统一落盘：单事务 write_batch（puts + deletes）。
    fn flush_pending(&mut self) -> Result<(), Error> {
        if self.pending_ops.is_empty() {
            return Ok(());
        }
        // 操作序列 → puts/deletes（写缓冲内允许同 key 多操作，write_batch 先删后插自洽）
        let ops = std::mem::take(&mut self.pending_ops);
        let mut puts = Vec::new();
        let mut deletes = Vec::new();
        for op in ops {
            match op {
                PendingOp::Put(k, v) => puts.push((k, v)),
                PendingOp::Delete(k) => deletes.push(k),
            }
        }
        self.store.write_batch(&puts, &deletes)
    }

    /// 命令内读（写后读可见：pending 优先）——apply 路径统一入口。
    fn load_merged<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, Error> {
        match self.get_merged(key.as_bytes())? {
            Some(raw) => serde_json::from_slice(&raw)
                .map(Some)
                .map_err(|e| Error::internal(format!("corrupt value at {key}: {e}"))),
            None => Ok(None),
        }
    }

    /// 命令内写（写缓冲）——apply 路径统一入口。
    fn save_pending<T: Serialize>(&mut self, key: &str, value: &T) -> Result<(), Error> {
        let raw =
            serde_json::to_vec(value).map_err(|e| Error::internal(format!("serialize: {e}")))?;
        self.put_pending(key.as_bytes(), &raw)
    }

    // ---------------- 读取 ----------------

    pub fn get_project(&self, id: &ProjectId) -> Result<Option<Project>, Error> {
        self.load_merged(&project_key(id))
    }

    pub fn list_projects(&self) -> Result<Vec<Project>, Error> {
        let rows = self.get_prefix_merged(b"p/")?;
        let mut out = Vec::new();
        for (k, v) in rows {
            let ks = String::from_utf8_lossy(&k);
            let rest = &ks[K_PROJECT.len()..];
            if rest.contains('/') {
                continue; // 子键（struct/branch/...）跳过
            }
            if let Ok(p) = serde_json::from_slice::<Project>(&v) {
                out.push(p);
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    pub fn get_structure(&self, id: &ProjectId) -> Result<Option<Structure>, Error> {
        self.load_merged(&struct_key(id))
    }

    pub fn get_structure_draft(&self, id: &ProjectId) -> Result<Option<StructureDraft>, Error> {
        self.load_merged(&struct_draft_key(id))
    }

    pub fn get_branch_state(
        &self,
        id: &ProjectId,
        branch: &BranchName,
    ) -> Result<Option<BranchState>, Error> {
        self.load_merged(&branch_state_key(id, branch))
    }

    /// 读取当前活动会话（I7；无会话返回 None）。
    pub fn get_session(&self) -> Result<Option<AdminSession>, Error> {
        self.load_merged(session_key())
    }

    /// 多会话管理员会话：读 sess/admin/{session_id}（multisession 改造）。
    pub fn get_session_with(&self, session_id: &str) -> Result<Option<AdminSession>, Error> {
        self.load_merged(&session_key_with(session_id))
    }

    /// 审计查询：按 action 过滤、since（ts ≥ since，墙钟 ms）过滤、按 seq 倒序、limit 截断。
    pub fn get_audit(
        &self,
        action: Option<&str>,
        project: Option<&str>,
        since: Option<i64>,
        limit: usize,
    ) -> Result<Vec<AuditEntry>, Error> {
        let rows = self.get_prefix_merged(K_AUDIT.as_bytes())?;
        let mut out = Vec::new();
        for (k, v) in rows {
            let ks = String::from_utf8_lossy(&k);
            let Some(rest) = ks.strip_prefix(K_AUDIT) else {
                continue;
            };
            // 跳过计数键 "seq"（非 20 位数字后缀）
            if rest.parse::<u64>().is_err() {
                continue;
            }
            if let Ok(e) = serde_json::from_slice::<AuditEntry>(&v) {
                if let Some(a) = action {
                    if e.action != a {
                        continue;
                    }
                }
                if let Some(p) = project {
                    if e.project.as_deref() != Some(p) {
                        continue;
                    }
                }
                if let Some(s) = since {
                    if e.ts < s {
                        continue;
                    }
                }
                out.push(e);
            }
        }
        out.sort_by_key(|b| std::cmp::Reverse(b.seq)); // 新 → 旧
        if out.len() > limit {
            out.truncate(limit);
        }
        Ok(out)
    }

    /// 审计保留：仅保留最近 keep 条（后台任务用；keep=0 清空全部）。
    pub fn prune_audit(&self, keep: usize) -> Result<usize, Error> {
        let rows = self.get_prefix_merged(K_AUDIT.as_bytes())?;
        let mut seqs: Vec<u64> = Vec::new();
        for (k, _) in rows {
            let ks = String::from_utf8_lossy(&k);
            if let Some(rest) = ks.strip_prefix(K_AUDIT) {
                if let Ok(seq) = rest.parse::<u64>() {
                    seqs.push(seq);
                }
            }
        }
        seqs.sort_unstable();
        let total = seqs.len();
        if total <= keep {
            return Ok(0);
        }
        let mut removed = 0;
        for seq in seqs.into_iter().take(total - keep) {
            self.store.delete(audit_key(seq).as_bytes())?;
            removed += 1;
        }
        Ok(removed)
    }

    /// DEK 重包（B6）：扫描全部存储中的 secret 密文，用 `f` 逐个重写（轮换后台任务用）。
    /// `f` 返回 None = 跳过（如代际已最新）；返回 Some(新密文) = 写回。
    /// 覆盖：版本快照（…/snap）、版本 diff（…/diff，perf 方案② D3）、
    /// 共享项（sh/、sh-draft/）、分支草稿（…/b/{branch}/state）、灰度快照（gray-snap/，G2）。
    pub fn rewrap_deks(
        &self,
        f: &dyn Fn(&Ciphertext) -> Option<Result<Ciphertext, Error>>,
    ) -> Result<usize, Error> {
        let rows = self.get_prefix_merged(b"")?;
        let mut rewrapped = 0usize;
        for (k, v) in rows {
            let ks = String::from_utf8_lossy(&k);
            let key = ks.as_ref();
            if key.ends_with("/snap") {
                let mut snap: SnapshotMap = match serde_json::from_slice(&v) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                if Self::rewrap_snapshot(&mut snap, f)? {
                    save(&*self.store, key, &snap)?;
                    rewrapped += 1;
                }
            } else if key.ends_with("/diff") {
                // perf 方案② D3：diff 中 Upsert 条目的 new_value 可能含 Secret 密文
                let mut diff: Vec<DiffEntry> = match serde_json::from_slice(&v) {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                let mut changed = false;
                for entry in diff.iter_mut() {
                    if let ChangeKind::Upsert = entry.kind {
                        if let Some(nv) = &mut entry.new_value {
                            if Self::rewrap_value(nv, f)? {
                                changed = true;
                            }
                        }
                    }
                    // Delete 条目 new_value=None，天然跳过
                }
                if changed {
                    save(&*self.store, key, &diff)?;
                    rewrapped += 1;
                }
            } else if key.starts_with(K_SHARED) || key.starts_with(K_SHARED_DRAFT) {
                let mut item: SharedItem = match serde_json::from_slice(&v) {
                    Ok(i) => i,
                    Err(_) => continue,
                };
                if Self::rewrap_value(&mut item.value, f)? {
                    save(&*self.store, key, &item)?;
                    rewrapped += 1;
                }
            } else if key.contains(K_GRAY_SNAP) {
                // G2：灰度快照（gray-snap/{seq}，SnapshotMap）——含 Secret 时同样重包，
                // 否则 KEK 轮换后灰度客户端解密失败（与 /snap 版本快照同等对待）。
                let mut snap: SnapshotMap = match serde_json::from_slice(&v) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                if Self::rewrap_snapshot(&mut snap, f)? {
                    save(&*self.store, key, &snap)?;
                    rewrapped += 1;
                }
            } else if let Some(rest) = key.strip_prefix(K_PROJECT) {
                // p/{pid}/b/{branch}/state —— 草稿值
                if rest.contains(K_BRANCH) && key.ends_with(K_STATE) {
                    let mut st: BranchState = match serde_json::from_slice(&v) {
                        Ok(s) => s,
                        Err(_) => continue,
                    };
                    let mut changed = false;
                    for items in st.value_draft.values_mut() {
                        for dv in items.values_mut() {
                            if Self::rewrap_value(&mut dv.value, f)? {
                                changed = true;
                            }
                        }
                    }
                    if changed {
                        save(&*self.store, key, &st)?;
                        rewrapped += 1;
                    }
                }
            }
        }
        Ok(rewrapped)
    }

    fn rewrap_snapshot(
        snap: &mut SnapshotMap,
        f: &dyn Fn(&Ciphertext) -> Option<Result<Ciphertext, Error>>,
    ) -> Result<bool, Error> {
        let mut changed = false;
        for items in snap.values_mut() {
            for v in items.values_mut() {
                if Self::rewrap_value(v, f)? {
                    changed = true;
                }
            }
        }
        Ok(changed)
    }

    fn rewrap_value(
        v: &mut Value,
        f: &dyn Fn(&Ciphertext) -> Option<Result<Ciphertext, Error>>,
    ) -> Result<bool, Error> {
        if let Value::Secret(ct) = v {
            if let Some(res) = f(ct) {
                *v = Value::Secret(res?);
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub fn list_branches(&self, id: &ProjectId) -> Result<Vec<BranchName>, Error> {
        let prefix = format!("{K_PROJECT}{}{K_BRANCH}", id.as_str());
        let rows = self.get_prefix_merged(prefix.as_bytes())?;
        let mut out = Vec::new();
        for (k, _) in rows {
            let ks = String::from_utf8_lossy(&k);
            let rest = &ks[prefix.len()..];
            if let Some(pos) = rest.find('/') {
                let name = &rest[..pos];
                if !name.is_empty() {
                    out.push(BranchName(name.to_string()));
                }
            }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    /// 读取某版本的值快照（perf 方案② D3：checkpoint 版本存 full，其余存 diff，读时重建）。
    /// 定位最近 checkpoint（含自身）作为基座，从基座 + 1 应用到目标版本。
    pub fn snapshot_of(
        &self,
        id: &ProjectId,
        branch: &BranchName,
        version: u64,
    ) -> Result<SnapshotMap, Error> {
        // 边界：version 必须 ≥1（调用方保证：get_config 对 version=0 解析 active_version）
        if version == 0 {
            return Err(Error::not_found(format!("version 0 of {branch}")));
        }
        // 最近 checkpoint 基座（向下取整；v=1 恒 full）
        let start = if version.is_multiple_of(CHECKPOINT_INTERVAL) {
            version // 自身即 checkpoint：直接读 full，0 个 diff 应用
        } else {
            let base = ((version - 1) / CHECKPOINT_INTERVAL) * CHECKPOINT_INTERVAL;
            if base == 0 {
                1
            } else {
                base
            }
        };
        let base_key = snapshot_key(id, branch, start);
        let mut snap: SnapshotMap = match self.get_merged(base_key.as_bytes())? {
            Some(raw) => serde_json::from_slice(&raw)
                .map_err(|e| Error::internal(format!("corrupt snapshot {base_key}: {e}")))?,
            None => {
                // 兼容旧数据/裁剪后基座缺失：退化直读目标版本（旧版全量存储或兜底）
                let fallback = snapshot_key(id, branch, version);
                return match self.get_merged(fallback.as_bytes())? {
                    Some(raw) => serde_json::from_slice(&raw)
                        .map_err(|e| Error::internal(format!("corrupt snapshot {fallback}: {e}"))),
                    None => Err(Error::not_found(format!("version {version} of {branch}"))),
                };
            }
        };
        for v in (start + 1)..=version {
            if v % CHECKPOINT_INTERVAL == 0 {
                // checkpoint 版本存 full：直接替换基座
                let cp_key = snapshot_key(id, branch, v);
                snap = match self.get_merged(cp_key.as_bytes())? {
                    Some(raw) => serde_json::from_slice(&raw)
                        .map_err(|e| Error::internal(format!("corrupt snapshot {cp_key}: {e}")))?,
                    None => {
                        return Err(Error::not_found(format!("snapshot {v} of {branch}")));
                    }
                };
            } else {
                let dk = diff_key(id, branch, v);
                let diff: Vec<DiffEntry> = match self.get_merged(dk.as_bytes())? {
                    Some(raw) => serde_json::from_slice(&raw)
                        .map_err(|e| Error::internal(format!("corrupt diff {dk}: {e}")))?,
                    None => {
                        // 旧版本（升级前全量存储）无 diff_key：退化直读目标版本全量
                        let fallback = snapshot_key(id, branch, v);
                        return match self.get_merged(fallback.as_bytes())? {
                            Some(raw) => serde_json::from_slice(&raw).map_err(|e| {
                                Error::internal(format!("corrupt snapshot {fallback}: {e}"))
                            }),
                            None => Err(Error::not_found(format!("version {v} of {branch}"))),
                        };
                    }
                };
                Self::apply_diff(&mut snap, &diff);
            }
        }
        Ok(snap)
    }

    /// 应用 diff 到快照（确定性：BTreeMap 有序；Upsert 写、Delete 删）。
    /// Delete 删除 item 后若组变空则移除组（与全量快照的空组语义一致，避免残留空组）。
    fn apply_diff(snap: &mut SnapshotMap, diff: &[DiffEntry]) {
        for entry in diff {
            match entry.kind {
                ChangeKind::Upsert => {
                    if let Some(v) = &entry.new_value {
                        snap.entry(entry.group.clone())
                            .or_default()
                            .insert(entry.key.clone(), v.clone());
                    }
                }
                ChangeKind::Delete => {
                    if let Some(items) = snap.get_mut(&entry.group) {
                        items.remove(&entry.key);
                        if items.is_empty() {
                            snap.remove(&entry.group);
                        }
                    }
                }
            }
        }
    }

    /// 写版本快照（perf 方案② D3）：checkpoint（每 100 或首次）存 full，其余存 diff。
    /// 需同时传入 old 快照（compute_diff 的输入）；`record.kind` 会被覆写为 Full/Diff。
    fn write_version_snapshot(
        &mut self,
        id: &ProjectId,
        branch: &BranchName,
        vno: u64,
        old: &SnapshotMap,
        new: &SnapshotMap,
        record: &mut VersionRecord,
    ) -> Result<(), Error> {
        let is_checkpoint = vno == 1 || vno.is_multiple_of(CHECKPOINT_INTERVAL);
        if is_checkpoint {
            record.kind = VersionKind::Full;
            self.save_pending(&snapshot_key(id, branch, vno), new)?;
        } else {
            record.kind = VersionKind::Diff;
            let diff = compute_diff(old, new);
            self.save_pending(&diff_key(id, branch, vno), &diff)?;
        }
        self.save_pending(&version_key(id, branch, vno), record)
    }

    pub fn get_version_record(
        &self,
        id: &ProjectId,
        branch: &BranchName,
        no: u64,
    ) -> Result<Option<VersionRecord>, Error> {
        self.load_merged(&version_key(id, branch, no))
    }

    pub fn version_history(
        &self,
        id: &ProjectId,
        branch: &BranchName,
    ) -> Result<Vec<VersionRecord>, Error> {
        let prefix = format!(
            "{K_PROJECT}{}{K_BRANCH}{}{K_VERSION}",
            id.as_str(),
            branch.as_str()
        );
        let rows = self.get_prefix_merged(prefix.as_bytes())?;
        let mut out = Vec::new();
        for (k, v) in rows {
            let ks = String::from_utf8_lossy(&k);
            // 跳过快照与 diff 后缀（perf 方案② D3：snap/diff 与 version 同前缀）
            if ks.ends_with("/snap") || ks.ends_with("/diff") {
                continue;
            }
            if let Ok(r) = serde_json::from_slice::<VersionRecord>(&v) {
                out.push(r);
            }
        }
        out.sort_by_key(|r| r.no);
        Ok(out)
    }

    /// 导出全部状态（快照构建用）。
    pub fn dump_all(&self) -> Result<crate::store::KeyValuePairs, Error> {
        self.get_prefix_merged(b"")
    }

    /// 清空并恢复全部状态（快照安装用）。
    pub fn restore_all(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Error> {
        for (k, _) in self.get_prefix_merged(b"")? {
            self.store.delete(&k)?;
        }
        for (k, v) in pairs {
            self.store.put(k, v)?;
        }
        Ok(())
    }

    /// 版本裁剪：保留活动版本 + 最近 keep 个版本，删除更早的历史（后台任务用）。
    /// perf 方案② D3：同时删除 diff_key；且删除下限对齐到"最近保留 checkpoint 之前"——
    /// 保证最新保留版本是 checkpoint（full 基座），其后的 diff 链可完整重建。
    /// G2/Q5：灰度快照位于 gray-snap/ 独立前缀（Q1），不在本方法的 v/ 扫描范围内，
    /// 天然不会被裁剪（灰度客户端 resolve 目标永不 NotFound；Abort 后历史仍可查）。
    /// 灰度快照回收在 apply 路径（publish/promote/abort/结构发布 bump）删除旧序号快照（G5 后收口），
    /// 此处仅保证 v/ 裁剪不误删当前灰度快照。
    pub fn prune_versions(
        &self,
        project: &ProjectId,
        branch: &BranchName,
        keep: usize,
    ) -> Result<usize, Error> {
        let st = self
            .get_branch_state(project, branch)?
            .ok_or_else(|| Error::not_found(format!("branch {branch}")))?;
        let hist = self.version_history(project, branch)?; // 升序
        let total = hist.len();
        if total <= keep {
            return Ok(0);
        }
        // 目标：保留最近 keep 个版本。若裁剪导致最新保留版本不是 checkpoint，
        // 则额外保留其 checkpoint 基座（否则 diff 链断裂、历史全部不可读）。
        // 最新保留版本号 = total - keep（1-based 第 total-keep 个）；其基座 = 该版本向下取整到 checkpoint。
        let newest_kept_no = hist[total - keep - 1].no;
        let mut keep_from = newest_kept_no; // 语义上保留 >= keep_from 的版本
        if !newest_kept_no.is_multiple_of(CHECKPOINT_INTERVAL) && newest_kept_no != 1 {
            // 向下对齐到最近 checkpoint（含）——额外保留基座
            keep_from = ((newest_kept_no - 1) / CHECKPOINT_INTERVAL) * CHECKPOINT_INTERVAL;
            if keep_from == 0 {
                keep_from = 1;
            }
        }
        let mut removed = 0;
        for rec in hist.iter().take(total) {
            let no = rec.no;
            if no >= keep_from || no == st.active_version {
                continue; // 保留区间或活动版本
            }
            self.store
                .delete(version_key(project, branch, no).as_bytes())?;
            // 该版本可能存 full（checkpoint）或 diff——两个 key 都尝试删除（幂等）
            self.store
                .delete(snapshot_key(project, branch, no).as_bytes())?;
            self.store
                .delete(diff_key(project, branch, no).as_bytes())?;
            removed += 1;
        }
        Ok(removed)
    }

    /// GetConfig：version=0 取活动版本（稳定路径；灰度解析见 [`Self::get_config_resolved`]）。
    pub fn get_config(
        &self,
        id: &ProjectId,
        branch: &BranchName,
        version: u64,
    ) -> Result<ConfigSnapshot, Error> {
        let st = self
            .get_branch_state(id, branch)?
            .ok_or_else(|| Error::not_found(format!("branch {branch} of {id}")))?;
        let vno = if version == 0 {
            st.active_version
        } else {
            version
        };
        if vno == 0 {
            return Err(Error::not_found("no published version yet"));
        }
        let snap = self.snapshot_of(id, branch, vno)?;
        let structure = self.get_structure(id)?.unwrap_or(Structure {
            version: 0,
            groups: vec![],
        });
        Ok(ConfigSnapshot {
            project: id.to_string(),
            branch: branch.to_string(),
            version: vno,
            resolved_version: vno,
            structure_version: structure.version,
            groups: snap,
            gray: false,
        })
    }

    /// 数据面统一入口（G3/D27-D28）：`version=0` 按客户端身份 resolve 并分流读取；
    /// `version≠0` 显式版本（管理面/历史/reveal）恒走 v/ 空间、不 resolve（Q6 绕过）。
    pub fn get_config_resolved(
        &self,
        id: &ProjectId,
        branch: &BranchName,
        version: u64,
        ctx: &ClientCtx,
    ) -> Result<ConfigSnapshot, Error> {
        if version != 0 {
            return self.get_config(id, branch, version);
        }
        match self.resolve_version(id, branch, ctx)? {
            ResolvedVersion::Stable(_) => self.get_config(id, branch, 0),
            ResolvedVersion::Gray(seq) => {
                let snap = self.gray_snapshot_of(id, branch, seq)?;
                let structure = self.get_structure(id)?.unwrap_or(Structure {
                    version: 0,
                    groups: vec![],
                });
                // R1（审核修订）：version 保持 active_version（v/ 空间）——客户端 watch 游标
                // 不错位（after_version=active 增量重放正确）；resolved_version=gray_seq 标记
                // 内容实际来自哪个灰度快照；gray=true 提示客户端可见自己在灰度。
                let st = self
                    .get_branch_state(id, branch)?
                    .ok_or_else(|| Error::not_found(format!("branch {branch} of {id}")))?;
                Ok(ConfigSnapshot {
                    project: id.to_string(),
                    branch: branch.to_string(),
                    version: st.active_version,
                    resolved_version: seq,
                    structure_version: structure.version,
                    groups: snap,
                    gray: true,
                })
            }
        }
    }

    /// 读取灰度快照（gray-snap/{seq}，Q1 独立前缀；不存在 → NotFound）。
    /// G2 供 GrayPromote 取内容；G3 数据面按 resolve_version 结果读取。
    pub fn gray_snapshot_of(
        &self,
        id: &ProjectId,
        branch: &BranchName,
        seq: u64,
    ) -> Result<SnapshotMap, Error> {
        let key = gray_snap_key(id, branch, seq);
        match self.get_merged(key.as_bytes())? {
            Some(raw) => Ok(serde_json::from_slice(&raw)
                .map_err(|e| Error::internal(format!("corrupt gray snapshot {key}: {e}")))?),
            None => Err(Error::not_found(format!("gray snapshot {seq} of {branch}"))),
        }
    }

    // ---------------- 灰度解析（G2 读路径纯函数，D20：apply 不读请求，selector 求值在此） ----------------

    /// 灰度版本解析（G3/D24）：返回客户端应读取的版本（带语义，消除数值巧合歧义）。
    /// - 无灰度（gray_seq==0 / 规则 None）→ `Stable(active_version)`；
    /// - Q2：无身份（instance_id 空）永不进灰度 → `Stable(active_version)`；
    /// - 规则命中 → `Gray(gray_seq)`；未命中 → `Stable(active_version)`。
    pub fn resolve_version(
        &self,
        id: &ProjectId,
        branch: &BranchName,
        ctx: &ClientCtx,
    ) -> Result<ResolvedVersion, Error> {
        let st = self
            .get_branch_state(id, branch)?
            .ok_or_else(|| Error::not_found(format!("branch {branch} of {id}")))?;
        if st.gray_seq == 0 || st.gray_rule.is_none() {
            return Ok(ResolvedVersion::Stable(st.active_version));
        }
        // Q2：无身份永不进灰度（空 instance_id 哈希恒恒定，禁止参与分桶）
        if ctx.instance_id.is_empty() {
            return Ok(ResolvedVersion::Stable(st.active_version));
        }
        let rule = st.gray_rule.as_ref().expect("gray_rule checked is_some");
        if Self::rule_matches(rule, ctx) {
            Ok(ResolvedVersion::Gray(st.gray_seq))
        } else {
            Ok(ResolvedVersion::Stable(st.active_version))
        }
    }

    /// 规则求值（纯函数；固定次序 labels → IP → percent，任一命中即命中）。
    pub fn rule_matches(rule: &GrayRule, ctx: &ClientCtx) -> bool {
        // 1. 标签（OR：任一 key=value 命中）
        for sel in &rule.match_labels {
            if ctx.labels.get(&sel.key) == Some(&sel.value) {
                return true;
            }
        }
        // 2. IP（CIDR 段）
        if let Some(ip) = ctx.ip {
            for cidr in &rule.ip_cidrs {
                if Self::ip_in_cidr(ip, cidr) {
                    return true;
                }
            }
        }
        // 3. 百分比（fnv1a(instance_id) % 100 < pct；空身份防御性跳过）
        if let Some(pct) = rule.percentage {
            if !ctx.instance_id.is_empty() && Self::fnv1a_hash(&ctx.instance_id) % 100 < pct {
                return true;
            }
        }
        false
    }

    /// FNV-1a 32 位哈希（分桶用；确定性纯函数，无墙钟/随机/IO）。
    pub fn fnv1a_hash(s: &str) -> u32 {
        let mut h: u32 = 0x811c9dc5;
        for b in s.as_bytes() {
            h ^= u32::from(*b);
            h = h.wrapping_mul(0x0100_0193);
        }
        h
    }

    /// IP 是否落在 CIDR 段内（v4/v6 分族比较；非法段 → false）。
    fn ip_in_cidr(ip: std::net::IpAddr, cidr: &str) -> bool {
        let Some((addr_s, prefix_s)) = cidr.split_once('/') else {
            return false;
        };
        let Ok(addr) = addr_s.parse::<std::net::IpAddr>() else {
            return false;
        };
        let Ok(prefix) = prefix_s.parse::<u32>() else {
            return false;
        };
        match (ip, addr) {
            (std::net::IpAddr::V4(a), std::net::IpAddr::V4(b)) => {
                if prefix > 32 {
                    return false;
                }
                let mask = if prefix == 0 {
                    0u32
                } else {
                    u32::MAX << (32 - prefix)
                };
                (u32::from(a) & mask) == (u32::from(b) & mask)
            }
            (std::net::IpAddr::V6(a), std::net::IpAddr::V6(b)) => {
                if prefix > 128 {
                    return false;
                }
                let mask = if prefix == 0 {
                    0u128
                } else {
                    u128::MAX << (128 - prefix)
                };
                (u128::from(a) & mask) == (u128::from(b) & mask)
            }
            _ => false,
        }
    }

    /// 灰度规则校验（apply 时执行；规则是状态机数据，坏规则必须在写路径拒绝）：
    /// 至少一个判据；标签键值非空；CIDR 语法合法；百分比 ≤ 100。
    fn validate_gray_rule(rule: &GrayRule) -> Result<(), Error> {
        if rule.match_labels.is_empty() && rule.ip_cidrs.is_empty() && rule.percentage.is_none() {
            return Err(Error::validation(
                "gray rule must have at least one criterion (labels / ip / percentage)",
            ));
        }
        for sel in &rule.match_labels {
            if sel.key.is_empty() || sel.value.is_empty() {
                return Err(Error::validation(
                    "gray rule label key/value must be non-empty",
                ));
            }
        }
        for cidr in &rule.ip_cidrs {
            let Some((addr_s, prefix_s)) = cidr.split_once('/') else {
                return Err(Error::validation(format!("invalid CIDR: {cidr:?}")));
            };
            let Ok(addr) = addr_s.parse::<std::net::IpAddr>() else {
                return Err(Error::validation(format!("invalid CIDR: {cidr:?}")));
            };
            let Ok(prefix) = prefix_s.parse::<u32>() else {
                return Err(Error::validation(format!("invalid CIDR: {cidr:?}")));
            };
            let max_prefix = match addr {
                std::net::IpAddr::V4(_) => 32,
                std::net::IpAddr::V6(_) => 128,
            };
            if prefix > max_prefix {
                return Err(Error::validation(format!("invalid CIDR: {cidr:?}")));
            }
        }
        if let Some(pct) = rule.percentage {
            if pct > 100 {
                return Err(Error::validation(format!("gray percentage {pct} > 100")));
            }
        }
        Ok(())
    }

    /// 分支级单调分配器（Q1）：下一个版本号 = max(active, gray) + 1。
    /// 灰度与稳定版本号共享一条单调序列（存储空间分离：v/ 与 gray-snap/），
    /// 保证 promote 的新 active 号严格大于客户端见过的任何号（含 gray_seq）。
    fn next_monotonic(active: u64, gray: u64) -> u64 {
        active.max(gray) + 1
    }

    // ---------------- apply ----------------

    /// 命令载荷墙钟（API 层注入）；0 = 回退 apply 的 now_ms 参数（旧日志重放兼容）。
    fn eff_ts(ts: &i64, fallback: i64) -> i64 {
        if *ts > 0 {
            *ts
        } else {
            fallback
        }
    }

    /// 应用命令（perf 方案①）：命令级写缓冲——apply 内多次写合并为一次 write_batch 单事务。
    /// 失败时 abort（pending 清空，无部分写）；成功时 flush（一次 fsync）。
    pub fn apply(&mut self, cmd: &Command, now_ms: i64) -> ApplyOutcome {
        self.pending_ops.clear();
        let result = self.apply_inner(cmd, now_ms);
        match result {
            Ok(events) => {
                if let Err(e) = self.flush_pending() {
                    self.pending_ops.clear();
                    return Err(e);
                }
                Ok(events)
            }
            Err(e) => {
                // abort：丢弃未提交写（命令失败无部分生效，语义优于旧逐事务提交）
                self.pending_ops.clear();
                Err(e)
            }
        }
    }

    fn apply_inner(&mut self, cmd: &Command, now_ms: i64) -> ApplyOutcome {
        match cmd {
            Command::ProjectCreate { name, operator, ts } => {
                self.apply_project_create(name, Self::eff_ts(ts, now_ms), operator)
            }
            Command::ProjectDelete { id, operator } => self.apply_project_delete(id, operator),
            Command::BranchCreate {
                project,
                name,
                source,
                operator,
                ts,
            } => self.apply_branch_create(
                project,
                name,
                source.as_ref(),
                Self::eff_ts(ts, now_ms),
                operator,
            ),
            Command::BranchDelete {
                project,
                name,
                operator,
            } => self.apply_branch_delete(project, name, operator),
            Command::StructureDraftSet {
                project,
                base_version,
                groups,
                operator,
            } => self.apply_structure_draft_set(project, *base_version, groups, operator),
            Command::PublishStructure {
                project,
                comment,
                request_id,
                operator,
                ts,
                policy,
            } => self.apply_publish_structure(
                project,
                comment,
                request_id,
                Self::eff_ts(ts, now_ms),
                operator,
                *policy,
            ),
            Command::DraftUpdate {
                project,
                branch,
                updates,
                deletes,
                shared_bindings,
                operator,
                ts,
                expected_draft_rev,
            } => self.apply_draft_update(
                project,
                branch,
                updates,
                deletes,
                shared_bindings,
                Self::eff_ts(ts, now_ms),
                operator,
                expected_draft_rev.as_ref(),
            ),
            Command::Publish {
                project,
                branch,
                comment,
                request_id,
                operator,
                ts,
                policy,
            } => self.apply_publish(
                project,
                branch,
                comment,
                request_id,
                Self::eff_ts(ts, now_ms),
                operator,
                *policy,
            ),
            Command::Rollback {
                project,
                branch,
                to_version,
                comment,
                request_id,
                operator,
                ts,
            } => self.apply_rollback(
                project,
                branch,
                *to_version,
                comment,
                request_id,
                Self::eff_ts(ts, now_ms),
                operator,
            ),
            Command::SharedDraftUpdate { item, operator } => {
                self.apply_shared_draft_update(item, operator)
            }
            Command::SharedPublish {
                comment,
                request_id,
                operator,
                ts,
                cascade,
                policy,
            } => self.apply_shared_publish(
                comment,
                request_id,
                Self::eff_ts(ts, now_ms),
                operator,
                *cascade,
                *policy,
            ),
            Command::SharedDelete { key, operator } => self.apply_shared_delete(key, operator),
            Command::SessionLogin {
                token_hash,
                issued_at,
                expires_at,
            } => self.apply_session_login(token_hash, *issued_at, *expires_at),
            Command::SessionLogout => self.apply_session_logout(),
            Command::SessionHeartbeat { expires_at } => self.apply_session_heartbeat(*expires_at),
            Command::ProjectAdminCreate {
                project,
                username,
                salt,
                password_hash,
                ts,
            } => self.apply_project_admin_create(
                project,
                username,
                salt,
                password_hash,
                Self::eff_ts(ts, now_ms),
            ),
            Command::ProjectAdminDelete { username } => self.apply_project_admin_delete(username),
            Command::ProjectAdminSetPassword {
                username,
                salt,
                password_hash,
            } => self.apply_project_admin_set_password(username, salt, password_hash),
            Command::PaSessionLogin {
                username,
                token_hash,
                issued_at,
                expires_at,
                device_id,
            } => self.apply_pa_session_login(
                username,
                token_hash,
                *issued_at,
                *expires_at,
                device_id,
            ),
            Command::PaSessionLogout { username } => self.apply_pa_session_logout(username),
            Command::PaSessionHeartbeat {
                username,
                expires_at,
            } => self.apply_pa_session_heartbeat(username, *expires_at),
            Command::AdminSetPassword { password_hash } => {
                self.apply_admin_set_password(password_hash)
            }
            Command::MultiSessionLogin {
                token_hash,
                issued_at,
                expires_at,
                session_id,
            } => self.apply_multi_session_login(token_hash, *issued_at, *expires_at, session_id),
            Command::MultiSessionLogout { session_id } => {
                self.apply_multi_session_logout(session_id)
            }
            Command::MultiSessionHeartbeat {
                session_id,
                expires_at,
            } => self.apply_multi_session_heartbeat(session_id, *expires_at),
            Command::MultiPaSessionLogin {
                username,
                token_hash,
                issued_at,
                expires_at,
                device_id,
                session_id,
            } => self.apply_multi_pa_session_login(
                username,
                token_hash,
                *issued_at,
                *expires_at,
                device_id,
                session_id,
            ),
            Command::MultiPaSessionLogout {
                username,
                session_id,
            } => self.apply_multi_pa_session_logout(username, session_id),
            Command::MultiPaSessionHeartbeat {
                username,
                session_id,
                expires_at,
            } => self.apply_multi_pa_session_heartbeat(username, session_id, *expires_at),
            Command::MultiSessionLogoutAll => self.apply_multi_session_logout_all(),
            Command::MultiPaSessionLogoutAll { username } => {
                self.apply_multi_pa_session_logout_all(username)
            }
            Command::AuditAppend { entry } => self.apply_audit_append(entry),
            Command::RotateMasterKey { .. } => self.apply_rotate_master_key(),
            Command::GrayPublish {
                project,
                branch,
                rule,
                comment,
                request_id,
                operator,
                ts,
                policy,
            } => self.apply_gray_publish(
                project,
                branch,
                rule,
                comment,
                request_id,
                Self::eff_ts(ts, now_ms),
                operator,
                *policy,
            ),
            Command::GrayPromote {
                project,
                branch,
                comment,
                request_id,
                operator,
                ts,
            } => self.apply_gray_promote(
                project,
                branch,
                comment,
                request_id,
                Self::eff_ts(ts, now_ms),
                operator,
            ),
            Command::GrayAbort {
                project,
                branch,
                comment,
                request_id,
                operator,
                ts,
            } => self.apply_gray_abort(
                project,
                branch,
                comment,
                request_id,
                Self::eff_ts(ts, now_ms),
                operator,
            ),
            Command::ProjectTokenCreate {
                project,
                name,
                token_hash,
                operator,
                ts,
            } => self.apply_project_token_create(
                project,
                name,
                token_hash,
                operator,
                Self::eff_ts(ts, now_ms),
            ),
            Command::ProjectTokenRevoke { project, token_id } => {
                self.apply_project_token_revoke(project, token_id)
            }
        }
    }

    fn apply_project_create(&mut self, name: &str, now_ms: i64, _operator: &str) -> ApplyOutcome {
        if !valid_name(name) {
            return Err(Error::validation(format!("invalid project name: {name:?}")));
        }
        // N2：限额表 MAX_PROJECTS 强制（此前为死常量，未实施）
        if self.list_projects()?.len() >= MAX_PROJECTS {
            return Err(Error::limit_exceeded("too many projects"));
        }
        let id = ProjectId(name.to_string());
        if self.get_project(&id)?.is_some() {
            return Err(Error::conflict(format!("project {name} already exists")));
        }
        let project = Project {
            id: id.clone(),
            name: name.to_string(),
            created_at: now_ms,
        };
        let structure = Structure {
            version: 1,
            groups: vec![],
        };
        self.save_pending(&project_key(&id), &project)?;
        self.save_pending(&idx_pname(name), &"1")?;
        self.save_pending(&struct_key(&id), &structure)?;
        for default_branch in [BranchName::DEV, BranchName::TEST, BranchName::PROD] {
            self.save_pending(
                &branch_state_key(&id, &BranchName(default_branch.to_string())),
                &BranchState::new(1),
            )?;
        }
        Ok(vec![])
    }

    fn apply_project_delete(&mut self, id: &ProjectId, _operator: &str) -> ApplyOutcome {
        let project = self
            .get_project(id)?
            .ok_or_else(|| Error::not_found(format!("project {id}")))?;
        let prefix = project_key(id);
        for (k, _) in self.get_prefix_merged(prefix.as_bytes())? {
            self.delete_pending(&k)?;
        }
        self.delete_pending(idx_pname(&project.name).as_bytes())?;
        // 级联删除该项目全部项目管理员账号及其会话（设计 §5；多会话双删）
        for acct in self.list_project_admins(&id.0)? {
            self.delete_all_pa_sessions(&acct.username)?;
            self.delete_pending(project_admin_key(&acct.username).as_bytes())?;
        }
        // 级联删除该项目全部访问令牌（扁平 tok/ 前缀过滤项目，不在 p/{pid} 下，必须显式清理）
        for (k, raw) in self.get_prefix_merged(K_DATA_TOKEN.as_bytes())? {
            if let Ok(rec) = serde_json::from_slice::<ProjectTokenRecord>(&raw) {
                if rec.project == *id {
                    self.delete_pending(&k)?;
                }
            }
        }
        // 共享引用已内嵌项目结构（随项目前缀一并删除），无需清理独立引用索引。
        Ok(vec![])
    }

    fn apply_branch_create(
        &mut self,
        id: &ProjectId,
        name: &BranchName,
        source: Option<&BranchName>,
        now_ms: i64,
        _operator: &str,
    ) -> ApplyOutcome {
        if !valid_branch(name.as_str()) {
            return Err(Error::validation(format!("invalid branch name: {name:?}")));
        }
        self.get_project(id)?
            .ok_or_else(|| Error::not_found(format!("project {id}")))?;
        if self.get_branch_state(id, name)?.is_some() {
            return Err(Error::conflict(format!("branch {name} exists")));
        }
        let branches = self.list_branches(id)?;
        if branches.len() >= MAX_BRANCHES_PER_PROJECT {
            return Err(Error::limit_exceeded("too many branches"));
        }
        let structure = self.get_structure(id)?.unwrap_or(Structure {
            version: 1,
            groups: vec![],
        });
        let mut state = BranchState::new(structure.version);
        if let Some(src) = source {
            let src_state = self
                .get_branch_state(id, src)?
                .ok_or_else(|| Error::validation(format!("source branch {src} not found")))?;
            if src_state.active_version == 0 {
                return Err(Error::validation(format!(
                    "source branch {src} has no published version"
                )));
            }
            let snap = self.snapshot_of(id, src, src_state.active_version)?;
            // 跳过结构标记 shared=true 的 item（避免物化值变成引用项本地草稿）；
            // 继承源分支的共享引用绑定（设计 shared-ref-branch-scope §4.8）
            let shared_items: std::collections::HashSet<(String, String)> = structure
                .groups
                .iter()
                .flat_map(|g| {
                    g.items
                        .iter()
                        .filter(|i| i.shared)
                        .map(|i| (g.name.clone(), i.key.clone()))
                })
                .collect();
            state.value_draft = snap
                .into_iter()
                .map(|(g, items)| {
                    let m: BTreeMap<String, DraftValue> = items
                        .into_iter()
                        .filter(|(k, _)| !shared_items.contains(&(g.clone(), k.clone())))
                        .map(|(k, v)| {
                            (
                                k,
                                DraftValue {
                                    value: v,
                                    updated_at: now_ms,
                                },
                            )
                        })
                        .collect();
                    (g, m)
                })
                .filter(|(_, m)| !m.is_empty())
                .collect();
            state.shared_bindings = src_state.shared_bindings.clone();
            state.bindings_dirty = false;
        }
        self.save_pending(&branch_state_key(id, name), &state)?;
        Ok(vec![])
    }

    fn apply_branch_delete(
        &mut self,
        id: &ProjectId,
        name: &BranchName,
        _operator: &str,
    ) -> ApplyOutcome {
        let st = self
            .get_branch_state(id, name)?
            .ok_or_else(|| Error::not_found(format!("branch {name} of {id}")))?;
        if st.active_version > 0 || !st.value_draft.is_empty() {
            return Err(Error::conflict(
                "branch has published versions or pending draft",
            ));
        }
        let prefix = branch_prefix(id, name);
        for (k, _) in self.get_prefix_merged(prefix.as_bytes())? {
            self.delete_pending(&k)?;
        }
        Ok(vec![])
    }

    fn apply_structure_draft_set(
        &mut self,
        id: &ProjectId,
        base_version: u64,
        groups: &[GroupDef],
        _operator: &str,
    ) -> ApplyOutcome {
        let structure = self
            .get_structure(id)?
            .ok_or_else(|| Error::not_found(format!("project {id}")))?;
        if base_version != structure.version {
            return Err(Error::conflict(format!(
                "base_version {base_version} != current structure version {}",
                structure.version
            )));
        }
        let draft_structure = Structure {
            version: base_version,
            groups: groups.to_vec(),
        };
        let errs = validator::validate_structure(&draft_structure);
        if !errs.is_empty() {
            return Err(Error::publish_blocked(
                serde_json::json!({ "errors": errs }),
            ));
        }
        let draft = StructureDraft {
            base_version,
            groups: groups.to_vec(),
        };
        self.save_pending(&struct_draft_key(id), &draft)?;
        Ok(vec![])
    }

    fn apply_publish_structure(
        &mut self,
        id: &ProjectId,
        comment: &str,
        request_id: &str,
        now_ms: i64,
        operator: &str,
        policy: PublishPolicy,
    ) -> ApplyOutcome {
        let structure = self
            .get_structure(id)?
            .ok_or_else(|| Error::not_found(format!("project {id}")))?;
        let draft = self
            .get_structure_draft(id)?
            .ok_or_else(|| Error::new(ErrorKind::NoDraft, "no structure draft"))?;
        if draft.base_version != structure.version {
            return Err(Error::conflict("structure draft base_version mismatch"));
        }
        let draft_structure = Structure {
            version: structure.version,
            groups: draft.groups.clone(),
        };
        let errs = validator::validate_structure(&draft_structure);
        // G1/D35：Warn 时校验失败仅记录继续（策略编码进命令，确定性由日志序保证）
        if !errs.is_empty() && policy == PublishPolicy::Block {
            return Err(Error::publish_blocked(
                serde_json::json!({ "errors": errs }),
            ));
        }
        let new_structure = Structure {
            version: structure.version + 1,
            groups: draft.groups.clone(),
        };
        let mut events = Vec::new();
        let branches = self.list_branches(id)?;
        for branch in &branches {
            let mut st = self
                .get_branch_state(id, branch)?
                .ok_or_else(|| Error::internal("branch state missing"))?;
            // Q1：分支级单调分配器——灰度活跃时新 active 号严格大于 gray_seq；
            // 灰度快照同步 bump 分配另一个不同号（D23：灰度期间结构演进不中断观察）。
            let vno = Self::next_monotonic(st.active_version, st.gray_seq);
            let gray_next = vno + 1; // 与稳定号不同；且恒 > 旧 gray_seq
                                     // 结构发布：值不变（D14 只清理被删 item 的草稿值）
            let values = if st.active_version == 0 {
                SnapshotMap::new()
            } else {
                self.snapshot_of(id, branch, st.active_version)?
            };
            let mut record = VersionRecord {
                no: vno,
                structure_version: new_structure.version,
                created_at: now_ms,
                operator: Self::operator_id(operator),
                comment: comment.to_string(),
                rollback_of: None,
                kind: VersionKind::Full,
                snapshot_ref: None,
                diff_ref: None,
                event_ty: Some(EventType::StructurePublish),
                gray: false,
            };
            // 结构发布值不变：old==values==new → diff 恒空（checkpoint 规则仍按 vno）
            self.write_version_snapshot(id, branch, vno, &values, &values, &mut record)?;
            st.active_version = vno;
            st.structure_version = new_structure.version;
            // D23：灰度活跃 → 灰度快照同步 bump（内容不变、序号前移；structure_version
            // 取 BranchState 最新值，灰度客户端重拉后拿到结构一致的灰度快照）。
            if st.gray_seq > 0 {
                let old_gray_seq = st.gray_seq;
                let gray_snap = self.gray_snapshot_of(id, branch, old_gray_seq)?;
                self.save_pending(&gray_snap_key(id, branch, gray_next), &gray_snap)?;
                // 回收：旧灰度快照键（序号前移后旧键不再引用）
                self.delete_pending(gray_snap_key(id, branch, old_gray_seq).as_bytes())?;
                st.gray_seq = gray_next;
            }
            // D14：清理结构发布后不存在的 item 草稿值
            let mut known: BTreeMap<String, BTreeMap<String, ()>> = BTreeMap::new();
            for g in &new_structure.groups {
                for item in &g.items {
                    known
                        .entry(g.name.clone())
                        .or_default()
                        .insert(item.key.clone(), ());
                }
            }
            st.value_draft.retain(|g, items| {
                known.contains_key(g) && {
                    items.retain(|k, _| known[g].contains_key(k));
                    !items.is_empty()
                }
            });
            // D14 扩展：引用项只读——清理 shared 项的既有草稿值（值由共享库物化；含 local→shared 翻转）
            for g in &new_structure.groups {
                for item in &g.items {
                    if item.shared {
                        if let Some(m) = st.value_draft.get_mut(&g.name) {
                            m.remove(&item.key);
                        }
                    }
                }
            }
            // 分支级绑定清理：仅保留仍在结构中、仍 shared=true、且绑定共享项类型与新结构 ty 一致的条目
            // （删除 item / shared→local 翻转 / ty 变更致失配 → 绑定丢弃，分支需重新选择）
            let new_shared: std::collections::HashMap<(String, String), ValueType> = new_structure
                .groups
                .iter()
                .flat_map(|g| {
                    g.items
                        .iter()
                        .filter(|i| i.shared)
                        .map(|i| ((g.name.clone(), i.key.clone()), i.ty))
                })
                .collect();
            st.shared_bindings.retain(|g, m| {
                m.retain(|k, rk| {
                    match new_shared.get(&(g.clone(), k.clone())) {
                        None => false, // item 已删除或不再 shared
                        Some(ty) => self
                            .get_shared(rk)
                            .ok()
                            .flatten()
                            .map(|s| s.ty == *ty)
                            .unwrap_or(false), // 类型失配或共享项缺失 → 丢弃
                    }
                });
                !m.is_empty()
            });
            self.save_pending(&branch_state_key(id, branch), &st)?;
            events.push(PublishEvent {
                project: id.clone(),
                branch: branch.clone(),
                version: vno,
                ty: EventType::StructurePublish,
                structure_version: new_structure.version,
                comment: comment.to_string(),
                request_id: request_id.to_string(),
                changes: vec![],
                gray: false,
            });
        }
        self.save_pending(&struct_key(id), &new_structure)?;
        self.delete_pending(struct_draft_key(id).as_bytes())?;
        Ok(events)
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_draft_update(
        &mut self,
        id: &ProjectId,
        branch: &BranchName,
        updates: &[crate::command::DraftUpdateItem],
        deletes: &[(String, String)],
        bindings: &[crate::command::SharedBinding],
        now_ms: i64,
        _operator: &str,
        expected_draft_rev: Option<&u64>,
    ) -> ApplyOutcome {
        let mut st = self
            .get_branch_state(id, branch)?
            .ok_or_else(|| Error::not_found(format!("branch {branch} of {id}")))?;
        // 乐观锁（并发编辑冲突检测）：expected_draft_rev > 0 时校验 == 当前草稿修订号，
        // 不匹配 → Conflict（客户端须刷新最新草稿后重试）。0 = 旧客户端/旧日志，不校验。
        if let Some(exp) = expected_draft_rev {
            if *exp != st.draft_rev {
                return Err(Error::conflict(format!(
                    "草稿已被他人修改（draft_rev {} != expected {exp}），请刷新后重试",
                    st.draft_rev
                )));
            }
        }
        let structure = self
            .get_structure(id)?
            .ok_or_else(|| Error::not_found(format!("project {id}")))?;

        // 建立 group → item 定义索引
        let mut index: BTreeMap<String, BTreeMap<String, &ItemDef>> = BTreeMap::new();
        for g in &structure.groups {
            for item in &g.items {
                index
                    .entry(g.name.clone())
                    .or_default()
                    .insert(item.key.clone(), item);
            }
        }

        let mut total = st.value_draft.values().map(|m| m.len()).sum::<usize>();
        for u in updates {
            let def = index
                .get(&u.group)
                .and_then(|m| m.get(&u.key))
                .ok_or_else(|| Error::validation(format!("unknown item {}/{}", u.group, u.key)))?;
            // 引用项只读：值由共享库物化，禁止分支草稿设置本地值（选择引用走 shared_bindings）
            if def.shared {
                return Err(Error::validation(format!(
                    "item {}/{} 引用共享项，不可设置本地值",
                    u.group, u.key
                )));
            }
            let errs = validator::validate_value(def, &u.value);
            if !errs.is_empty() {
                return Err(Error::validation(errs.join("; ")));
            }
            let size = serde_json::to_vec(&u.value)
                .map_err(|e| Error::internal(format!("serialize value: {e}")))?
                .len();
            if size > MAX_VALUE_BYTES {
                return Err(Error::limit_exceeded("value too large"));
            }
            let fresh = !st
                .value_draft
                .get(&u.group)
                .is_some_and(|m| m.contains_key(&u.key));
            if fresh {
                total += 1;
                if total > MAX_ITEMS_PER_PROJECT {
                    return Err(Error::limit_exceeded("too many draft items"));
                }
            }
        }
        // 分支级共享引用绑定：upsert/解除（空 shared_key = 解除）；def 须存在且 shared=true。
        // 仅在实际变更时置 bindings_dirty（设计 shared-ref-branch-scope §4.6）。
        let mut bindings_changed = false;
        for b in bindings {
            let def = index
                .get(&b.group)
                .and_then(|m| m.get(&b.key))
                .ok_or_else(|| Error::validation(format!("unknown item {}/{}", b.group, b.key)))?;
            if !def.shared {
                return Err(Error::validation(format!(
                    "item {}/{} 未标记为引用共享，不可绑定共享项",
                    b.group, b.key
                )));
            }
            let cur = st
                .shared_bindings
                .get(&b.group)
                .and_then(|m| m.get(&b.key))
                .map(|s| s.as_str());
            if b.shared_key.is_empty() {
                if cur.is_some() {
                    bindings_changed = true;
                    if let Some(m) = st.shared_bindings.get_mut(&b.group) {
                        m.remove(&b.key);
                        if m.is_empty() {
                            st.shared_bindings.remove(&b.group);
                        }
                    }
                }
                continue;
            }
            if !validator::valid_key_name(&b.shared_key) {
                return Err(Error::validation(format!(
                    "invalid shared key {:?}: only [A-Za-z0-9._-] allowed",
                    b.shared_key
                )));
            }
            let shared = self
                .get_shared(&b.shared_key)?
                .ok_or_else(|| Error::validation(format!("shared item {} 未发布", b.shared_key)))?;
            if shared.ty != def.ty {
                return Err(Error::validation(format!(
                    "{}/{}: type {:?} 与共享项 {} 的 {:?} 不一致",
                    b.group, b.key, def.ty, b.shared_key, shared.ty
                )));
            }
            if cur != Some(b.shared_key.as_str()) {
                bindings_changed = true;
            }
            st.shared_bindings
                .entry(b.group.clone())
                .or_default()
                .insert(b.key.clone(), b.shared_key.clone());
        }
        if bindings_changed {
            st.bindings_dirty = true;
        }
        for (g, key) in deletes {
            if let Some(m) = st.value_draft.get_mut(g) {
                m.remove(key);
            }
        }
        for u in updates {
            st.value_draft.entry(u.group.clone()).or_default().insert(
                u.key.clone(),
                DraftValue {
                    value: u.value.clone(),
                    updated_at: now_ms,
                },
            );
        }
        // 乐观锁：草稿修订号 +1（无论是否校验，提交即推进；发布时保持 rev 供下次编辑锚定）
        st.draft_rev += 1;
        self.save_pending(&branch_state_key(id, branch), &st)?;
        Ok(vec![])
    }

    /// 物化草稿为已解析快照（apply_publish 与 apply_gray_publish 共用，行为一致）：
    /// 完整性校验（G1/D35：Block=拒绝 / Warn=仅记录继续）+ 草稿值 + 共享库引用补全。
    /// 返回 (快照, 校验告警列表——Warn 模式下非空)。
    fn materialize_resolved(
        &self,
        st: &BranchState,
        structure: &Structure,
        old: &SnapshotMap,
        policy: PublishPolicy,
    ) -> Result<(SnapshotMap, Vec<String>), Error> {
        // 完整性校验（G1/D35：策略编码进命令——确定性由日志序保证）。
        // 注意：共享解析（未绑定/悬空/类型失配）产生的 errs 也在本策略判定范围内——
        // 判定须在完整 errs 收集之后（未绑定是分支级正常态，Block 必须拦住）。
        let mut draft_map: BTreeMap<String, BTreeMap<String, DraftValue>> = st.value_draft.clone();
        // secret 保留语义（与草稿页「留空不修改」一致）：草稿未给 secret 新值时，
        // 沿用已发布快照中的密文，避免每次发布必填 secret 都要重输；非 secret 仍以草稿为完整快照。
        for g in &structure.groups {
            for item in &g.items {
                if item.shared || item.ty != ValueType::Secret {
                    continue;
                }
                if draft_map
                    .get(&g.name)
                    .and_then(|m| m.get(&item.key))
                    .is_some()
                {
                    continue;
                }
                if let Some(v) = old.get(&g.name).and_then(|m| m.get(&item.key)) {
                    draft_map.entry(g.name.clone()).or_default().insert(
                        item.key.clone(),
                        DraftValue {
                            value: v.clone(),
                            updated_at: 0,
                        },
                    );
                }
            }
        }
        let mut errs = validator::validate_publish(&draft_map, structure);

        // 物化：草稿值 + 共享引用（引用项只读：值来自本分支 shared_bindings 选定的共享项）
        let mut resolved: SnapshotMap = draft_map
            .into_iter()
            .map(|(g, items)| {
                let m = items.into_iter().map(|(k, dv)| (k, dv.value)).collect();
                (g, m)
            })
            .collect();
        for g in &structure.groups {
            for item in &g.items {
                if !item.shared {
                    continue;
                }
                let rk = st
                    .shared_bindings
                    .get(&g.name)
                    .and_then(|m| m.get(&item.key));
                let Some(rk) = rk else {
                    errs.push(format!("{}/{}: 未选择引用共享项", g.name, item.key));
                    continue;
                };
                match self.get_shared(rk)? {
                    Some(shared) => {
                        // 防御性类型复查：结构 ty 在绑定后被修改的残留（正常流程不可达，结构发布已清失配绑定）
                        if shared.ty != item.ty {
                            errs.push(format!(
                                "{}/{}: 共享项 {rk} 类型 {:?} 与结构声明 {:?} 不一致",
                                g.name, item.key, shared.ty, item.ty
                            ));
                            continue;
                        }
                        resolved
                            .entry(g.name.clone())
                            .or_default()
                            .insert(item.key.clone(), shared.value.clone());
                    }
                    None => errs.push(format!(
                        "{}/{}: shared item {rk} 缺失（悬空引用）",
                        g.name, item.key
                    )),
                }
            }
        }
        // 策略判定在完整 errs 收集之后（含共享解析错误：未绑定/悬空/类型失配）
        if !errs.is_empty() && policy == PublishPolicy::Block {
            return Err(Error::publish_blocked(
                serde_json::json!({ "errors": errs }),
            ));
        }
        Ok((resolved, errs))
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_publish(
        &mut self,
        id: &ProjectId,
        branch: &BranchName,
        comment: &str,
        request_id: &str,
        now_ms: i64,
        operator: &str,
        policy: PublishPolicy,
    ) -> ApplyOutcome {
        let mut st = self
            .get_branch_state(id, branch)?
            .ok_or_else(|| Error::not_found(format!("branch {branch} of {id}")))?;
        let structure = self
            .get_structure(id)?
            .ok_or_else(|| Error::not_found(format!("project {id}")))?;

        // 幂等（I10）：同 request_id 直接返回（已生效，不重复）
        if st.last_request_id.as_deref() == Some(request_id) {
            return Ok(vec![]);
        }
        // 发布守卫：值草稿非空或有未发布的绑定变更（只改绑定的分支也可发布）
        if st.value_draft.is_empty() && !st.bindings_dirty {
            return Err(Error::new(ErrorKind::NoDraft, "no pending draft"));
        }

        // 完整性校验 + 物化（草稿值 + 共享库引用）
        let old = if st.active_version == 0 {
            SnapshotMap::new()
        } else {
            self.snapshot_of(id, branch, st.active_version)?
        };
        let (resolved, _warnings) = self.materialize_resolved(&st, &structure, &old, policy)?;
        let diff = compute_diff(&old, &resolved);

        let vno = st.active_version + 1;
        let mut record = VersionRecord {
            no: vno,
            structure_version: structure.version,
            created_at: now_ms,
            operator: Self::operator_id(operator),
            comment: comment.to_string(),
            rollback_of: None,
            kind: VersionKind::Full,
            snapshot_ref: None,
            diff_ref: None,
            event_ty: Some(EventType::ValuePublish),
            gray: false,
        };
        self.write_version_snapshot(id, branch, vno, &old, &resolved, &mut record)?;
        st.active_version = vno;
        st.last_request_id = Some(request_id.to_string());
        st.value_draft.clear();
        // 绑定常驻分支状态（值草稿清空但绑定不清——shared 项无本地值可清）；发布后脏标记复位
        st.bindings_dirty = false;
        self.save_pending(&branch_state_key(id, branch), &st)?;

        Ok(vec![PublishEvent {
            project: id.clone(),
            branch: branch.clone(),
            version: vno,
            ty: EventType::ValuePublish,
            structure_version: structure.version,
            comment: comment.to_string(),
            request_id: request_id.to_string(),
            changes: diff,
            gray: false,
        }])
    }

    // ---------------- 回滚（I6/I9） ----------------

    #[allow(clippy::too_many_arguments)]
    fn apply_rollback(
        &mut self,
        project: &ProjectId,
        branch: &BranchName,
        to_version: u64,
        comment: &str,
        request_id: &str,
        now_ms: i64,
        operator: &str,
    ) -> ApplyOutcome {
        let mut st = self
            .get_branch_state(project, branch)?
            .ok_or_else(|| Error::not_found(format!("branch {branch} of {project}")))?;
        // 幂等（I10）
        if st.last_request_id.as_deref() == Some(request_id) {
            return Ok(vec![]);
        }
        if to_version == 0 || to_version >= st.active_version {
            return Err(Error::validation(format!(
                "to_version {to_version} must be 0 < v < active {}",
                st.active_version
            )));
        }
        let snap = self.snapshot_of(project, branch, to_version)?; // 不存在 → NotFound
        let old = if st.active_version == 0 {
            SnapshotMap::new()
        } else {
            self.snapshot_of(project, branch, st.active_version)?
        };
        let diff = compute_diff(&old, &snap);
        let vno = st.active_version + 1;
        let mut record = VersionRecord {
            no: vno,
            structure_version: st.structure_version,
            created_at: now_ms,
            operator: Self::operator_id(operator),
            comment: comment.to_string(),
            rollback_of: Some(to_version),
            kind: VersionKind::Full,
            snapshot_ref: None,
            diff_ref: None,
            event_ty: Some(EventType::Rollback),
            gray: false,
        };
        self.write_version_snapshot(project, branch, vno, &old, &snap, &mut record)?;
        st.active_version = vno;
        st.last_request_id = Some(request_id.to_string());
        self.save_pending(&branch_state_key(project, branch), &st)?;
        Ok(vec![PublishEvent {
            project: project.clone(),
            branch: branch.clone(),
            version: vno,
            ty: EventType::Rollback,
            structure_version: st.structure_version,
            comment: comment.to_string(),
            request_id: request_id.to_string(),
            changes: diff,
            gray: false,
        }])
    }

    // ---------------- 灰度发布（G2，纯新增命令；Q1 独立灰度序号 / Q3 gray 标记 / Q4 事件语义） ----------------

    /// 灰度发布：固化草稿 → 灰度快照（gray-snap/{seq}）+ 设置灰度规则。
    /// 与普通发布共用物化/校验（materialize_resolved）；稳定版 active_version 不动。
    #[allow(clippy::too_many_arguments)]
    fn apply_gray_publish(
        &mut self,
        id: &ProjectId,
        branch: &BranchName,
        rule: &GrayRule,
        comment: &str,
        request_id: &str,
        _now_ms: i64,
        _operator: &str,
        policy: PublishPolicy,
    ) -> ApplyOutcome {
        Self::validate_gray_rule(rule)?;
        let mut st = self
            .get_branch_state(id, branch)?
            .ok_or_else(|| Error::not_found(format!("branch {branch} of {id}")))?;
        let structure = self
            .get_structure(id)?
            .ok_or_else(|| Error::not_found(format!("project {id}")))?;

        // 幂等（I10）：同 request_id 直接返回（已生效，不重复）
        if st.last_request_id.as_deref() == Some(request_id) {
            return Ok(vec![]);
        }
        // 发布守卫：值草稿非空或有未发布的绑定变更（只改绑定的分支也可发布）
        if st.value_draft.is_empty() && !st.bindings_dirty {
            return Err(Error::new(ErrorKind::NoDraft, "no pending draft"));
        }

        // 完整性校验 + 物化（草稿值 + 共享库引用；与普通发布同一路径）
        let old = if st.active_version == 0 {
            SnapshotMap::new()
        } else {
            self.snapshot_of(id, branch, st.active_version)?
        };
        let (gray_snap, _warnings) = self.materialize_resolved(&st, &structure, &old, policy)?;
        let diff = compute_diff(&old, &gray_snap);

        // Q1：独立灰度序号 + 独立前缀（gray-snap/），与 active_version 版本号空间完全隔离
        let old_seq = st.gray_seq;
        st.gray_seq += 1;
        let seq = st.gray_seq;
        self.save_pending(&gray_snap_key(id, branch, seq), &gray_snap)?;
        // 回收：旧灰度快照（若有）不再被引用，删除防累积
        if old_seq > 0 {
            self.delete_pending(gray_snap_key(id, branch, old_seq).as_bytes())?;
        }
        st.gray_rule = Some(rule.clone());
        st.last_request_id = Some(request_id.to_string());
        st.value_draft.clear();
        // 绑定常驻分支状态；发布后脏标记复位
        st.bindings_dirty = false;
        self.save_pending(&branch_state_key(id, branch), &st)?;

        Ok(vec![PublishEvent {
            project: id.clone(),
            branch: branch.clone(),
            version: st.active_version, // 稳定版未动；事件版本供稳定客户端（gray=false 过滤）
            ty: EventType::ValuePublish,
            structure_version: structure.version,
            comment: comment.to_string(),
            request_id: request_id.to_string(),
            changes: diff,
            gray: true, // Q3：复用 EventType + gray 标记；SDK 契约：gray 事件永不按版本过滤
        }])
    }

    /// 灰度转正：读灰度快照 → 写新 active_version（next = max(active, gray)+1，Q1）→ 清灰度。
    /// 事件 gray=true 携带新 active 版本号：灰度客户端收到后无条件重拉（Q4 补发语义）。
    fn apply_gray_promote(
        &mut self,
        id: &ProjectId,
        branch: &BranchName,
        comment: &str,
        request_id: &str,
        now_ms: i64,
        operator: &str,
    ) -> ApplyOutcome {
        let mut st = self
            .get_branch_state(id, branch)?
            .ok_or_else(|| Error::not_found(format!("branch {branch} of {id}")))?;

        // 幂等（I10）
        if st.last_request_id.as_deref() == Some(request_id) {
            return Ok(vec![]);
        }
        if st.gray_seq == 0 || st.gray_rule.is_none() {
            return Err(Error::validation(format!(
                "no active gray on branch {branch}"
            )));
        }
        let gray_snap = self.gray_snapshot_of(id, branch, st.gray_seq)?;
        let old = if st.active_version == 0 {
            SnapshotMap::new()
        } else {
            self.snapshot_of(id, branch, st.active_version)?
        };
        let diff = compute_diff(&old, &gray_snap);

        // Q1：新 active 号 = max(active, gray)+1（单调分配器；严格大于客户端见过的任何号）
        let vno = Self::next_monotonic(st.active_version, st.gray_seq);
        let mut record = VersionRecord {
            no: vno,
            structure_version: st.structure_version,
            created_at: now_ms,
            operator: Self::operator_id(operator),
            comment: comment.to_string(),
            rollback_of: None,
            kind: VersionKind::Full,
            snapshot_ref: None,
            diff_ref: None,
            event_ty: Some(EventType::ValuePublish),
            gray: true, // 转正版本：重放时还原 gray 事件标记
        };
        self.write_version_snapshot(id, branch, vno, &old, &gray_snap, &mut record)?;
        // 回收：转正后灰度快照已并入 v/，删除 gray-snap 键
        self.delete_pending(gray_snap_key(id, branch, st.gray_seq).as_bytes())?;
        st.active_version = vno;
        st.gray_seq = 0;
        st.gray_rule = None;
        st.last_request_id = Some(request_id.to_string());
        self.save_pending(&branch_state_key(id, branch), &st)?;

        Ok(vec![PublishEvent {
            project: id.clone(),
            branch: branch.clone(),
            version: vno, // 新稳定版号：灰度客户端据此重拉
            ty: EventType::ValuePublish,
            structure_version: st.structure_version,
            comment: comment.to_string(),
            request_id: request_id.to_string(),
            changes: diff,
            gray: true,
        }])
    }

    /// 灰度下量/回滚：清灰度（gray_seq=0, gray_rule=None），不产生新版本。
    /// 事件 gray=true 携带回落版本号（active_version）：灰度客户端据此重拉稳定版（Q4）。
    fn apply_gray_abort(
        &mut self,
        id: &ProjectId,
        branch: &BranchName,
        comment: &str,
        request_id: &str,
        _now_ms: i64,
        _operator: &str,
    ) -> ApplyOutcome {
        let mut st = self
            .get_branch_state(id, branch)?
            .ok_or_else(|| Error::not_found(format!("branch {branch} of {id}")))?;

        // 幂等（I10）
        if st.last_request_id.as_deref() == Some(request_id) {
            return Ok(vec![]);
        }
        if st.gray_seq == 0 || st.gray_rule.is_none() {
            return Err(Error::validation(format!(
                "no active gray on branch {branch}"
            )));
        }
        // 回收：下量后删除灰度快照
        self.delete_pending(gray_snap_key(id, branch, st.gray_seq).as_bytes())?;
        st.gray_seq = 0;
        st.gray_rule = None;
        st.last_request_id = Some(request_id.to_string());
        self.save_pending(&branch_state_key(id, branch), &st)?;

        Ok(vec![PublishEvent {
            project: id.clone(),
            branch: branch.clone(),
            version: st.active_version, // 回落版本号：客户端重拉目标
            ty: EventType::ValuePublish,
            structure_version: st.structure_version,
            comment: comment.to_string(),
            request_id: request_id.to_string(),
            changes: vec![],
            gray: true,
        }])
    }

    // ---------------- 共享库（R6） ----------------

    fn apply_shared_draft_update(&mut self, item: &SharedItem, _operator: &str) -> ApplyOutcome {
        if item.key.is_empty() || !validator::valid_key_name(&item.key) {
            return Err(Error::validation("shared key 须为 1-128 位 [A-Za-z0-9._-]"));
        }
        // F9（状态机兜底，防绕过 API 层校验）：secret 标志与类型一致性——
        // secret 项只能是 Secret 类型（密文）；Secret 类型必须标记 secret=true。
        if item.secret && item.ty != ValueType::Secret {
            return Err(Error::validation("secret 共享项 type 必须为 secret"));
        }
        if !item.secret && item.ty == ValueType::Secret {
            return Err(Error::validation(
                "type=secret 的共享项必须标记 secret=true",
            ));
        }
        let size = serde_json::to_vec(item)
            .map_err(|e| Error::internal(format!("serialize shared: {e}")))?
            .len();
        if size > MAX_VALUE_BYTES {
            return Err(Error::limit_exceeded("shared item too large"));
        }
        self.save_pending(&shared_draft_key(&item.key), item)?;
        Ok(vec![])
    }

    /// 管理面访问器：共享草稿列表（GET /api/v1/shared-draft）。
    pub fn list_shared_drafts(&self) -> Result<Vec<SharedItem>, Error> {
        let rows = self.get_prefix_merged(K_SHARED_DRAFT.as_bytes())?;
        let mut out = Vec::new();
        for (_, v) in rows {
            if let Ok(item) = serde_json::from_slice::<SharedItem>(&v) {
                out.push(item);
            }
        }
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    /// 管理面访问器：已发布共享项列表（GET /api/v1/shared）。
    pub fn list_shared_published(&self) -> Result<Vec<SharedItem>, Error> {
        let rows = self.get_prefix_merged(K_SHARED.as_bytes())?;
        let mut out = Vec::new();
        for (_, v) in rows {
            if let Ok(item) = serde_json::from_slice::<SharedItem>(&v) {
                out.push(item);
            }
        }
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    pub fn get_shared(&self, key: &str) -> Result<Option<SharedItem>, Error> {
        self.load_merged(&shared_key(key))
    }

    /// shared-edit-ui：共享项**当前生效值**（草稿优先，无草稿回落已发布）——
    /// 供「编辑保留密文」路径按 key 直读既有密文（list_shared_drafts 为全量扫描，不适用）。
    pub fn get_shared_effective(&self, key: &str) -> Result<Option<SharedItem>, Error> {
        if let Some(item) = self.load_merged(&shared_draft_key(key))? {
            return Ok(Some(item));
        }
        self.load_merged(&shared_key(key))
    }

    fn apply_shared_publish(
        &mut self,
        comment: &str,
        request_id: &str,
        now_ms: i64,
        _operator: &str,
        cascade: SharedCascadeMode,
        _policy: PublishPolicy,
    ) -> ApplyOutcome {
        let drafts = self.list_shared_drafts()?;
        if drafts.is_empty() {
            return Err(Error::new(ErrorKind::NoDraft, "no shared draft"));
        }
        let mut events = Vec::new();
        for item in &drafts {
            let prev = self.get_shared(&item.key)?;
            let version = prev.as_ref().map(|p| p.version).unwrap_or(0) + 1;
            let published = SharedItem {
                key: item.key.clone(),
                ty: item.ty,
                secret: item.secret,
                required: item.required,
                value: item.value.clone(),
                version,
                description: item.description.clone(),
            };
            self.save_pending(&shared_key(&item.key), &published)?;
            self.store.delete(shared_draft_key(&item.key).as_bytes())?;

            // 级联（G1/D36）：Auto = 绑定该共享项的 (项目, 分支) 版本推进（原子 D15）；
            // Manual = 只更共享版本，引用分支下次发布时经 materialize_resolved 物化新值。
            if cascade == SharedCascadeMode::Manual {
                continue;
            }
            // 引用选择在分支 shared_bindings：扫描全项目全分支，收集绑定 == 本 key 的 (project, branch, group, item_key)
            for (project, branch, group, key) in self.shared_usage(&item.key)? {
                self.cascade_to_branch(
                    &project,
                    &branch,
                    &group,
                    &key,
                    &item.value,
                    comment,
                    request_id,
                    now_ms,
                    &mut events,
                )?;
            }
        }
        Ok(events)
    }

    /// 级联单个 (项目, 分支, group, key) 的值更新（版本推进 + SharedCascade 事件）。
    #[allow(clippy::too_many_arguments)]
    fn cascade_to_branch(
        &mut self,
        project: &ProjectId,
        branch: &BranchName,
        group: &str,
        key: &str,
        value: &Value,
        comment: &str,
        request_id: &str,
        now_ms: i64,
        events: &mut Vec<PublishEvent>,
    ) -> Result<(), Error> {
        let mut st = self
            .get_branch_state(project, branch)?
            .ok_or_else(|| Error::internal("branch state missing"))?;
        let old = if st.active_version == 0 {
            SnapshotMap::new()
        } else {
            self.snapshot_of(project, branch, st.active_version)?
        };
        let mut new_snap = old.clone();
        new_snap
            .entry(group.to_string())
            .or_default()
            .insert(key.to_string(), value.clone());
        let diff = compute_diff(&old, &new_snap);
        let vno = st.active_version + 1;
        let mut record = VersionRecord {
            no: vno,
            structure_version: st.structure_version,
            created_at: now_ms,
            operator: "shared".into(),
            comment: comment.to_string(),
            rollback_of: None,
            kind: VersionKind::Full,
            snapshot_ref: None,
            diff_ref: None,
            event_ty: Some(EventType::SharedCascade),
            gray: false,
        };
        self.write_version_snapshot(project, branch, vno, &old, &new_snap, &mut record)?;
        st.active_version = vno;
        self.save_pending(&branch_state_key(project, branch), &st)?;
        events.push(PublishEvent {
            project: project.clone(),
            branch: branch.clone(),
            version: vno,
            ty: EventType::SharedCascade,
            structure_version: st.structure_version,
            comment: comment.to_string(),
            request_id: request_id.to_string(),
            changes: diff,
            gray: false,
        });
        Ok(())
    }

    /// 删除共享项（草稿 + 已发布，幂等）：已发布项被任一分支绑定引用 → 拒绝。
    fn apply_shared_delete(&mut self, key: &str, _operator: &str) -> ApplyOutcome {
        if !validator::valid_key_name(key) {
            return Err(Error::validation("shared key 须为 1-128 位 [A-Za-z0-9._-]"));
        }
        if self.get_shared(key)?.is_some() {
            let refs = self.shared_usage(key)?;
            if !refs.is_empty() {
                let detail = refs
                    .iter()
                    .map(|(p, b, g, k)| format!("{}/{}/{}/{}", p.as_str(), b.as_str(), g, k))
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(Error::conflict(format!(
                    "shared item {key} 被 {} 处分支配置引用：{detail}；请先移除引用",
                    refs.len()
                )));
            }
            self.delete_pending(shared_key(key).as_bytes())?;
        }
        // 草稿一并删除（幂等：无草稿也成功）
        self.delete_pending(shared_draft_key(key).as_bytes())?;
        Ok(vec![])
    }

    /// 反向引用：扫描全项目全分支 shared_bindings，收集绑定 == key 的 (project, branch, group, item_key)。
    pub fn shared_usage(
        &self,
        key: &str,
    ) -> Result<Vec<(ProjectId, BranchName, String, String)>, Error> {
        let mut out = Vec::new();
        for p in self.list_projects()? {
            for b in self.list_branches(&p.id)? {
                if let Some(st) = self.get_branch_state(&p.id, &b)? {
                    for (g, m) in &st.shared_bindings {
                        for (k, rk) in m {
                            if rk == key {
                                out.push((p.id.clone(), b.clone(), g.clone(), k.clone()));
                            }
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    // ---------------- 会话（I7 单管理员；状态机内强制） ----------------

    /// 命令 operator 的落库值：空串（旧客户端/全局管理员）→ "admin"。
    fn operator_id(operator: &str) -> String {
        if operator.is_empty() {
            "admin".to_string()
        } else {
            operator.to_string()
        }
    }

    fn apply_session_login(
        &mut self,
        token_hash: &str,
        issued_at: i64,
        expires_at: Option<i64>,
    ) -> ApplyOutcome {
        if self.get_session()?.is_some() {
            return Err(Error::new(ErrorKind::SessionInUse, "已有管理员在线"));
        }
        let session = AdminSession {
            token_hash: token_hash.to_string(),
            issued_at,
            expires_at,
            device_id: "cli".into(),
            principal: Principal::Admin,
        };
        self.save_pending(session_key(), &session)?;
        Ok(vec![])
    }

    fn apply_session_logout(&mut self) -> ApplyOutcome {
        self.delete_pending(session_key().as_bytes())?;
        Ok(vec![])
    }

    fn apply_session_heartbeat(&mut self, expires_at: Option<i64>) -> ApplyOutcome {
        let mut session = self
            .get_session()?
            .ok_or_else(|| Error::new(ErrorKind::SessionExpired, "未登录"))?;
        session.expires_at = expires_at;
        self.save_pending(session_key(), &session)?;
        Ok(vec![])
    }

    // ---------------- 多会话（multisession 改造，纯新增变体） ----------------

    /// 多会话管理员登录：写 sess/admin/{session_id}（多会话并存，不检查已存在、不 409）。
    fn apply_multi_session_login(
        &mut self,
        token_hash: &str,
        issued_at: i64,
        expires_at: Option<i64>,
        session_id: &str,
    ) -> ApplyOutcome {
        let session = AdminSession {
            token_hash: token_hash.to_string(),
            issued_at,
            expires_at,
            device_id: "cli".into(),
            principal: Principal::Admin,
        };
        self.save_pending(&session_key_with(session_id), &session)?;
        Ok(vec![])
    }

    /// 多会话管理员登出：删 sess/admin/{session_id}（幂等）。
    fn apply_multi_session_logout(&mut self, session_id: &str) -> ApplyOutcome {
        self.delete_pending(session_key_with(session_id).as_bytes())?;
        Ok(vec![])
    }

    /// 多会话管理员心跳：续期 sess/admin/{session_id}（无该会话 → ERR_SESSION_EXPIRED）。
    fn apply_multi_session_heartbeat(
        &mut self,
        session_id: &str,
        expires_at: Option<i64>,
    ) -> ApplyOutcome {
        let key = session_key_with(session_id);
        let mut session: AdminSession = self
            .load_merged(&key)?
            .ok_or_else(|| Error::new(ErrorKind::SessionExpired, "会话不存在或已过期"))?;
        session.expires_at = expires_at;
        self.save_pending(&key, &session)?;
        Ok(vec![])
    }

    /// 多会话 PA 登录：写 sess/pa/{username}/{session_id}（多会话并存，不 409）。
    #[allow(clippy::too_many_arguments)]
    fn apply_multi_pa_session_login(
        &mut self,
        username: &str,
        token_hash: &str,
        issued_at: i64,
        expires_at: Option<i64>,
        device_id: &str,
        session_id: &str,
    ) -> ApplyOutcome {
        let session = AdminSession {
            token_hash: token_hash.to_string(),
            issued_at,
            expires_at,
            device_id: device_id.to_string(),
            principal: Principal::ProjectAdmin {
                username: username.to_string(),
                project: self
                    .get_project_admin(username)?
                    .map(|a| a.project)
                    .ok_or_else(|| Error::new(ErrorKind::NotFound, "账号不存在"))?,
            },
        };
        self.save_pending(&pa_session_key_with(username, session_id), &session)?;
        Ok(vec![])
    }

    /// 多会话 PA 登出：删 sess/pa/{username}/{session_id}（幂等）。
    fn apply_multi_pa_session_logout(&mut self, username: &str, session_id: &str) -> ApplyOutcome {
        self.delete_pending(pa_session_key_with(username, session_id).as_bytes())?;
        Ok(vec![])
    }

    /// 多会话 PA 心跳：续期 sess/pa/{username}/{session_id}。
    fn apply_multi_pa_session_heartbeat(
        &mut self,
        username: &str,
        session_id: &str,
        expires_at: Option<i64>,
    ) -> ApplyOutcome {
        let key = pa_session_key_with(username, session_id);
        let mut session: AdminSession = self
            .load_merged(&key)?
            .ok_or_else(|| Error::new(ErrorKind::SessionExpired, "会话不存在或已过期"))?;
        session.expires_at = expires_at;
        self.save_pending(&key, &session)?;
        Ok(vec![])
    }

    /// 踢全部管理员会话（multisession：旧 key + 前缀双删）。
    fn apply_multi_session_logout_all(&mut self) -> ApplyOutcome {
        self.delete_all_admin_sessions()?;
        Ok(vec![])
    }

    /// 踢某 PA 账号全部会话（multisession：旧 key + 前缀双删）。
    fn apply_multi_pa_session_logout_all(&mut self, username: &str) -> ApplyOutcome {
        self.delete_all_pa_sessions(username)?;
        Ok(vec![])
    }

    // ---------------- 项目管理员（Project Admin）----------------
    // 设计文档 dev_docs/design/project-admin.md §3.1/§6。
    // 会话判定只看 is_some()，不读墙钟（D16 确定性）。

    fn valid_pa_username(name: &str) -> bool {
        !name.is_empty()
            && name != "admin"
            && name.len() <= 64
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            && name.len() >= 2
    }

    fn apply_project_admin_create(
        &mut self,
        project: &ProjectId,
        username: &str,
        salt: &str,
        password_hash: &str,
        now_ms: i64,
    ) -> ApplyOutcome {
        if !Self::valid_pa_username(username) {
            return Err(Error::new(
                ErrorKind::Validation,
                "用户名须为 2-64 位 [A-Za-z0-9_-] 且不可为 admin",
            ));
        }
        if load::<Project>(&*self.store, &project_key(project))?.is_none() {
            return Err(Error::new(
                ErrorKind::NotFound,
                format!("项目 {project} 不存在"),
            ));
        }
        let key = project_admin_key(username);
        if load::<ProjectAdminAccount>(&*self.store, &key)?.is_some() {
            return Err(Error::new(ErrorKind::Conflict, "账号已存在"));
        }
        let acct = ProjectAdminAccount {
            username: username.to_string(),
            project: project.clone(),
            salt: salt.to_string(),
            password_hash: password_hash.to_string(),
            created_at: now_ms,
        };
        self.save_pending(&key, &acct)?;
        Ok(vec![])
    }

    /// 删除某 PA 账号的全部会话（multisession 改造）：旧格式单 key + 多会话前缀双删。
    fn delete_all_pa_sessions(&mut self, username: &str) -> Result<(), Error> {
        // 旧格式单 key（sess/pa/{username}，旧客户端/旧日志产生）——显式删
        self.delete_pending(pa_session_key(username).as_bytes())?;
        // 多会话前缀（sess/pa/{username}/...）——前缀扫全部删
        let prefix = pa_session_prefix(username);
        let rows = self.get_prefix_merged(prefix.as_bytes())?;
        for (k, _) in rows {
            self.delete_pending(&k)?;
        }
        Ok(())
    }

    /// 删除全部管理员会话（multisession 改造）：旧 key + 前缀双删。
    fn delete_all_admin_sessions(&mut self) -> Result<(), Error> {
        self.delete_pending(session_key().as_bytes())?;
        let rows = self.get_prefix_merged(K_SESSION_PREFIX.as_bytes())?;
        for (k, _) in rows {
            self.delete_pending(&k)?;
        }
        Ok(())
    }

    fn apply_project_admin_delete(&mut self, username: &str) -> ApplyOutcome {
        let key = project_admin_key(username);
        if load::<ProjectAdminAccount>(&*self.store, &key)?.is_none() {
            return Err(Error::new(ErrorKind::NotFound, "账号不存在"));
        }
        self.delete_all_pa_sessions(username)?;
        self.delete_pending(key.as_bytes())?;
        Ok(vec![])
    }

    fn apply_project_admin_set_password(
        &mut self,
        username: &str,
        salt: &str,
        password_hash: &str,
    ) -> ApplyOutcome {
        let key = project_admin_key(username);
        let Some(mut acct) = load::<ProjectAdminAccount>(&*self.store, &key)? else {
            return Err(Error::new(ErrorKind::NotFound, "账号不存在"));
        };
        acct.salt = salt.to_string();
        acct.password_hash = password_hash.to_string();
        self.save_pending(&key, &acct)?;
        // 改密即时收回全部会话（权限立即生效；旧+新格式双删）
        self.delete_all_pa_sessions(username)?;
        Ok(vec![])
    }

    /// token 名称字符集：[A-Za-z0-9._-]{1,64}。
    fn valid_token_name(name: &str) -> bool {
        !name.is_empty()
            && name.len() <= 64
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
    }

    fn apply_project_token_create(
        &mut self,
        project: &ProjectId,
        name: &str,
        token_hash: &str,
        operator: &str,
        now_ms: i64,
    ) -> ApplyOutcome {
        if !Self::valid_token_name(name) {
            return Err(Error::new(
                ErrorKind::Validation,
                "token 名称须为 1-64 位 [A-Za-z0-9._-]",
            ));
        }
        if load::<Project>(&*self.store, &project_key(project))?.is_none() {
            return Err(Error::new(
                ErrorKind::NotFound,
                format!("项目 {project} 不存在"),
            ));
        }
        // 幂等：同 hash（同一明文 token）已存在 → no-op（重试/重放安全）
        if load::<ProjectTokenRecord>(&*self.store, &data_token_key(token_hash))?.is_some() {
            return Ok(vec![]);
        }
        // name 项目内唯一（扫 tok/ 前缀过滤项目，O(全部 token 数)，创建低频可接受）
        for (_, raw) in self.get_prefix_merged(K_DATA_TOKEN.as_bytes())? {
            if let Ok(rec) = serde_json::from_slice::<ProjectTokenRecord>(&raw) {
                if rec.project == *project && rec.name == name {
                    return Err(Error::new(ErrorKind::Conflict, "该项目下 token 名称已存在"));
                }
            }
        }
        // operator 空串（旧日志）按 command.rs 约定归一为 "admin"
        let created_by = if operator.is_empty() {
            "admin"
        } else {
            operator
        };
        let id: String = token_hash.chars().take(16).collect();
        let rec = ProjectTokenRecord {
            id,
            name: name.to_string(),
            project: project.clone(),
            hash: token_hash.to_string(),
            created_at: now_ms.max(0) as u64,
            created_by: created_by.to_string(),
            revoked: false,
        };
        self.save_pending(&data_token_key(token_hash), &rec)?;
        Ok(vec![])
    }

    fn apply_project_token_revoke(&mut self, project: &ProjectId, token_id: &str) -> ApplyOutcome {
        // 按项目 + id 定位（扫 tok/ 前缀；吊销低频可接受）
        let mut target: Option<Vec<u8>> = None;
        for (k, raw) in self.get_prefix_merged(K_DATA_TOKEN.as_bytes())? {
            if let Ok(rec) = serde_json::from_slice::<ProjectTokenRecord>(&raw) {
                if rec.project == *project && rec.id == token_id {
                    target = Some(k);
                    break;
                }
            }
        }
        let Some(key) = target else {
            return Err(Error::new(ErrorKind::NotFound, "token 不存在"));
        };
        let key_str = String::from_utf8_lossy(&key).to_string();
        let Some(mut rec) = load::<ProjectTokenRecord>(&*self.store, &key_str)? else {
            return Err(Error::new(ErrorKind::NotFound, "token 不存在"));
        };
        if rec.revoked {
            return Ok(vec![]); // 幂等
        }
        rec.revoked = true;
        self.save_pending(&key_str, &rec)?;
        Ok(vec![])
    }

    /// 数据面鉴权：按 hash 读 token 记录（O(1) 单次 KV 读）。
    pub fn get_data_token(&self, hash: &str) -> Result<Option<ProjectTokenRecord>, Error> {
        self.load_merged(&data_token_key(hash))
    }

    /// 管理面列表：某项目全部 token（含已吊销；按创建时间升序）。
    pub fn list_project_tokens(
        &self,
        project: &ProjectId,
    ) -> Result<Vec<ProjectTokenRecord>, Error> {
        let mut out = vec![];
        for (_, raw) in self.get_prefix_merged(K_DATA_TOKEN.as_bytes())? {
            if let Ok(rec) = serde_json::from_slice::<ProjectTokenRecord>(&raw) {
                if rec.project == *project {
                    out.push(rec);
                }
            }
        }
        out.sort_by_key(|a| a.created_at);
        Ok(out)
    }

    fn apply_pa_session_login(
        &mut self,
        username: &str,
        token_hash: &str,
        issued_at: i64,
        expires_at: Option<i64>,
        device_id: &str,
    ) -> ApplyOutcome {
        let key = pa_session_key(username);
        if load::<AdminSession>(&*self.store, &key)?.is_some() {
            return Err(Error::new(ErrorKind::SessionInUse, "该账号已有会话在线"));
        }
        let Some(acct) = load::<ProjectAdminAccount>(&*self.store, &project_admin_key(username))?
        else {
            return Err(Error::new(ErrorKind::NotFound, "账号不存在"));
        };
        let session = AdminSession {
            token_hash: token_hash.to_string(),
            issued_at,
            expires_at,
            device_id: device_id.to_string(),
            principal: Principal::ProjectAdmin {
                username: username.to_string(),
                project: acct.project.clone(),
            },
        };
        self.save_pending(&key, &session)?;
        Ok(vec![])
    }

    fn apply_pa_session_logout(&mut self, username: &str) -> ApplyOutcome {
        self.delete_pending(pa_session_key(username).as_bytes())?;
        Ok(vec![])
    }

    fn apply_pa_session_heartbeat(
        &mut self,
        username: &str,
        expires_at: Option<i64>,
    ) -> ApplyOutcome {
        let key = pa_session_key(username);
        let Some(mut session) = load::<AdminSession>(&*self.store, &key)? else {
            return Err(Error::new(ErrorKind::SessionExpired, "会话不存在"));
        };
        session.expires_at = expires_at;
        self.save_pending(&key, &session)?;
        Ok(vec![])
    }

    /// 读取项目管理员账号。
    pub fn get_project_admin(&self, username: &str) -> Result<Option<ProjectAdminAccount>, Error> {
        self.load_merged(&project_admin_key(username))
    }

    /// 列出项目全部项目管理员账号（扫 adm/pa/ 前缀过滤，O(账号数)）。
    pub fn list_project_admins(&self, project: &str) -> Result<Vec<ProjectAdminAccount>, Error> {
        let mut out = vec![];
        for (_, raw) in self.get_prefix_merged(K_PA_ACCOUNT.as_bytes())? {
            if let Ok(acct) = serde_json::from_slice::<ProjectAdminAccount>(&raw) {
                if acct.project.0 == project {
                    out.push(acct);
                }
            }
        }
        out.sort_by(|a, b| a.username.cmp(&b.username));
        Ok(out)
    }

    pub fn get_pa_session(&self, username: &str) -> Result<Option<AdminSession>, Error> {
        self.load_merged(&pa_session_key(username))
    }

    /// 多会话 PA 会话：读 sess/pa/{username}/{session_id}（multisession 改造）。
    pub fn get_pa_session_with(
        &self,
        username: &str,
        session_id: &str,
    ) -> Result<Option<AdminSession>, Error> {
        self.load_merged(&pa_session_key_with(username, session_id))
    }

    /// 列出全部管理员会话（multisession：前缀扫 sess/admin/；force-logout 批量/审计用）。
    pub fn list_admin_sessions(&self) -> Result<Vec<AdminSession>, Error> {
        let rows = self.get_prefix_merged(K_SESSION_PREFIX.as_bytes())?;
        let mut out = Vec::new();
        for (_, v) in rows {
            if let Ok(s) = serde_json::from_slice::<AdminSession>(&v) {
                out.push(s);
            }
        }
        Ok(out)
    }

    fn apply_admin_set_password(&mut self, password_hash: &str) -> ApplyOutcome {
        self.save_pending(K_ADMIN_PW, &password_hash.to_string())?;
        Ok(vec![])
    }

    /// 状态机内管理员密码哈希（set-password 后登录用它校验；未设置时回退节点配置）。
    pub fn get_admin_password_hash(&self) -> Result<Option<String>, Error> {
        self.load_merged(K_ADMIN_PW)
    }

    /// 审计追加：seq 单调分配（audit/seq 计数），条目落 audit/{seq:020}。
    /// 入参 entry.seq 忽略（由状态机分配）。
    fn apply_audit_append(&mut self, entry: &AuditEntry) -> ApplyOutcome {
        let prev: Option<u64> = self.load_merged(K_AUDIT_SEQ)?;
        let seq = prev.unwrap_or(0) + 1;
        let entry = AuditEntry {
            seq,
            ..entry.clone()
        };
        self.save_pending(&audit_key(seq), &entry)?;
        self.save_pending(K_AUDIT_SEQ, &seq)?;
        Ok(vec![])
    }

    /// 密钥轮换：副作用（更新 Cipher/写 ring 文件）由 dsh-raft 的 apply 钩子执行，
    /// 状态机本身不落任何数据（保证确定性，跨节点重放结果一致）。
    fn apply_rotate_master_key(&mut self) -> ApplyOutcome {
        Ok(vec![])
    }
}

/// 会话令牌哈希（SHA-256 hex；明文 token 不落库/不落日志，I7）。
pub fn token_hash(token: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(token.as_bytes());
    h.finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::InMemoryStore;

    fn sm() -> StateMachine {
        StateMachine::new(Box::new(InMemoryStore::new()))
    }

    fn shared_item(key: &str) -> SharedItem {
        SharedItem {
            key: key.into(),
            ty: ValueType::String,
            secret: false,
            required: false,
            value: Value::String("v".into()),
            version: 0,
            description: None,
        }
    }

    #[test]
    fn shared_draft_rejects_dangerous_names() {
        let mut s = sm();
        // `/` 会破坏 sh/{key} 键分隔
        assert!(s
            .apply_shared_draft_update(&shared_item("k/x"), "")
            .is_err());
        // HTML/XSS 载荷（S1）、非 ASCII、空白、引号
        for k in ["<img>", "配置", "a b", "a'b", "a&b"] {
            assert!(
                s.apply_shared_draft_update(&shared_item(k), "").is_err(),
                "{k:?} must be rejected"
            );
        }
    }

    #[test]
    fn shared_draft_accepts_safe_names() {
        let mut s = sm();
        for k in ["host_name-1", "max_conns", "k", "db.host"] {
            assert!(
                s.apply_shared_draft_update(&shared_item(k), "").is_ok(),
                "{k:?} must be accepted"
            );
        }
    }

    #[test]
    fn shared_delete_rejects_dangerous_key() {
        let mut s = sm();
        for k in ["a/b", "<img>", "配置", "a b"] {
            let e = s.apply_shared_delete(k, "").expect_err("must reject");
            assert_eq!(e.kind, ErrorKind::Validation, "{k:?}: {e:?}");
        }
    }

    /// N1 回归（引用内嵌结构后）：删除项目即删除其结构中的 shared_ref，
    /// shared_usage 反向扫描不再命中已删项目（替代旧孤儿引用索引清理）。
    #[test]
    fn project_delete_removes_structure_shared_refs() {
        let mut s = sm();
        let proj = "order-service";
        s.apply(
            &Command::ProjectCreate {
                name: proj.into(),
                operator: String::new(),
                ts: 0,
            },
            1,
        )
        .unwrap();
        // 发布共享项，结构标记引用共享 + 分支绑定它（经 Command 提交，保证 pending 落库）
        s.apply(
            &Command::SharedDraftUpdate {
                item: shared_item("db_host"),
                operator: String::new(),
            },
            2,
        )
        .unwrap();
        s.apply(
            &Command::SharedPublish {
                comment: "s".into(),
                request_id: "sp1".into(),
                operator: String::new(),
                ts: 0,
                cascade: SharedCascadeMode::Auto,
                policy: PublishPolicy::Block,
            },
            2,
        )
        .unwrap();
        s.apply(
            &Command::StructureDraftSet {
                project: proj.into(),
                base_version: 1,
                groups: vec![GroupDef {
                    name: "redis".into(),
                    items: vec![ItemDef {
                        key: "host".into(),
                        ty: ValueType::String,
                        required: false,
                        secret: false,
                        validate: None,
                        description: None,
                        shared: true,
                    }],
                }],
                operator: String::new(),
            },
            3,
        )
        .unwrap();
        s.apply(
            &Command::PublishStructure {
                project: proj.into(),
                comment: "s".into(),
                request_id: "s1".into(),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            4,
        )
        .unwrap();
        // 项目自动创建 dev/test/prod；用自定义分支绑定共享项
        s.apply(
            &Command::BranchCreate {
                project: ProjectId(proj.into()),
                name: BranchName("staging".into()),
                source: None,
                operator: String::new(),
                ts: 0,
            },
            5,
        )
        .unwrap();
        s.apply(
            &Command::DraftUpdate {
                project: ProjectId(proj.into()),
                branch: BranchName("staging".into()),
                updates: vec![],
                deletes: vec![],
                shared_bindings: vec![crate::command::SharedBinding {
                    group: "redis".into(),
                    key: "host".into(),
                    shared_key: "db_host".into(),
                }],
                operator: String::new(),
                ts: 0,
                expected_draft_rev: None,
            },
            6,
        )
        .unwrap();
        assert_eq!(s.shared_usage("db_host").unwrap().len(), 1);

        s.apply(
            &Command::ProjectDelete {
                id: ProjectId(proj.into()),
                operator: String::new(),
            },
            7,
        )
        .unwrap();
        assert!(s.shared_usage("db_host").unwrap().is_empty());
    }

    /// perf 方案① T5：命令内读合并——pending 覆盖/删除对 load/get_prefix 可见（写后读）。
    #[test]
    fn pending_read_merge_visibility() {
        let mut s = sm();
        // put → merged get 命中 pending（未提交即可见）
        s.put_pending(b"p/x", b"v1").unwrap();
        assert_eq!(s.get_merged(b"p/x").unwrap().unwrap(), b"v1");
        // 覆盖：后写优先（逆序）
        s.put_pending(b"p/x", b"v2").unwrap();
        assert_eq!(s.get_merged(b"p/x").unwrap().unwrap(), b"v2");
        // 删除优先于插入（同 key 先插后删 → None）
        s.delete_pending(b"p/x").unwrap();
        assert_eq!(s.get_merged(b"p/x").unwrap(), None);
        // 先删后插 → 插生效
        s.put_pending(b"p/x", b"v3").unwrap();
        assert_eq!(s.get_merged(b"p/x").unwrap().unwrap(), b"v3");
        // get_prefix 合并：store 基 + pending 插 + pending 删
        s.store.put(b"p/a", b"sa").unwrap();
        s.store.put(b"p/z", b"sz").unwrap();
        s.put_pending(b"p/m", b"pm").unwrap();
        s.delete_pending(b"p/a").unwrap();
        let rows = s.get_prefix_merged(b"p/").unwrap();
        let map: std::collections::BTreeMap<_, _> = rows.into_iter().collect();
        assert_eq!(map.get(b"p/a".as_slice()), None, "pending 删除遮蔽 store");
        assert_eq!(
            map.get(b"p/m".as_slice()).unwrap(),
            b"pm",
            "pending 插入合并"
        );
        assert_eq!(map.get(b"p/z".as_slice()).unwrap(), b"sz", "store 基保留");
        assert_eq!(map.get(b"p/x".as_slice()).unwrap(), b"v3");
        // 前缀边界：prefix "p/m" 只命中自己
        let rows2 = s.get_prefix_merged(b"p/m").unwrap();
        assert_eq!(rows2.len(), 1);
    }

    /// perf 方案① T4：命令失败 → pending abort，store 无部分写。
    #[test]
    fn apply_failure_aborts_pending() {
        let mut s = sm();
        // 建项目 + 结构（正常路径）
        s.apply(
            &Command::ProjectCreate {
                name: "p".into(),
                operator: String::new(),
                ts: 0,
            },
            1,
        )
        .unwrap();
        s.apply(
            &Command::StructureDraftSet {
                project: "p".into(),
                base_version: 1,
                groups: vec![GroupDef {
                    name: "redis".into(),
                    items: vec![ItemDef {
                        key: "host".into(),
                        ty: ValueType::String,
                        required: true,
                        secret: false,
                        validate: None,
                        description: None,
                        shared: false,
                    }],
                }],
                operator: String::new(),
            },
            2,
        )
        .unwrap();
        s.apply(
            &Command::PublishStructure {
                project: "p".into(),
                comment: "s".into(),
                request_id: "s1".into(),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            3,
        )
        .unwrap();
        // 无草稿直接发布 → 失败（NoDraft）；不产生版本/快照
        let e = s
            .apply(
                &Command::Publish {
                    project: "p".into(),
                    branch: BranchName("dev".into()),
                    comment: "x".into(),
                    request_id: "r1".into(),
                    operator: String::new(),
                    ts: 0,
                    policy: PublishPolicy::Block,
                },
                4,
            )
            .unwrap_err();
        assert_eq!(e.kind, ErrorKind::NoDraft);
        assert!(s.pending_ops.is_empty(), "失败后 pending 必须清空");
        // store 无版本记录/快照（无部分写）
        let pid: ProjectId = "p".into();
        let dev = BranchName("dev".into());
        assert!(s
            .store
            .get(version_key(&pid, &dev, 4).as_bytes())
            .unwrap()
            .is_none());
        assert!(s
            .store
            .get(snapshot_key(&pid, &dev, 4).as_bytes())
            .unwrap()
            .is_none());
        // 分支仍可正常发布（后续成功路径不受污染）
        s.apply(
            &Command::DraftUpdate {
                project: "p".into(),
                branch: BranchName("dev".into()),
                updates: vec![crate::command::DraftUpdateItem {
                    group: "redis".into(),
                    key: "host".into(),
                    value: Value::String("h".into()),
                }],
                deletes: vec![],
                shared_bindings: vec![],
                operator: String::new(),
                ts: 0,
                expected_draft_rev: None,
            },
            5,
        )
        .unwrap();
        assert!(s
            .apply(
                &Command::Publish {
                    project: "p".into(),
                    branch: BranchName("dev".into()),
                    comment: "v1".into(),
                    request_id: "r2".into(),
                    operator: String::new(),
                    ts: 0,
                    policy: PublishPolicy::Block,
                },
                6,
            )
            .is_ok());
    }
}
