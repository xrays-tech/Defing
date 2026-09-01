//! 后台任务（模块 11）：版本裁剪等。任务仅在 leader 节点执行。

use std::sync::{Arc, RwLock};

use dsh_core::StateMachine;
use dsh_crypto::Cipher;
use tokio::sync::watch;

/// 任务执行上下文。
pub struct JobCtx {
    pub is_leader: Arc<watch::Receiver<bool>>,
}

pub trait Job: Send + Sync {
    fn name(&self) -> &'static str;
    fn interval(&self) -> std::time::Duration;
    fn run(&self, sm: &RwLock<StateMachine>) -> Result<(), String>;
}

/// 版本裁剪：每分支保留最近 keep 个版本 + 活动版本。
pub struct VersionRetention {
    pub keep: usize,
}

impl Job for VersionRetention {
    fn name(&self) -> &'static str {
        "version-retention"
    }

    fn interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(60)
    }

    fn run(&self, sm: &RwLock<StateMachine>) -> Result<(), String> {
        let guard = sm.write().map_err(|e| e.to_string())?;
        let projects = guard.list_projects().map_err(|e| e.to_string())?;
        for p in projects {
            for b in guard.list_branches(&p.id).map_err(|e| e.to_string())? {
                let removed = guard
                    .prune_versions(&p.id, &b, self.keep)
                    .map_err(|e| e.to_string())?;
                if removed > 0 {
                    tracing::info!("pruned {removed} versions of {}/{}", p.id, b);
                }
            }
        }
        Ok(())
    }
}

/// 审计保留：仅保留最近 keep 条（对齐 design-v2：审计保留 100k 条或 30 天）。
pub struct AuditRetention {
    pub keep: usize,
}

impl Job for AuditRetention {
    fn name(&self) -> &'static str {
        "audit-retention"
    }

    fn interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(3600)
    }

    fn run(&self, sm: &RwLock<StateMachine>) -> Result<(), String> {
        let guard = sm.write().map_err(|e| e.to_string())?;
        let removed = guard.prune_audit(self.keep).map_err(|e| e.to_string())?;
        if removed > 0 {
            tracing::info!("pruned {removed} audit entries (keep {})", self.keep);
        }
        Ok(())
    }
}

/// DEK 重包（B6）：轮换主密钥后把全部 secret 密文的 edek 重包到当前 KEK（数据不重加密）。
/// 仅重包 `dek_v < 当前代际` 的密文（已最新则跳过，幂等）。
pub struct RewrapDeks {
    pub cipher: Arc<Cipher>,
}

impl Job for RewrapDeks {
    fn name(&self) -> &'static str {
        "rewrap-deks"
    }

    fn interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(300)
    }

    fn run(&self, sm: &RwLock<StateMachine>) -> Result<(), String> {
        let cipher = self.cipher.clone();
        let gen = cipher.keyring().generation();
        let guard = sm.write().map_err(|e| e.to_string())?;
        let count = guard
            .rewrap_deks(&|ct| {
                if ct.dek_v >= gen {
                    None
                } else {
                    Some(
                        cipher
                            .rewrap_dek(ct)
                            .map_err(|e| dsh_core::Error::internal(format!("rewrap: {e}"))),
                    )
                }
            })
            .map_err(|e| e.to_string())?;
        if count > 0 {
            tracing::info!("rewrapped {count} secret DEKs to KEK generation {gen}");
        }
        Ok(())
    }
}

/// 调度器：按间隔运行任务（仅 leader）。
#[derive(Default)]
pub struct JobScheduler {
    jobs: Vec<Box<dyn Job>>,
}
impl JobScheduler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, job: impl Job + 'static) {
        self.jobs.push(Box::new(job));
    }

    pub fn spawn(self, sm: Arc<RwLock<StateMachine>>, is_leader: watch::Receiver<bool>) {
        for job in self.jobs {
            let sm = sm.clone();
            let is_leader = is_leader.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(job.interval());
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    ticker.tick().await;
                    if !*is_leader.borrow() {
                        continue;
                    }
                    if let Err(e) = job.run(&sm) {
                        tracing::warn!("job {} failed: {e}", job.name());
                    }
                }
            });
        }
    }
}

// ---------------- G5/D33：灰度自动回滚钩子（可选，默认禁用） ----------------

/// 灰度健康探针：返回当前错误率（0.0-1.0）；None = 本轮无法获取（跳过）。
/// 业务错误率由外部系统决定，Defing 只提供框架 + 本地探针（对接 /metrics 计数）。
pub trait GrayHealthProbe: Send + Sync {
    fn error_rate(&self) -> Option<f64>;
}

/// 内置探针：节点本地 /metrics 的 HTTP 5xx 比例（dsh_http_5xx_total / dsh_http_requests_total）。
pub struct LocalHttp5xxProbe;

impl GrayHealthProbe for LocalHttp5xxProbe {
    fn error_rate(&self) -> Option<f64> {
        let (reqs, errs) = dsh_observability::http_counters();
        if reqs == 0 {
            return None;
        }
        Some(errs as f64 / reqs as f64)
    }
}

/// 自动回滚判定（纯函数，可测）：错误率可得且 > 阈值 → 回滚。
fn should_rollback(rate: Option<f64>, threshold: f64) -> bool {
    match rate {
        Some(r) => r > threshold,
        None => false,
    }
}

/// 灰度自动回滚任务（仅 leader）：错误率 > 阈值 → 对全部活跃灰度分支执行 GrayAbort。
/// - 走 dsh_raft::write_command 统一写路径（dev-single 直 apply+broadcast；集群 client_write 复制）；
/// - 审计 action="gray_auto_abort"（与手动 gray_abort 区分，可追溯）；
/// - 防抖：abort 后分支 gray_seq=0，不再触发；后台任务读墙钟/网络不违反 D16（仅约束 apply）。
#[allow(clippy::too_many_arguments)]
pub fn spawn_gray_auto_rollback(
    sm: Arc<RwLock<StateMachine>>,
    raft: Option<dsh_raft::RaftHandle>,
    events_tx: Option<tokio::sync::broadcast::Sender<dsh_core::model::PublishEvent>>,
    audit: dsh_observability::AuditLog,
    probe: Box<dyn GrayHealthProbe>,
    threshold: f64,
    interval: std::time::Duration,
    is_leader: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if !*is_leader.borrow() {
                continue;
            }
            let Some(rate) = probe.error_rate() else {
                continue;
            };
            if !should_rollback(Some(rate), threshold) {
                continue;
            }
            // 扫描活跃灰度分支
            let targets: Vec<(dsh_core::model::ProjectId, dsh_core::model::BranchName)> = {
                let Ok(guard) = sm.read() else {
                    tracing::warn!("gray auto-rollback: sm lock poisoned");
                    continue;
                };
                let Ok(projects) = guard.list_projects() else {
                    continue;
                };
                let mut out = Vec::new();
                for p in projects {
                    if let Ok(bs) = guard.list_branches(&p.id) {
                        for b in bs {
                            if let Ok(Some(st)) = guard.get_branch_state(&p.id, &b) {
                                if st.gray_seq > 0 {
                                    out.push((p.id.clone(), b));
                                }
                            }
                        }
                    }
                }
                out
            };
            if targets.is_empty() {
                continue;
            }
            for (pid, bname) in targets {
                let ts = now_ms();
                let cmd = dsh_core::command::Command::GrayAbort {
                    project: pid.clone(),
                    branch: bname.clone(),
                    comment: format!(
                        "auto-rollback: error rate {rate:.2} > threshold {threshold:.2}"
                    ),
                    request_id: format!("auto-{ts}"),
                    operator: "auto-rollback".into(),
                    ts: 0,
                };
                match dsh_raft::write_command(&sm, raft.as_ref(), &cmd, ts, events_tx.as_ref())
                    .await
                {
                    Ok(_) => {
                        tracing::warn!(
                            "gray auto-rollback: aborted {}/{} (error rate {rate:.2} > {threshold:.2})",
                            pid,
                            bname
                        );
                        audit
                            .append(
                                "gray_auto_abort",
                                Some(pid.to_string()),
                                Some(bname.to_string()),
                                None,
                                None,
                                serde_json::json!({ "error_rate": rate, "threshold": threshold }),
                                "auto-rollback",
                            )
                            .await;
                    }
                    Err(e) => {
                        tracing::warn!("gray auto-rollback: abort {}/{} failed: {e}", pid, bname)
                    }
                }
            }
        }
    });
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsh_core::command::{Command, DraftUpdateItem};
    use dsh_core::model::{BranchName, PublishPolicy};
    use dsh_core::InMemoryStore;

    #[test]
    fn retention_keeps_active_and_recent() {
        let mut sm = StateMachine::new(Box::new(InMemoryStore::new()));
        sm.apply(
            &Command::ProjectCreate {
                name: "p".into(),
                operator: String::new(),
                ts: 0,
                clone_from: None,
            },
            1,
        )
        .unwrap();
        sm.apply(
            &Command::StructureDraftSet {
                project: "p".into(),
                base_version: 1,
                groups: vec![dsh_core::model::GroupDef {
                    name: "g".into(),
                    items: vec![dsh_core::model::ItemDef {
                        key: "k".into(),
                        ty: dsh_core::model::ValueType::String,
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
        sm.apply(
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
        // 发布 5 个版本（v2..v6）
        for i in 0..5 {
            sm.apply(
                &Command::DraftUpdate {
                    project: "p".into(),
                    branch: BranchName("dev".into()),
                    updates: vec![DraftUpdateItem {
                        group: "g".into(),
                        key: "k".into(),
                        value: dsh_core::model::Value::String(format!("v{i}")),
                    }],
                    deletes: vec![],
                    shared_bindings: vec![],

                    operator: String::new(),
                    ts: 0,
                    expected_draft_rev: None,
                },
                10 + i,
            )
            .unwrap();
            sm.apply(
                &Command::Publish {
                    project: "p".into(),
                    branch: BranchName("dev".into()),
                    comment: "c".into(),
                    request_id: format!("r{i}"),

                    operator: String::new(),
                    ts: 0,
                    policy: PublishPolicy::Block,
                },
                20 + i,
            )
            .unwrap();
        }
        let total = sm
            .version_history(&"p".into(), &BranchName("dev".into()))
            .unwrap()
            .len();
        assert!(total >= 6); // 结构 v1 + 5 次发布

        // perf 方案② D3：diff 链完整性优先——版本数小于 checkpoint 间隔时基座为 v1，
        // 裁剪不删任何版本（removed=0 是保守正确行为）；活动版本始终可读。
        let removed = sm
            .prune_versions(&"p".into(), &BranchName("dev".into()), 2)
            .unwrap();
        // 6 个版本全在 v1 基座 + diff 链内 → 不裁剪
        assert_eq!(removed, 0, "小版本数 diff 链必须完整保留");
        // 活动版本仍可读
        let cfg = sm
            .get_config(&"p".into(), &BranchName("dev".into()), 0)
            .unwrap();
        assert_eq!(cfg.version, 6);
        // 跨 checkpoint 裁剪：发布到 v250 后 keep=10 → 删除 v<200 且保留 v200 基座
        for i in 0..244 {
            sm.apply(
                &Command::DraftUpdate {
                    project: "p".into(),
                    branch: BranchName("dev".into()),
                    updates: vec![DraftUpdateItem {
                        group: "g".into(),
                        key: "k".into(),
                        value: dsh_core::model::Value::String(format!("x{i}")),
                    }],
                    deletes: vec![],
                    shared_bindings: vec![],
                    operator: String::new(),
                    ts: 0,
                    expected_draft_rev: None,
                },
                100 + i,
            )
            .unwrap();
            sm.apply(
                &Command::Publish {
                    project: "p".into(),
                    branch: BranchName("dev".into()),
                    comment: "c".into(),
                    request_id: format!("rx{i}"),
                    operator: String::new(),
                    ts: 0,
                    policy: PublishPolicy::Block,
                },
                200 + i,
            )
            .unwrap();
        }
        let removed2 = sm
            .prune_versions(&"p".into(), &BranchName("dev".into()), 10)
            .unwrap();
        assert!(removed2 > 0, "跨 checkpoint 裁剪应删除旧版本");
        let hist2 = sm
            .version_history(&"p".into(), &BranchName("dev".into()))
            .unwrap();
        // 保留 v200 基座 + v241..v250 + 活动版本，总 <= 61
        assert!(hist2.len() <= 61, "裁剪后历史应受限: {}", hist2.len());
        // 活动版本（v250）仍可读且内容正确
        let cfg2 = sm
            .get_config(&"p".into(), &BranchName("dev".into()), 0)
            .unwrap();
        assert_eq!(cfg2.version, 250);
        assert_eq!(
            cfg2.groups["g"]["k"],
            dsh_core::model::Value::String("x243".into())
        );
    }
}

#[cfg(test)]
mod gray_rollback_tests {
    use super::*;
    use dsh_observability::{reset_http_counters, HTTP_5XX, HTTP_REQUESTS};
    use std::sync::atomic::Ordering;

    #[test]
    fn local_probe_reads_http_counters() {
        reset_http_counters();
        let probe = LocalHttp5xxProbe;
        assert_eq!(probe.error_rate(), None, "无请求 → None（跳过本轮）");
        HTTP_REQUESTS.store(100, Ordering::Relaxed);
        HTTP_5XX.store(10, Ordering::Relaxed);
        assert_eq!(probe.error_rate(), Some(0.1));
        reset_http_counters();
    }

    #[test]
    fn rollback_decision() {
        assert!(!should_rollback(None, 0.05), "无数据不触发");
        assert!(!should_rollback(Some(0.03), 0.05), "低于阈值不触发");
        assert!(should_rollback(Some(0.08), 0.05), "超过阈值触发");
        assert!(
            !should_rollback(Some(0.05), 0.05),
            "等于阈值不触发（严格大于）"
        );
        assert!(
            should_rollback(Some(0.01), 0.0),
            "阈值 0 = 有任何错误即触发"
        );
    }
}

#[cfg(test)]
mod rewrap_tests {
    use super::*;
    use dsh_core::command::{Command, DraftUpdateItem};
    use dsh_core::model::{
        BranchName, GroupDef, ItemDef, PublishPolicy, SharedCascadeMode, SharedItem, Value,
        ValueType,
    };
    use dsh_core::InMemoryStore;

    #[test]
    fn rewrap_job_bumps_generation_and_keeps_data() {
        let cipher = Arc::new(Cipher::new([1u8; 32]));
        let mut sm = StateMachine::new(Box::new(InMemoryStore::new()));
        sm.apply(
            &Command::ProjectCreate {
                name: "p".into(),
                operator: String::new(),
                ts: 0,
                clone_from: None,
            },
            1,
        )
        .unwrap();
        sm.apply(
            &Command::StructureDraftSet {
                project: "p".into(),
                base_version: 1,
                groups: vec![GroupDef {
                    name: "g".into(),
                    items: vec![
                        ItemDef {
                            key: "host".into(),
                            ty: ValueType::String,
                            required: true,
                            secret: false,
                            validate: None,
                            description: None,
                            shared: false,
                        },
                        ItemDef {
                            key: "pass".into(),
                            ty: ValueType::Secret,
                            required: false,
                            secret: true,
                            validate: None,
                            description: None,
                            shared: false,
                        },
                    ],
                }],

                operator: String::new(),
            },
            2,
        )
        .unwrap();
        sm.apply(
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
        let ct = cipher.encrypt_secret(b"job-secret").unwrap();
        sm.apply(
            &Command::DraftUpdate {
                project: "p".into(),
                branch: BranchName("dev".into()),
                updates: vec![
                    DraftUpdateItem {
                        group: "g".into(),
                        key: "host".into(),
                        value: Value::String("h".into()),
                    },
                    DraftUpdateItem {
                        group: "g".into(),
                        key: "pass".into(),
                        value: Value::Secret(ct),
                    },
                ],
                deletes: vec![],
                shared_bindings: vec![],

                operator: String::new(),
                ts: 0,
                expected_draft_rev: None,
            },
            4,
        )
        .unwrap();
        sm.apply(
            &Command::Publish {
                project: "p".into(),
                branch: BranchName("dev".into()),
                comment: "v".into(),
                request_id: "r1".into(),

                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            5,
        )
        .unwrap();
        // 共享项 secret（代际 1）
        sm.apply(
            &Command::SharedDraftUpdate {
                item: SharedItem {
                    key: "tok".into(),
                    ty: ValueType::Secret,
                    secret: true,
                    required: false,
                    value: Value::Secret(cipher.encrypt_secret(b"shared-job").unwrap()),
                    version: 0,
                    description: None,
                },

                operator: String::new(),
            },
            6,
        )
        .unwrap();
        sm.apply(
            &Command::SharedPublish {
                comment: "c".into(),
                request_id: "sp".into(),

                operator: String::new(),
                ts: 0,

                cascade: SharedCascadeMode::Auto,

                policy: PublishPolicy::Block,
            },
            7,
        )
        .unwrap();

        // 轮换：KEK 2 成为当前 → 任务重包代际 1 的密文
        cipher.rotate_master_key([2u8; 32]);
        let job = RewrapDeks {
            cipher: cipher.clone(),
        };
        let sm_mutex = RwLock::new(sm);
        job.run(&sm_mutex).unwrap();

        let guard = sm_mutex.read().unwrap();
        let cfg = guard
            .get_config(&"p".into(), &BranchName("dev".into()), 0)
            .unwrap();
        match cfg.groups.get("g").unwrap().get("pass").unwrap() {
            Value::Secret(ct2) => {
                assert_eq!(ct2.dek_v, 2, "快照 secret 已重包到新代际");
                assert_eq!(cipher.decrypt_secret(ct2).unwrap(), b"job-secret");
            }
            _ => panic!("expected secret"),
        }
    }
}

#[cfg(test)]
mod auto_rollback_tests {
    use super::*;
    use dsh_core::command::{Command, DraftUpdateItem};
    use dsh_core::model::{
        BranchName, GrayRule, GroupDef, ItemDef, LabelSelector, PublishPolicy, Value, ValueType,
    };
    use dsh_core::InMemoryStore;
    use std::time::Duration;

    struct FakeProbe {
        rate: f64,
    }
    impl GrayHealthProbe for FakeProbe {
        fn error_rate(&self) -> Option<f64> {
            Some(self.rate)
        }
    }

    /// 装配一个带活跃灰度的状态机（项目 + 结构 + 稳定发布 + 草稿 + 灰度发布）。
    fn sm_with_active_gray() -> StateMachine {
        let mut sm = StateMachine::new(Box::new(InMemoryStore::new()));
        sm.apply(
            &Command::ProjectCreate {
                name: "p".into(),
                operator: String::new(),
                ts: 0,
                clone_from: None,
            },
            1,
        )
        .unwrap();
        sm.apply(
            &Command::StructureDraftSet {
                project: "p".into(),
                base_version: 1,
                groups: vec![GroupDef {
                    name: "app".into(),
                    items: vec![ItemDef {
                        key: "feature".into(),
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
        sm.apply(
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
        for (i, v) in ["stable-v", "gray-v"].iter().enumerate() {
            sm.apply(
                &Command::DraftUpdate {
                    project: "p".into(),
                    branch: BranchName("dev".into()),
                    updates: vec![DraftUpdateItem {
                        group: "app".into(),
                        key: "feature".into(),
                        value: Value::String(v.to_string()),
                    }],
                    deletes: vec![],
                    shared_bindings: vec![],
                    operator: String::new(),
                    ts: 0,
                    expected_draft_rev: None,
                },
                10 + i as i64,
            )
            .unwrap();
            if i == 0 {
                sm.apply(
                    &Command::Publish {
                        project: "p".into(),
                        branch: BranchName("dev".into()),
                        comment: "stable v2".into(),
                        request_id: "p1".into(),
                        operator: String::new(),
                        ts: 0,

                        policy: PublishPolicy::Block,
                    },
                    20,
                )
                .unwrap();
            }
        }
        sm.apply(
            &Command::GrayPublish {
                project: "p".into(),
                branch: BranchName("dev".into()),
                rule: GrayRule {
                    match_labels: vec![LabelSelector {
                        key: "zone".into(),
                        value: "cn-north-1".into(),
                    }],
                    ip_cidrs: vec![],
                    percentage: None,
                },
                comment: "g".into(),
                request_id: "g1".into(),
                operator: String::new(),
                ts: 0,

                policy: PublishPolicy::Block,
            },
            30,
        )
        .unwrap();
        sm
    }

    /// G5/D33 完整循环：错误率超阈值 → 自动 abort 活跃灰度 + 审计 gray_auto_abort。
    #[tokio::test]
    async fn auto_rollback_aborts_active_gray() {
        let sm = Arc::new(RwLock::new(sm_with_active_gray()));
        let (_leader_tx, leader_rx) = tokio::sync::watch::channel(true);
        let audit = dsh_observability::AuditLog::new(sm.clone(), None);
        spawn_gray_auto_rollback(
            sm.clone(),
            None,
            None,
            audit,
            Box::new(FakeProbe { rate: 0.9 }),
            0.05,
            Duration::from_millis(50),
            leader_rx,
        );
        // 等若干轮（50ms 间隔）→ 灰度应被清空
        tokio::time::sleep(Duration::from_millis(800)).await;
        let g = sm.read().unwrap();
        let st = g
            .get_branch_state(&"p".into(), &BranchName("dev".into()))
            .unwrap()
            .unwrap();
        assert_eq!(st.gray_seq, 0, "错误率超阈值 → 自动 abort");
        assert!(st.gray_rule.is_none());
        // 审计留痕
        let audits = g
            .get_audit(Some("gray_auto_abort"), None, None, 10)
            .unwrap();
        assert!(!audits.is_empty(), "gray_auto_abort 审计落库");
    }

    /// 负例：低错误率不误伤（灰度保持活跃）。
    #[tokio::test]
    async fn auto_rollback_skips_when_healthy() {
        let sm = Arc::new(RwLock::new(sm_with_active_gray()));
        let (_leader_tx, leader_rx) = tokio::sync::watch::channel(true);
        let audit = dsh_observability::AuditLog::new(sm.clone(), None);
        spawn_gray_auto_rollback(
            sm.clone(),
            None,
            None,
            audit,
            Box::new(FakeProbe { rate: 0.001 }),
            0.05,
            Duration::from_millis(50),
            leader_rx,
        );
        tokio::time::sleep(Duration::from_millis(400)).await;
        let g = sm.read().unwrap();
        let st = g
            .get_branch_state(&"p".into(), &BranchName("dev".into()))
            .unwrap()
            .unwrap();
        assert_eq!(st.gray_seq, 1, "低错误率不触发回滚");
    }
}
