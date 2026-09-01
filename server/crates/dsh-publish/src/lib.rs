//! 发布编排（模块 04）：提交前 secret 加密（I8）、发布/回滚/结构发布/草稿更新的写路径封装。
//! 确定性 apply 仍在 dsh-core 状态机；本模块负责 API 层到状态机之间的发布域逻辑。

use std::sync::{Arc, RwLock};

use dsh_core::command::{Command, DraftUpdateItem, SharedBinding};
use dsh_core::error::Error;
use dsh_core::model::{
    BranchName, DiffEntry, GrayRule, ProjectId, PublishEvent, PublishPolicy, SharedCascadeMode,
    Value, ValueType,
};
use dsh_core::StateMachine;
use dsh_crypto::Cipher;
use dsh_raft::RaftHandle;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 发布结果（handler 直接序列化）。
#[derive(Debug, Clone)]
pub struct PublishOutcome {
    pub version: u64,
    pub changes: Vec<DiffEntry>,
    /// G1/D35：Warn 模式下发布校验的告警明细（Block 模式恒空）。
    pub warnings: Vec<String>,
}

/// 结构发布结果。
#[derive(Debug, Clone)]
pub struct StructurePublishOutcome {
    pub affected: Vec<(String, u64)>,
}

/// 发布编排服务。
#[derive(Clone)]
pub struct PublishService {
    sm: Arc<RwLock<StateMachine>>,
    cipher: Option<Arc<Cipher>>,
    raft: Option<RaftHandle>,
    events_tx: Option<tokio::sync::broadcast::Sender<PublishEvent>>,
    /// G1/D35：发布校验策略（CLI 注入；默认 Block = 现状）
    pub publish_policy: PublishPolicy,
    /// G1/D36：共享发布级联模式（CLI 注入；默认 Auto = 现状）
    pub shared_cascade: SharedCascadeMode,
}

impl PublishService {
    pub fn new(
        sm: Arc<RwLock<StateMachine>>,
        cipher: Option<Arc<Cipher>>,
        raft: Option<RaftHandle>,
        events_tx: Option<tokio::sync::broadcast::Sender<PublishEvent>>,
    ) -> Self {
        Self {
            sm,
            cipher,
            raft,
            events_tx,
            publish_policy: PublishPolicy::Block,
            shared_cascade: SharedCascadeMode::Auto,
        }
    }

    /// 通用写（dev-single 直 apply；集群经 Raft client_write，含 leader 转发提示）。
    async fn write(&self, cmd: &Command, now_ms: i64) -> Result<dsh_raft::WriteOutcome, Error> {
        dsh_raft::write_command(
            &self.sm,
            self.raft.as_ref(),
            cmd,
            now_ms,
            self.events_tx.as_ref(),
        )
        .await
    }

    /// 提交前加密 secret 项（明文仅存在于 API 输入；状态机只存密文，保证 Raft apply 确定性，I8）。
    pub fn encrypt_secret_updates(
        &self,
        project: &ProjectId,
        updates: &mut [DraftUpdateItem],
    ) -> Result<(), Error> {
        let Some(cipher) = &self.cipher else {
            return Ok(());
        };
        let sm = self
            .sm
            .read()
            .map_err(|e| dsh_core::Error::internal(e.to_string()))?;
        let structure = sm.get_structure(project)?;
        let secret_keys: std::collections::HashSet<(String, String)> = structure
            .map(|s| {
                s.groups
                    .iter()
                    .flat_map(|g| {
                        g.items
                            .iter()
                            .filter(|it| it.ty == ValueType::Secret)
                            .map(move |it| (g.name.clone(), it.key.clone()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        drop(sm);
        for u in updates.iter_mut() {
            if secret_keys.contains(&(u.group.clone(), u.key.clone())) {
                if let Value::String(plain) = &u.value {
                    let ct = cipher
                        .encrypt_secret(plain.as_bytes())
                        .map_err(|e| Error::internal(format!("encrypt secret: {e}")))?;
                    u.value = Value::Secret(ct);
                }
            }
        }
        Ok(())
    }

    /// 值草稿更新（secret 项自动加密）。
    /// `expected_draft_rev`（乐观锁）：0 = 不校验（旧客户端）；>0 校验，不匹配 → Conflict。
    // 既有签名（7 显式参数 + self，clippy 阈值 7）；重构为参数结构体将波及 4 处调用点，
    // 与 CI 门禁 lint 相比风险更高，保留签名并显式豁免。
    #[allow(clippy::too_many_arguments)]
    pub async fn update_draft(
        &self,
        project: &ProjectId,
        branch: &BranchName,
        mut updates: Vec<DraftUpdateItem>,
        deletes: Vec<(String, String)>,
        bindings: Vec<SharedBinding>,
        expected_draft_rev: Option<u64>,
        operator: &str,
    ) -> Result<(), Error> {
        self.encrypt_secret_updates(project, &mut updates)?;
        self.write(
            &Command::DraftUpdate {
                project: project.clone(),
                branch: branch.clone(),
                updates,
                deletes,
                shared_bindings: bindings,
                operator: operator.to_string(),
                ts: now_ms(),
                expected_draft_rev,
            },
            now_ms(),
        )
        .await
        .map(|_| ())
    }

    /// 发布分支版本（幂等 I10：同 request_id 返回当前活动版本 + 空 changes）。
    pub async fn publish(
        &self,
        project: &ProjectId,
        branch: &BranchName,
        comment: &str,
        request_id: &str,
        operator: &str,
    ) -> Result<PublishOutcome, Error> {
        let wr = self
            .write(
                &Command::Publish {
                    project: project.clone(),
                    branch: branch.clone(),
                    comment: comment.to_string(),
                    request_id: request_id.to_string(),

                    operator: operator.to_string(),
                    ts: now_ms(),
                    policy: self.publish_policy,
                },
                now_ms(),
            )
            .await?;
        let version = if wr.version > 0 {
            wr.version
        } else {
            let sm = self
                .sm
                .read()
                .map_err(|e| dsh_core::Error::internal(e.to_string()))?;
            sm.get_branch_state(project, branch)?
                .map(|s| s.active_version)
                .unwrap_or(0)
        };
        let changes = wr
            .events
            .first()
            .map(|e| e.changes.clone())
            .unwrap_or_default();
        // G1/D35：Warn 模式下把校验告警暴露给调用方（Block 模式恒空）
        let warnings = if self.publish_policy == PublishPolicy::Warn {
            let sm = self
                .sm
                .read()
                .map_err(|e| dsh_core::Error::internal(e.to_string()))?;
            let draft = sm
                .get_branch_state(project, branch)?
                .map(|st| st.value_draft.clone())
                .unwrap_or_default();
            let structure = sm
                .get_structure(project)?
                .unwrap_or(dsh_core::model::Structure {
                    version: 0,
                    groups: vec![],
                });
            dsh_core::validator::validate_publish(&draft, &structure)
        } else {
            vec![]
        };
        Ok(PublishOutcome {
            version,
            changes,
            warnings,
        })
    }

    /// 回滚（新版本 = 旧版本内容，历史不可变 I6/I9）。
    pub async fn rollback(
        &self,
        project: &ProjectId,
        branch: &BranchName,
        to_version: u64,
        comment: &str,
        request_id: &str,
        operator: &str,
    ) -> Result<u64, Error> {
        let wr = self
            .write(
                &Command::Rollback {
                    project: project.clone(),
                    branch: branch.clone(),
                    to_version,
                    comment: comment.to_string(),
                    request_id: request_id.to_string(),

                    operator: operator.to_string(),
                    ts: now_ms(),
                },
                now_ms(),
            )
            .await?;
        if wr.version > 0 {
            return Ok(wr.version);
        }
        let sm = self
            .sm
            .read()
            .map_err(|e| dsh_core::Error::internal(e.to_string()))?;
        Ok(sm
            .get_branch_state(project, branch)?
            .map(|s| s.active_version)
            .unwrap_or(0))
    }

    /// 发布结构草稿（全部分支同时生效，I3/I5）。
    pub async fn publish_structure(
        &self,
        project: &ProjectId,
        comment: &str,
        request_id: &str,
        operator: &str,
    ) -> Result<StructurePublishOutcome, Error> {
        let wr = self
            .write(
                &Command::PublishStructure {
                    project: project.clone(),
                    comment: comment.to_string(),
                    request_id: request_id.to_string(),

                    operator: operator.to_string(),
                    ts: now_ms(),
                    policy: self.publish_policy,
                },
                now_ms(),
            )
            .await?;
        let affected = wr
            .events
            .iter()
            .map(|e| (e.branch.as_str().to_string(), e.version))
            .collect();
        Ok(StructurePublishOutcome { affected })
    }

    // ---------------- 灰度发布（G2 命令的写路径；G3 最小管理面端点使用） ----------------

    /// 灰度发布：固化草稿 → 灰度快照 + 灰度规则（写路径 dev-single/集群一致）。
    /// 返回 apply 产出的事件（事件 gray=true，供 watch 广播）。
    pub async fn gray_publish(
        &self,
        project: &ProjectId,
        branch: &BranchName,
        rule: GrayRule,
        comment: &str,
        request_id: &str,
        operator: &str,
    ) -> Result<Vec<PublishEvent>, Error> {
        let wr = self
            .write(
                &Command::GrayPublish {
                    project: project.clone(),
                    branch: branch.clone(),
                    rule,
                    comment: comment.to_string(),
                    request_id: request_id.to_string(),
                    operator: operator.to_string(),
                    ts: now_ms(),
                    policy: self.publish_policy,
                },
                now_ms(),
            )
            .await?;
        Ok(wr.events)
    }

    /// 灰度转正：灰度内容 → 新 active_version（next=max(active,gray)+1），清灰度。
    pub async fn gray_promote(
        &self,
        project: &ProjectId,
        branch: &BranchName,
        comment: &str,
        request_id: &str,
        operator: &str,
    ) -> Result<Vec<PublishEvent>, Error> {
        let wr = self
            .write(
                &Command::GrayPromote {
                    project: project.clone(),
                    branch: branch.clone(),
                    comment: comment.to_string(),
                    request_id: request_id.to_string(),
                    operator: operator.to_string(),
                    ts: now_ms(),
                },
                now_ms(),
            )
            .await?;
        Ok(wr.events)
    }

    /// 灰度下量/回滚：清灰度，事件携带回落版本号。
    pub async fn gray_abort(
        &self,
        project: &ProjectId,
        branch: &BranchName,
        comment: &str,
        request_id: &str,
        operator: &str,
    ) -> Result<Vec<PublishEvent>, Error> {
        let wr = self
            .write(
                &Command::GrayAbort {
                    project: project.clone(),
                    branch: branch.clone(),
                    comment: comment.to_string(),
                    request_id: request_id.to_string(),
                    operator: operator.to_string(),
                    ts: now_ms(),
                },
                now_ms(),
            )
            .await?;
        Ok(wr.events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsh_core::command::Command;
    use dsh_core::model::{GroupDef, ItemDef, ValueType};
    use dsh_core::{InMemoryStore, StateMachine};

    fn sm_with_structure() -> Arc<RwLock<StateMachine>> {
        let mut sm = StateMachine::new(Box::new(InMemoryStore::new()));
        sm.apply(
            &Command::ProjectCreate {
                name: "p".into(),
                operator: "test".to_string(),
                ts: 0,
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

                operator: "test".to_string(),
            },
            2,
        )
        .unwrap();
        sm.apply(
            &Command::PublishStructure {
                project: "p".into(),
                comment: "s".into(),
                request_id: "s1".into(),
                operator: "test".to_string(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            3,
        )
        .unwrap();
        Arc::new(RwLock::new(sm))
    }

    #[test]
    fn encrypt_secret_updates_encrypts_only_secret_items() {
        let sm = sm_with_structure();
        let svc = PublishService::new(sm, Some(Arc::new(Cipher::new([7u8; 32]))), None, None);
        let mut updates = vec![
            DraftUpdateItem {
                group: "g".into(),
                key: "host".into(),
                value: Value::String("h".into()),
            },
            DraftUpdateItem {
                group: "g".into(),
                key: "pass".into(),
                value: Value::String("s3cret".into()),
            },
        ];
        svc.encrypt_secret_updates(&ProjectId("p".into()), &mut updates)
            .unwrap();
        assert!(
            matches!(updates[0].value, Value::String(_)),
            "非 secret 项不加密"
        );
        assert!(
            matches!(updates[1].value, Value::Secret(_)),
            "secret 项提交前加密（I8）"
        );
    }

    #[test]
    fn encrypt_without_cipher_is_noop() {
        let sm = sm_with_structure();
        let svc = PublishService::new(sm, None, None, None);
        let mut updates = vec![DraftUpdateItem {
            group: "g".into(),
            key: "pass".into(),
            value: Value::String("plain".into()),
        }];
        svc.encrypt_secret_updates(&ProjectId("p".into()), &mut updates)
            .unwrap();
        assert!(matches!(updates[0].value, Value::String(_)));
    }
}
