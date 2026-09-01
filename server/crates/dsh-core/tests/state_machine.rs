//! 状态机流程测试（M1）：CRUD / 结构发布 / 值草稿 / 发布 / GetConfig / 幂等 / 隔离。

use dsh_core::command::{Command, DraftUpdateItem, SharedBinding};
use dsh_core::model::*;
use dsh_core::{ClientCtx, ErrorKind, InMemoryStore, ResolvedVersion, StateMachine, Value};
use std::collections::BTreeMap;

fn sm() -> StateMachine {
    StateMachine::new(Box::new(InMemoryStore::new()))
}

fn redis_structure() -> Vec<GroupDef> {
    vec![GroupDef {
        name: "redis".into(),
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
                key: "port".into(),
                ty: ValueType::Int,
                required: false,
                secret: false,
                validate: None,
                description: None,
                shared: false,
            },
            ItemDef {
                key: "password".into(),
                ty: ValueType::Secret,
                required: false,
                secret: true,
                validate: None,
                description: None,
                shared: false,
            },
        ],
    }]
}

fn setup(s: &mut StateMachine) -> (ProjectId, Vec<BranchName>) {
    assert!(s
        .apply(
            &Command::ProjectCreate {
                name: "order-service".into(),
                operator: String::new(),
                ts: 0,
            },
            1
        )
        .is_ok());
    let pid: ProjectId = "order-service".into();
    let branches = s.list_branches(&pid).unwrap();
    // 默认 dev/test/prod
    assert_eq!(branches.len(), 3);
    // 结构草稿 + 发布
    assert!(s
        .apply(
            &Command::StructureDraftSet {
                project: pid.clone(),
                base_version: 1,
                groups: redis_structure(),
                operator: String::new(),
            },
            2,
        )
        .is_ok());
    let events = s
        .apply(
            &Command::PublishStructure {
                project: pid.clone(),
                comment: "init".into(),
                request_id: "s1".into(),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            3,
        )
        .unwrap();
    assert_eq!(events.len(), 3); // 全部分支版本推进
    (pid, branches)
}

#[test]
fn full_flow_dev_publish() {
    let mut s = sm();
    let (pid, branches) = setup(&mut s);

    // 草稿编辑 dev
    assert!(s
        .apply(
            &Command::DraftUpdate {
                project: pid.clone(),
                branch: BranchName("dev".into()),
                updates: vec![
                    DraftUpdateItem {
                        group: "redis".into(),
                        key: "host".into(),
                        value: Value::String("127.0.0.1".into())
                    },
                    DraftUpdateItem {
                        group: "redis".into(),
                        key: "port".into(),
                        value: Value::Int(6379)
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
        .is_ok());

    // 草稿隔离（I4）：发布前 GetConfig 不变（结构发布后的版本，值仍为空）
    let before = s.get_config(&pid, &BranchName("dev".into()), 0).unwrap();
    assert_eq!(before.version, 1);
    assert!(before.groups.is_empty());

    // 发布 dev
    let events = s
        .apply(
            &Command::Publish {
                project: pid.clone(),
                branch: BranchName("dev".into()),
                comment: "dev host".into(),
                request_id: "r1".into(),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            5,
        )
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].ty, EventType::ValuePublish);
    assert_eq!(events[0].version, 2);

    // GetConfig 读到新版本（M1 验收核心）
    let after = s.get_config(&pid, &BranchName("dev".into()), 0).unwrap();
    assert_eq!(after.version, 2);
    assert_eq!(
        after.groups["redis"]["host"],
        Value::String("127.0.0.1".into())
    );
    assert_eq!(after.groups["redis"]["port"], Value::Int(6379));

    // 其他分支不受影响（仍为结构发布版本 1，空值）
    let test = s.get_config(&pid, &BranchName("test".into()), 0).unwrap();
    assert_eq!(test.version, 1);
    assert!(test.groups.is_empty());
    assert_eq!(branches.len(), 3);
}

#[test]
fn publish_is_idempotent_by_request_id() {
    let mut s = sm();
    let (pid, _) = setup(&mut s);
    let b = BranchName("dev".into());
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: b.clone(),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "host".into(),
                value: Value::String("x".into()),
            }],
            deletes: vec![],
            shared_bindings: vec![],

            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        4,
    )
    .unwrap();
    let cmd = Command::Publish {
        project: pid.clone(),
        branch: b.clone(),
        comment: "c".into(),
        request_id: "r9".into(),

        operator: String::new(),
        ts: 0,
        policy: PublishPolicy::Block,
    };
    let first = s.apply(&cmd, 5).unwrap();
    assert_eq!(first.len(), 1);
    // 同 request_id 重放 → 不重复生效（I10）
    let second = s.apply(&cmd, 6).unwrap();
    assert!(second.is_empty());
    let snap = s.get_config(&pid, &b, 0).unwrap();
    assert_eq!(snap.version, 2);
}

#[test]
fn required_unset_blocks_publish() {
    let mut s = sm();
    let (pid, _) = setup(&mut s);
    let b = BranchName("dev".into());
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: b.clone(),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "port".into(),
                value: Value::Int(6379),
            }],
            deletes: vec![],
            shared_bindings: vec![],

            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        4,
    )
    .unwrap();
    let err = s
        .apply(
            &Command::Publish {
                project: pid.clone(),
                branch: b.clone(),
                comment: "c".into(),
                request_id: "r2".into(),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            5,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::PublishBlocked);
    // 未发布：版本不变
    assert_eq!(s.get_config(&pid, &b, 0).unwrap().version, 1);
}

#[test]
fn no_draft_publish_errors() {
    let mut s = sm();
    let (pid, _) = setup(&mut s);
    let err = s
        .apply(
            &Command::Publish {
                project: pid.clone(),
                branch: BranchName("test".into()),
                comment: "c".into(),
                request_id: "r3".into(),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            5,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::NoDraft);
}

#[test]
fn branch_inherits_structure_and_values() {
    let mut s = sm();
    let (pid, _) = setup(&mut s);
    // dev 发布一些值
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: BranchName("dev".into()),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "host".into(),
                value: Value::String("10.0.0.1".into()),
            }],
            deletes: vec![],
            shared_bindings: vec![],

            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        4,
    )
    .unwrap();
    s.apply(
        &Command::Publish {
            project: pid.clone(),
            branch: BranchName("dev".into()),
            comment: "c".into(),
            request_id: "r4".into(),

            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        5,
    )
    .unwrap();
    // 新分支从 dev 继承活动版本值到草稿
    assert!(s
        .apply(
            &Command::BranchCreate {
                project: pid.clone(),
                name: "gray".into(),
                source: Some(BranchName("dev".into())),
                operator: String::new(),
                ts: 0,
            },
            6
        )
        .is_ok());
    let st = s
        .get_branch_state(&pid, &BranchName("gray".into()))
        .unwrap()
        .unwrap();
    assert_eq!(st.structure_version, 2);
    assert_eq!(
        st.value_draft["redis"]["host"].value,
        Value::String("10.0.0.1".into())
    );
}

#[test]
fn draft_update_validates_unknown_item_and_type() {
    let mut s = sm();
    let (pid, _) = setup(&mut s);
    // 未知 item
    let err = s
        .apply(
            &Command::DraftUpdate {
                project: pid.clone(),
                branch: BranchName("dev".into()),
                updates: vec![DraftUpdateItem {
                    group: "redis".into(),
                    key: "nope".into(),
                    value: Value::String("x".into()),
                }],
                deletes: vec![],
                shared_bindings: vec![],
                operator: String::new(),
                ts: 0,
                expected_draft_rev: None,
            },
            4,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Validation);
    // 类型不匹配
    let err = s
        .apply(
            &Command::DraftUpdate {
                project: pid.clone(),
                branch: BranchName("dev".into()),
                updates: vec![DraftUpdateItem {
                    group: "redis".into(),
                    key: "port".into(),
                    value: Value::String("abc".into()),
                }],
                deletes: vec![],
                shared_bindings: vec![],
                operator: String::new(),
                ts: 0,
                expected_draft_rev: None,
            },
            4,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Validation);
}

#[test]
fn duplicate_project_conflicts() {
    let mut s = sm();
    s.apply(
        &Command::ProjectCreate {
            name: "p1".into(),
            operator: String::new(),
            ts: 0,
        },
        1,
    )
    .unwrap();
    let err = s
        .apply(
            &Command::ProjectCreate {
                name: "p1".into(),
                operator: String::new(),
                ts: 0,
            },
            2,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Conflict);
}

#[test]
fn project_delete_removes_everything() {
    let mut s = sm();
    let (pid, _) = setup(&mut s);
    s.apply(
        &Command::ProjectDelete {
            id: pid.clone(),
            operator: String::new(),
        },
        10,
    )
    .unwrap();
    assert!(s.get_project(&pid).unwrap().is_none());
    assert!(s.list_projects().unwrap().is_empty());
    assert!(s.list_branches(&pid).unwrap().is_empty());
    assert!(s.get_structure(&pid).unwrap().is_none());
}

#[test]
fn branch_delete_guards_published() {
    let mut s = sm();
    let (pid, _) = setup(&mut s);
    // 结构发布后 active_version=1 → 不可删
    let err = s
        .apply(
            &Command::BranchDelete {
                project: pid.clone(),
                name: BranchName("test".into()),
                operator: String::new(),
            },
            5,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Conflict);
}

// ---------------- M2：回滚（I6/I9） ----------------

#[test]
fn rollback_creates_new_version_with_old_content() {
    let mut s = sm();
    let (pid, _) = setup(&mut s);
    let b = BranchName("dev".into());
    // 发布 v2（结构发布 v1）
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: b.clone(),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "host".into(),
                value: Value::String("10.0.0.9".into()),
            }],
            deletes: vec![],
            shared_bindings: vec![],

            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        4,
    )
    .unwrap();
    s.apply(
        &Command::Publish {
            project: pid.clone(),
            branch: b.clone(),
            comment: "v2".into(),
            request_id: "r1".into(),

            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        5,
    )
    .unwrap();
    assert_eq!(s.get_config(&pid, &b, 0).unwrap().version, 2);

    // 回滚到 v1
    let events = s
        .apply(
            &Command::Rollback {
                project: pid.clone(),
                branch: b.clone(),
                to_version: 1,
                comment: "rollback".into(),
                request_id: "rb1".into(),
                operator: String::new(),
                ts: 0,
            },
            6,
        )
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].ty, EventType::Rollback);
    assert_eq!(events[0].version, 3);

    // v3 内容 = v1 内容（host 空）
    let snap = s.get_config(&pid, &b, 0).unwrap();
    assert_eq!(snap.version, 3);
    assert!(snap.groups.is_empty());

    // 历史记录 rollback_of=1
    let rec = s.get_version_record(&pid, &b, 3).unwrap().unwrap();
    assert_eq!(rec.rollback_of, Some(1));

    // 幂等：同 request_id 不重复
    let again = s
        .apply(
            &Command::Rollback {
                project: pid.clone(),
                branch: b.clone(),
                to_version: 1,
                comment: "x".into(),
                request_id: "rb1".into(),
                operator: String::new(),
                ts: 0,
            },
            7,
        )
        .unwrap();
    assert!(again.is_empty());
    assert_eq!(s.get_config(&pid, &b, 0).unwrap().version, 3);
}

#[test]
fn rollback_invalid_version_rejected() {
    let mut s = sm();
    let (pid, _) = setup(&mut s);
    let b = BranchName("dev".into());
    let err = s
        .apply(
            &Command::Rollback {
                project: pid.clone(),
                branch: b.clone(),
                to_version: 1,
                comment: "x".into(),
                request_id: "r".into(),
                operator: String::new(),
                ts: 0,
            },
            5,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Validation); // to_version >= active(1)
    let err = s
        .apply(
            &Command::Rollback {
                project: pid.clone(),
                branch: b.clone(),
                to_version: 99,
                comment: "x".into(),
                request_id: "r".into(),
                operator: String::new(),
                ts: 0,
            },
            5,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Validation); // 超出活动版本范围
}

// ---------------- M2：共享库 + 引用（结构内嵌 shared_ref）+ 级联 ----------------

fn publish_shared(s: &mut StateMachine, key: &str, value: Value, request_id: &str) {
    s.apply(
        &Command::SharedDraftUpdate {
            item: SharedItem {
                key: key.into(),
                ty: value.value_type(),
                secret: false,
                required: false,
                value,
                version: 0,
                description: None,
            },
            operator: String::new(),
        },
        10,
    )
    .unwrap();
    s.apply(
        &Command::SharedPublish {
            comment: "shared".into(),
            request_id: request_id.into(),
            operator: String::new(),
            ts: 0,
            cascade: SharedCascadeMode::Auto,
            policy: PublishPolicy::Block,
        },
        11,
    )
    .unwrap();
}

/// redis 组（setup 结构）+ db 组（db/host 引用共享项 db_host）。
fn struct_with_shared_ref() -> Vec<GroupDef> {
    let mut groups = redis_structure();
    groups.push(GroupDef {
        name: "db".into(),
        items: vec![ItemDef {
            key: "host".into(),
            ty: ValueType::String,
            required: false,
            secret: false,
            validate: None,
            description: Some("数据库地址（共享）".into()),
            shared: true,
        }],
    });
    groups
}

#[test]
fn shared_ref_materializes_and_cascades() {
    let mut s = sm();
    publish_shared(
        &mut s,
        "db_host",
        Value::String("db.internal".into()),
        "sp1",
    );

    let (pid, _) = setup(&mut s); // 结构 v1（redis 组）
                                  // 结构 v2：加 db 组（db/host 引用共享项 db_host）
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: 2,
            groups: struct_with_shared_ref(),
            operator: String::new(),
        },
        12,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "add db".into(),
            request_id: "s2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();

    // 分支发布（dev：只填 redis/host + 绑定 db/host → db_host）→ db/host 由共享物化
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: BranchName("dev".into()),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "host".into(),
                value: Value::String("127.0.0.1".into()),
            }],
            deletes: vec![],
            shared_bindings: vec![SharedBinding {
                group: "db".into(),
                key: "host".into(),
                shared_key: "db_host".into(),
            }],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        15,
    )
    .unwrap();
    s.apply(
        &Command::Publish {
            project: pid.clone(),
            branch: BranchName("dev".into()),
            comment: "dev".into(),
            request_id: "r1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        16,
    )
    .unwrap();
    let dev_ver = s.get_config(&pid, &BranchName("dev".into()), 0).unwrap();
    assert_eq!(
        dev_ver.groups["db"]["host"],
        Value::String("db.internal".into())
    );

    // 共享项变更 → 级联：dev 分支版本推进，值更新（shared_usage 扫描结构命中）
    publish_shared(
        &mut s,
        "db_host",
        Value::String("db.internal.2".into()),
        "sp2",
    );
    let dev_after = s.get_config(&pid, &BranchName("dev".into()), 0).unwrap();
    assert_eq!(
        dev_after.groups["db"]["host"],
        Value::String("db.internal.2".into())
    );
    // 事件类型 SharedCascade
    let hist = s.version_history(&pid, &BranchName("dev".into())).unwrap();
    assert!(hist.len() >= 3);
}

#[test]
fn shared_ref_rejects_local_draft_value() {
    let mut s = sm();
    publish_shared(&mut s, "db_host", Value::String("sh".into()), "sp1");
    let (pid, _) = setup(&mut s);
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: 2,
            groups: struct_with_shared_ref(),
            operator: String::new(),
        },
        12,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "s".into(),
            request_id: "s2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    // 引用项只读：分支草稿写本地值 → Validation 拒绝
    let err = s
        .apply(
            &Command::DraftUpdate {
                project: pid.clone(),
                branch: BranchName("dev".into()),
                updates: vec![DraftUpdateItem {
                    group: "db".into(),
                    key: "host".into(),
                    value: Value::String("local".into()),
                }],
                deletes: vec![],
                shared_bindings: vec![],
                operator: String::new(),
                ts: 0,
                expected_draft_rev: None,
            },
            15,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Validation);
}

#[test]
fn structure_publish_cleans_draft_of_shared_ref_items() {
    let mut s = sm();
    publish_shared(&mut s, "db_host", Value::String("sh".into()), "sp1");
    let (pid, _) = setup(&mut s); // v1 redis 组
                                  // v2：db/host 为本地项 → dev 草稿写本地值
    let mut g2 = redis_structure();
    g2.push(GroupDef {
        name: "db".into(),
        items: vec![ItemDef {
            key: "host".into(),
            ty: ValueType::String,
            required: false,
            secret: false,
            validate: None,
            description: None,
            shared: false,
        }],
    });
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: 2,
            groups: g2,
            operator: String::new(),
        },
        12,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "s".into(),
            request_id: "s2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: BranchName("dev".into()),
            updates: vec![DraftUpdateItem {
                group: "db".into(),
                key: "host".into(),
                value: Value::String("local".into()),
            }],
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        15,
    )
    .unwrap();
    assert!(s
        .get_branch_state(&pid, &BranchName("dev".into()))
        .unwrap()
        .unwrap()
        .value_draft["db"]
        .contains_key("host"));
    // v3：db/host 改为共享引用 → 发布后清理其草稿值
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: 3,
            groups: struct_with_shared_ref(),
            operator: String::new(),
        },
        16,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "s".into(),
            request_id: "s3".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        17,
    )
    .unwrap();
    let st = s
        .get_branch_state(&pid, &BranchName("dev".into()))
        .unwrap()
        .unwrap();
    assert!(!st
        .value_draft
        .get("db")
        .is_some_and(|m| m.contains_key("host")));
}

#[test]
fn binding_missing_shared_item_rejected() {
    let mut s = sm();
    let (pid, _) = setup(&mut s);
    // 结构标记 shared=true 保存/发布均成功（选择在分支，结构不再校验共享项存在性）
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: 2,
            groups: struct_with_shared_ref(),
            operator: String::new(),
        },
        12,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "s".into(),
            request_id: "s2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    // 分支绑定未发布的共享项 → DraftUpdate 校验拒绝
    let err = s
        .apply(
            &Command::DraftUpdate {
                project: pid.clone(),
                branch: BranchName("dev".into()),
                updates: vec![],
                deletes: vec![],
                shared_bindings: vec![SharedBinding {
                    group: "db".into(),
                    key: "host".into(),
                    shared_key: "nope".into(),
                }],
                operator: String::new(),
                ts: 0,
                expected_draft_rev: None,
            },
            15,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Validation);
}

#[test]
fn binding_type_mismatch_rejected() {
    let mut s = sm();
    publish_shared(&mut s, "db_port", Value::Int(5432), "sp1");
    let (pid, _) = setup(&mut s);
    let mut groups = redis_structure();
    groups.push(GroupDef {
        name: "db".into(),
        items: vec![ItemDef {
            key: "port".into(),
            ty: ValueType::String, // 与共享项 int 不一致
            required: false,
            secret: false,
            validate: None,
            description: None,
            shared: true,
        }],
    });
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: 2,
            groups,
            operator: String::new(),
        },
        12,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "s".into(),
            request_id: "s2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    // 分支绑定类型不一致的共享项 → DraftUpdate 校验拒绝
    let err = s
        .apply(
            &Command::DraftUpdate {
                project: pid.clone(),
                branch: BranchName("dev".into()),
                updates: vec![],
                deletes: vec![],
                shared_bindings: vec![SharedBinding {
                    group: "db".into(),
                    key: "port".into(),
                    shared_key: "db_port".into(),
                }],
                operator: String::new(),
                ts: 0,
                expected_draft_rev: None,
            },
            15,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Validation);
}

#[test]
fn shared_delete_blocks_when_bound() {
    let mut s = sm();
    publish_shared(&mut s, "db_host", Value::String("sh".into()), "sp1");
    let (pid, _) = setup(&mut s);
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: 2,
            groups: struct_with_shared_ref(),
            operator: String::new(),
        },
        12,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "s".into(),
            request_id: "s2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    // 结构标记 shared 但不绑定 → 删除仍成功（引用保护发生在分支绑定层）
    s.apply(
        &Command::SharedDelete {
            key: "db_host".into(),
            operator: String::new(),
        },
        14,
    )
    .unwrap();
    // 重新发布共享项 + 分支绑定 → 删除被拒（Conflict，detail 含分支）
    publish_shared(&mut s, "db_host", Value::String("sh".into()), "sp2");
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: BranchName("dev".into()),
            updates: vec![],
            deletes: vec![],
            shared_bindings: vec![SharedBinding {
                group: "db".into(),
                key: "host".into(),
                shared_key: "db_host".into(),
            }],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        15,
    )
    .unwrap();
    let err = s
        .apply(
            &Command::SharedDelete {
                key: "db_host".into(),
                operator: String::new(),
            },
            16,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Conflict);
    assert!(s.get_shared("db_host").unwrap().is_some());
}

#[test]
fn shared_delete_unreferenced_succeeds_idempotent() {
    let mut s = sm();
    publish_shared(&mut s, "db_host", Value::String("sh".into()), "sp1");
    // 未引用 → 删除已发布项；幂等：再删成功
    s.apply(
        &Command::SharedDelete {
            key: "db_host".into(),
            operator: String::new(),
        },
        12,
    )
    .unwrap();
    assert!(s.get_shared("db_host").unwrap().is_none());
    assert!(s
        .apply(
            &Command::SharedDelete {
                key: "db_host".into(),
                operator: String::new(),
            },
            13,
        )
        .is_ok());
    // 草稿路径：发布前删除草稿项
    publish_shared(&mut s, "db_host", Value::String("sh".into()), "sp2");
    s.apply(
        &Command::SharedDelete {
            key: "db_host".into(),
            operator: String::new(),
        },
        14,
    )
    .unwrap();
    assert!(s.list_shared_drafts().unwrap().is_empty());
}

#[test]
fn shared_usage_reverse_mapping() {
    let mut s = sm();
    publish_shared(&mut s, "db_host", Value::String("sh".into()), "sp1");
    let (pid, _) = setup(&mut s);
    assert!(s.shared_usage("db_host").unwrap().is_empty());
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: 2,
            groups: struct_with_shared_ref(),
            operator: String::new(),
        },
        12,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "s".into(),
            request_id: "s2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    // 仅结构标记不产生反向引用；dev 分支绑定后命中
    assert!(s.shared_usage("db_host").unwrap().is_empty());
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: BranchName("dev".into()),
            updates: vec![],
            deletes: vec![],
            shared_bindings: vec![SharedBinding {
                group: "db".into(),
                key: "host".into(),
                shared_key: "db_host".into(),
            }],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        15,
    )
    .unwrap();
    let usage = s.shared_usage("db_host").unwrap();
    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].0, pid);
    assert_eq!(usage[0].1, BranchName("dev".into()));
    assert_eq!(usage[0].2, "db");
    assert_eq!(usage[0].3, "host");
}
// ---------------- 分支级共享引用（shared-ref branch-scope） ----------------

/// 核心场景：结构声明 shared=true，dev 绑 A、prod 绑 B → 各自发布 → 快照值不同。
#[test]
fn branch_scoped_binding_differs() {
    let mut s = sm();
    publish_shared(&mut s, "db_a", Value::String("dev-db".into()), "sp1");
    publish_shared(&mut s, "db_b", Value::String("prod-db".into()), "sp2");
    let (pid, _) = setup(&mut s);
    // 结构 v2：redis/host 标记引用共享（type String）
    let mut groups = redis_structure();
    for g in &mut groups {
        for item in &mut g.items {
            if item.key == "host" {
                item.shared = true;
            }
        }
    }
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: 2,
            groups,
            operator: String::new(),
        },
        12,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "s".into(),
            request_id: "s2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    // dev 绑 db_a、prod 绑 db_b（同结构 item，不同分支不同选择）
    let dev = BranchName("dev".into());
    let prod = BranchName("prod".into());
    for (b, rk) in [(&dev, "db_a"), (&prod, "db_b")] {
        s.apply(
            &Command::DraftUpdate {
                project: pid.clone(),
                branch: b.clone(),
                updates: vec![DraftUpdateItem {
                    group: "redis".into(),
                    key: "port".into(),
                    value: Value::Int(6379),
                }],
                deletes: vec![],
                shared_bindings: vec![SharedBinding {
                    group: "redis".into(),
                    key: "host".into(),
                    shared_key: rk.into(),
                }],
                operator: String::new(),
                ts: 0,
                expected_draft_rev: None,
            },
            15,
        )
        .unwrap();
        s.apply(
            &Command::Publish {
                project: pid.clone(),
                branch: b.clone(),
                comment: "v".into(),
                request_id: "r1".into(),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            16,
        )
        .unwrap();
    }
    let dev_cfg = s.get_config(&pid, &dev, 0).unwrap();
    let prod_cfg = s.get_config(&pid, &prod, 0).unwrap();
    assert_eq!(
        dev_cfg.groups["redis"]["host"],
        Value::String("dev-db".into()),
        "dev 取 db_a"
    );
    assert_eq!(
        prod_cfg.groups["redis"]["host"],
        Value::String("prod-db".into()),
        "prod 取 db_b"
    );
}

/// 未绑定 shared 项：Block 拒绝发布（明细列出），Warn 记录继续（快照不含该项）。
#[test]
fn shared_item_unbound_blocks_publish() {
    let mut s = sm();
    let (pid, _) = setup(&mut s);
    let dev = BranchName("dev".into());
    let mut groups = redis_structure();
    for g in &mut groups {
        for item in &mut g.items {
            if item.key == "host" {
                item.shared = true;
            }
        }
    }
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: 2,
            groups,
            operator: String::new(),
        },
        12,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "s".into(),
            request_id: "s2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "port".into(),
                value: Value::Int(6379),
            }],
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        15,
    )
    .unwrap();
    // Block：未选择引用共享项 → 发布阻断
    let err = s
        .apply(
            &Command::Publish {
                project: pid.clone(),
                branch: dev.clone(),
                comment: "v".into(),
                request_id: "r1".into(),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            16,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::PublishBlocked);
    let detail = err.detail.as_ref().expect("detail 携带明细");
    let joined = serde_json::to_string(detail).unwrap();
    assert!(joined.contains("未选择引用共享项"), "{joined}");
    // Warn：记录继续发布，快照不含该 shared 项
    s.apply(
        &Command::Publish {
            project: pid.clone(),
            branch: dev.clone(),
            comment: "v".into(),
            request_id: "r2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Warn,
        },
        17,
    )
    .unwrap();
    let cfg = s.get_config(&pid, &dev, 0).unwrap();
    assert!(
        !cfg.groups["redis"].contains_key("host"),
        "Warn 快照不含未绑定项"
    );
}

/// 绑定类型不一致 → DraftUpdate 拒绝（绑定时校验，非发布期）。
#[test]
fn binding_type_mismatch_rejected_at_binding() {
    let mut s = sm();
    publish_shared(&mut s, "db_port", Value::Int(5432), "sp1");
    let (pid, _) = setup(&mut s);
    let dev = BranchName("dev".into());
    let mut groups = redis_structure();
    groups.push(GroupDef {
        name: "db".into(),
        items: vec![ItemDef {
            key: "host".into(),
            ty: ValueType::String,
            required: false,
            secret: false,
            validate: None,
            description: None,
            shared: true,
        }],
    });
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: 2,
            groups,
            operator: String::new(),
        },
        12,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "s".into(),
            request_id: "s2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    // 未发布共享项 → 拒
    let err = s
        .apply(
            &Command::DraftUpdate {
                project: pid.clone(),
                branch: dev.clone(),
                updates: vec![],
                deletes: vec![],
                shared_bindings: vec![SharedBinding {
                    group: "db".into(),
                    key: "host".into(),
                    shared_key: "nope".into(),
                }],
                operator: String::new(),
                ts: 0,
                expected_draft_rev: None,
            },
            15,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Validation);
    // 类型不一致 → 拒
    let err = s
        .apply(
            &Command::DraftUpdate {
                project: pid.clone(),
                branch: dev.clone(),
                updates: vec![],
                deletes: vec![],
                shared_bindings: vec![SharedBinding {
                    group: "db".into(),
                    key: "host".into(),
                    shared_key: "db_port".into(),
                }],
                operator: String::new(),
                ts: 0,
                expected_draft_rev: None,
            },
            16,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Validation);
}

/// 守卫：只改绑定（无值草稿）也可发布；再次发布无变更 → NoDraft。
#[test]
fn binding_only_publish_allowed() {
    let mut s = sm();
    publish_shared(&mut s, "db_host", Value::String("sh".into()), "sp1");
    let (pid, _) = setup(&mut s);
    let dev = BranchName("dev".into());
    // redis 组本地项全 optional（草稿=全量状态，绑定-only 发布不允许必填未设）
    let mut groups = redis_structure();
    for g in &mut groups {
        for item in &mut g.items {
            item.required = false;
        }
    }
    groups.push(GroupDef {
        name: "db".into(),
        items: vec![ItemDef {
            key: "host".into(),
            ty: ValueType::String,
            required: false,
            secret: false,
            validate: None,
            description: Some("数据库地址（共享）".into()),
            shared: true,
        }],
    });
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: 2,
            groups,
            operator: String::new(),
        },
        12,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "s".into(),
            request_id: "s2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    // 只改绑定（updates 空）→ 发布成功（守卫放行 bindings_dirty）
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![],
            deletes: vec![],
            shared_bindings: vec![SharedBinding {
                group: "db".into(),
                key: "host".into(),
                shared_key: "db_host".into(),
            }],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        15,
    )
    .unwrap();
    let st = s.get_branch_state(&pid, &dev).unwrap().unwrap();
    assert!(st.bindings_dirty, "绑定变更 → 脏标记");
    s.apply(
        &Command::Publish {
            project: pid.clone(),
            branch: dev.clone(),
            comment: "v".into(),
            request_id: "r1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        16,
    )
    .unwrap();
    let st = s.get_branch_state(&pid, &dev).unwrap().unwrap();
    assert!(!st.bindings_dirty, "发布后脏标记复位");
    assert!(
        st.shared_bindings["db"].contains_key("host"),
        "绑定跨发布持久化"
    );
    // 再次发布（无值无绑定变更）→ NoDraft
    let err = s
        .apply(
            &Command::Publish {
                project: pid.clone(),
                branch: dev.clone(),
                comment: "v2".into(),
                request_id: "r2".into(),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            17,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::NoDraft);
}

/// 绑定跨发布持久化：值草稿发布后绑定仍在，下次物化继续生效。
#[test]
fn bindings_persist_after_publish() {
    let mut s = sm();
    publish_shared(&mut s, "db_host", Value::String("sh1".into()), "sp1");
    let (pid, _) = setup(&mut s);
    let dev = BranchName("dev".into());
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: 2,
            groups: struct_with_shared_ref(),
            operator: String::new(),
        },
        12,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "s".into(),
            request_id: "s2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    // 第一次发布：值 + 绑定
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "host".into(),
                value: Value::String("127.0.0.1".into()),
            }],
            deletes: vec![],
            shared_bindings: vec![SharedBinding {
                group: "db".into(),
                key: "host".into(),
                shared_key: "db_host".into(),
            }],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        15,
    )
    .unwrap();
    s.apply(
        &Command::Publish {
            project: pid.clone(),
            branch: dev.clone(),
            comment: "v1".into(),
            request_id: "r1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        16,
    )
    .unwrap();
    // 共享项更新（Auto 级联会推进 dev）→ 改 Manual 只更共享版本，验证绑定持久化后的下次发布
    s.apply(
        &Command::SharedDraftUpdate {
            item: SharedItem {
                key: "db_host".into(),
                ty: ValueType::String,
                secret: false,
                required: false,
                value: Value::String("sh2".into()),
                version: 1,
                description: None,
            },
            operator: String::new(),
        },
        17,
    )
    .unwrap();
    s.apply(
        &Command::SharedPublish {
            comment: "v2".into(),
            request_id: "sp2".into(),
            operator: String::new(),
            ts: 0,
            cascade: SharedCascadeMode::Manual,
            policy: PublishPolicy::Block,
        },
        18,
    )
    .unwrap();
    // 值草稿变更 + 发布 → 物化仍读绑定（sh2），绑定未丢
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![
                DraftUpdateItem {
                    group: "redis".into(),
                    key: "host".into(),
                    value: Value::String("127.0.0.1".into()),
                },
                DraftUpdateItem {
                    group: "redis".into(),
                    key: "port".into(),
                    value: Value::Int(6379),
                },
            ],
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        19,
    )
    .unwrap();
    s.apply(
        &Command::Publish {
            project: pid.clone(),
            branch: dev.clone(),
            comment: "v3".into(),
            request_id: "r2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        20,
    )
    .unwrap();
    let cfg = s.get_config(&pid, &dev, 0).unwrap();
    assert_eq!(
        cfg.groups["db"]["host"],
        Value::String("sh2".into()),
        "绑定持久化：下次发布物化新共享值"
    );
}

/// 结构发布清理绑定：删除 item / shared→local 翻转 / ty 变更 → 绑定丢弃。
#[test]
fn structure_publish_cleans_bindings() {
    let mut s = sm();
    publish_shared(&mut s, "db_host", Value::String("sh".into()), "sp1");
    publish_shared(&mut s, "db_port", Value::Int(5432), "sp2");
    let (pid, _) = setup(&mut s);
    let dev = BranchName("dev".into());
    // v2：db 组 host(shared) + port(shared, String 声明)
    let mut g2 = redis_structure();
    g2.push(GroupDef {
        name: "db".into(),
        items: vec![
            ItemDef {
                key: "host".into(),
                ty: ValueType::String,
                required: false,
                secret: false,
                validate: None,
                description: None,
                shared: true,
            },
            ItemDef {
                key: "port".into(),
                ty: ValueType::String,
                required: false,
                secret: false,
                validate: None,
                description: None,
                shared: true,
            },
        ],
    });
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: 2,
            groups: g2,
            operator: String::new(),
        },
        12,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "s".into(),
            request_id: "s2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    // dev 绑定 host→db_host；port 声明 String 无法绑 int 的 db_port（校验即拒），跳过
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![],
            deletes: vec![],
            shared_bindings: vec![SharedBinding {
                group: "db".into(),
                key: "host".into(),
                shared_key: "db_host".into(),
            }],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        15,
    )
    .unwrap();
    assert!(s
        .get_branch_state(&pid, &dev)
        .unwrap()
        .unwrap()
        .shared_bindings["db"]
        .contains_key("host"));
    // v3：db/host 改本地项（shared→local 翻转）→ 发布后绑定被清
    let mut g3 = redis_structure();
    g3.push(GroupDef {
        name: "db".into(),
        items: vec![ItemDef {
            key: "host".into(),
            ty: ValueType::String,
            required: false,
            secret: false,
            validate: None,
            description: None,
            shared: false,
        }],
    });
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: 3,
            groups: g3,
            operator: String::new(),
        },
        16,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "s".into(),
            request_id: "s3".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        17,
    )
    .unwrap();
    let st = s.get_branch_state(&pid, &dev).unwrap().unwrap();
    assert!(
        !st.shared_bindings.contains_key("db"),
        "shared→local 翻转 → 绑定被清"
    );
}

/// 共享发布级联：仅推进绑定该共享项的分支，未绑定分支版本不变。
#[test]
fn shared_publish_cascades_only_bound_branches() {
    let mut s = sm();
    publish_shared(&mut s, "db_host", Value::String("sh1".into()), "sp1");
    let (pid, _) = setup(&mut s);
    let dev = BranchName("dev".into());
    let prod = BranchName("prod".into());
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: 2,
            groups: struct_with_shared_ref(),
            operator: String::new(),
        },
        12,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "s".into(),
            request_id: "s2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    // dev 绑定并发布；prod 不绑定
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![
                DraftUpdateItem {
                    group: "redis".into(),
                    key: "host".into(),
                    value: Value::String("127.0.0.1".into()),
                },
                DraftUpdateItem {
                    group: "redis".into(),
                    key: "port".into(),
                    value: Value::Int(6379),
                },
            ],
            deletes: vec![],
            shared_bindings: vec![SharedBinding {
                group: "db".into(),
                key: "host".into(),
                shared_key: "db_host".into(),
            }],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        15,
    )
    .unwrap();
    s.apply(
        &Command::Publish {
            project: pid.clone(),
            branch: dev.clone(),
            comment: "v1".into(),
            request_id: "r1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        16,
    )
    .unwrap();
    let prod_ver_before = s
        .get_branch_state(&pid, &prod)
        .unwrap()
        .unwrap()
        .active_version;
    // 共享项更新 → Auto 级联只推进 dev
    publish_shared(&mut s, "db_host", Value::String("sh2".into()), "sp2");
    let dev_after = s.get_config(&pid, &dev, 0).unwrap();
    assert_eq!(
        dev_after.groups["db"]["host"],
        Value::String("sh2".into()),
        "dev 级联取新值"
    );
    let prod_ver_after = s
        .get_branch_state(&pid, &prod)
        .unwrap()
        .unwrap()
        .active_version;
    assert_eq!(prod_ver_before, prod_ver_after, "prod 未绑定 → 版本不变");
}

/// 分支创建(source)：跳过 shared 项的值复制 + 继承源分支绑定。
#[test]
fn branch_create_source_skips_shared_and_inherits_bindings() {
    let mut s = sm();
    publish_shared(&mut s, "db_host", Value::String("sh".into()), "sp1");
    let (pid, _) = setup(&mut s);
    let dev = BranchName("dev".into());
    let staging = BranchName("staging".into());
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: 2,
            groups: struct_with_shared_ref(),
            operator: String::new(),
        },
        12,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "s".into(),
            request_id: "s2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    // dev：写 redis/host 值 + 绑定 db/host → 发布
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "host".into(),
                value: Value::String("127.0.0.1".into()),
            }],
            deletes: vec![],
            shared_bindings: vec![SharedBinding {
                group: "db".into(),
                key: "host".into(),
                shared_key: "db_host".into(),
            }],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        15,
    )
    .unwrap();
    s.apply(
        &Command::Publish {
            project: pid.clone(),
            branch: dev.clone(),
            comment: "v1".into(),
            request_id: "r1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        16,
    )
    .unwrap();
    // 从 dev 创建 staging：绑定继承、shared 项无本地草稿值
    s.apply(
        &Command::BranchCreate {
            project: pid.clone(),
            name: staging.clone(),
            source: Some(dev.clone()),
            operator: String::new(),
            ts: 0,
        },
        17,
    )
    .unwrap();
    let st = s.get_branch_state(&pid, &staging).unwrap().unwrap();
    assert_eq!(st.shared_bindings["db"]["host"], "db_host", "绑定继承");
    assert!(
        !st.value_draft
            .get("db")
            .is_some_and(|m| m.contains_key("host")),
        "shared 项物化值不复制为本地草稿"
    );
    assert!(
        st.value_draft["redis"].contains_key("host"),
        "本地值正常复制"
    );
}

// ---------------- 会话（I7 单管理员） ----------------

#[test]
fn session_login_logout_heartbeat() {
    let mut s = sm();
    let token = "tok-abc123";
    let hash = dsh_core::token_hash(token);
    // 登录成功 → 会话入库（只存哈希）
    s.apply(
        &Command::SessionLogin {
            token_hash: hash.clone(),
            issued_at: 1000,
            expires_at: Some(1000 + 86_400_000),
        },
        1,
    )
    .unwrap();
    let sess = s.get_session().unwrap().expect("session exists");
    assert_eq!(sess.token_hash, hash);
    assert_ne!(sess.token_hash, token); // 明文不落库
    assert_eq!(sess.expires_at, Some(1000 + 86_400_000));

    // 二次登录 → ERR_SESSION_IN_USE
    let err = s
        .apply(
            &Command::SessionLogin {
                token_hash: dsh_core::token_hash("tok-other"),
                issued_at: 2000,
                expires_at: None,
            },
            2,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::SessionInUse);

    // 心跳续期
    s.apply(
        &Command::SessionHeartbeat {
            expires_at: Some(3000),
        },
        3,
    )
    .unwrap();
    assert_eq!(s.get_session().unwrap().unwrap().expires_at, Some(3000));

    // 登出 → 会话清除；重复登出幂等
    s.apply(&Command::SessionLogout, 4).unwrap();
    assert!(s.get_session().unwrap().is_none());
    assert!(s.apply(&Command::SessionLogout, 5).is_ok());
}

#[test]
fn session_heartbeat_without_login_expired() {
    let mut s = sm();
    let err = s
        .apply(&Command::SessionHeartbeat { expires_at: None }, 1)
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::SessionExpired);
}

#[test]
fn session_token_hash_is_deterministic_and_distinct() {
    assert_eq!(dsh_core::token_hash("abc"), dsh_core::token_hash("abc"));
    assert_ne!(dsh_core::token_hash("abc"), dsh_core::token_hash("abd"));
    assert_eq!(dsh_core::token_hash("abc").len(), 64); // SHA-256 hex
}

// ---------------- 审计（B4：落库 audit/{seq}） ----------------

fn audit_cmd(action: &str, ts: i64) -> Command {
    Command::AuditAppend {
        entry: AuditEntry {
            seq: 0, // 状态机分配
            ts,
            operator: "admin".into(),
            action: action.into(),
            project: Some("order-service".into()),
            branch: Some("dev".into()),
            version: Some(3),
            request_id: Some("r-1".into()),
            detail: serde_json::json!({ "n": 1 }),
        },
    }
}

#[test]
fn audit_append_seq_monotonic_and_queryable() {
    let mut s = sm();
    s.apply(&audit_cmd("publish", 1000), 1).unwrap();
    s.apply(&audit_cmd("rollback", 2000), 2).unwrap();
    s.apply(&audit_cmd("publish", 3000), 3).unwrap();

    // 全量（新 → 旧）
    let all = s.get_audit(None, None, None, 100).unwrap();
    assert_eq!(all.len(), 3);
    assert_eq!(all[0].seq, 3);
    assert_eq!(all[0].action, "publish");
    assert_eq!(all[2].seq, 1);

    // action 过滤
    let pubs = s.get_audit(Some("publish"), None, None, 100).unwrap();
    assert_eq!(pubs.len(), 2);
    assert!(pubs.iter().all(|e| e.action == "publish"));

    // since 过滤（ts ≥ since）
    let recent = s.get_audit(None, None, Some(1500), 100).unwrap();
    assert_eq!(recent.len(), 2);

    // limit 截断
    let limited = s.get_audit(None, None, None, 2).unwrap();
    assert_eq!(limited.len(), 2);
    assert_eq!(limited[0].seq, 3);
}

#[test]
fn audit_persists_and_prunes() {
    let mut s = sm();
    for i in 0..5 {
        s.apply(&audit_cmd("publish", 1000 + i), 10 + i).unwrap();
    }
    // 保留最近 2 条
    let removed = s.prune_audit(2).unwrap();
    assert_eq!(removed, 3);
    let all = s.get_audit(None, None, None, 100).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].seq, 5);
    assert_eq!(all[1].seq, 4);
    // 再剪一次：已达标不再删
    assert_eq!(s.prune_audit(2).unwrap(), 0);
}

#[test]
fn audit_seq_counter_survives_restore() {
    // 模拟快照 dump/restore：seq 计数键随状态导出，恢复后继续递增
    let mut s = sm();
    s.apply(&audit_cmd("login", 1), 1).unwrap();
    let pairs = s.dump_all().unwrap();
    let mut s2 = StateMachine::new(Box::new(InMemoryStore::new()));
    s2.restore_all(&pairs).unwrap();
    s2.apply(&audit_cmd("logout", 2), 2).unwrap();
    let all = s2.get_audit(None, None, None, 100).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].seq, 2);
}

// ---------------- B6：DEK 重包（rewrap_deks） ----------------

fn fake_ct(dek_v: u64) -> Value {
    Value::Secret(Ciphertext {
        enc: "aes-256-gcm".into(),
        v: 1,
        dek_v,
        nonce: "n".into(),
        ct: "c".into(),
        edek: "e".into(),
        edek_nonce: "en".into(),
    })
}

#[test]
fn rewrap_deks_rewrites_snapshot_shared_and_draft_secrets() {
    let mut s = sm();
    let (pid, _) = setup(&mut s); // redis 组含 password(secret)
                                  // 共享项（secret 值，代际 1）
    s.apply(
        &Command::SharedDraftUpdate {
            item: SharedItem {
                key: "token".into(),
                ty: ValueType::Secret,
                secret: true,
                required: false,
                value: fake_ct(1),
                version: 0,
                description: None,
            },

            operator: String::new(),
        },
        60,
    )
    .unwrap();
    s.apply(
        &Command::SharedPublish {
            comment: "c".into(),
            request_id: "rw1".into(),

            operator: String::new(),
            ts: 0,
            cascade: SharedCascadeMode::Auto,
            policy: PublishPolicy::Block,
        },
        61,
    )
    .unwrap();
    // 分支发布 secret（快照，代际 1）
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: BranchName("dev".into()),
            updates: vec![
                DraftUpdateItem {
                    group: "redis".into(),
                    key: "host".into(),
                    value: Value::String("h".into()),
                },
                DraftUpdateItem {
                    group: "redis".into(),
                    key: "password".into(),
                    value: fake_ct(1),
                },
            ],
            deletes: vec![],
            shared_bindings: vec![],

            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        62,
    )
    .unwrap();
    s.apply(
        &Command::Publish {
            project: pid.clone(),
            branch: BranchName("dev".into()),
            comment: "c".into(),
            request_id: "rw2".into(),

            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        63,
    )
    .unwrap();

    // 重包：代际 < 2 的密文重写为代际 2（模拟轮换后 edek 换新 KEK）
    let count = s
        .rewrap_deks(&|ct| {
            if ct.dek_v >= 2 {
                None
            } else {
                let mut n = ct.clone();
                n.dek_v = 2;
                Some(Ok(n))
            }
        })
        .unwrap();
    assert!(
        count >= 2,
        "快照 + 共享项中的 secret 均被重包，实际 {count}"
    );

    let cfg = s.get_config(&pid, &BranchName("dev".into()), 0).unwrap();
    match cfg.groups.get("redis").unwrap().get("password").unwrap() {
        Value::Secret(ct2) => assert_eq!(ct2.dek_v, 2, "快照 secret 已重包"),
        _ => panic!("expected secret"),
    }
    let rows = s.dump_all().unwrap();
    let sh_row = rows
        .iter()
        .find(|(k, _)| String::from_utf8_lossy(k) == "sh/token")
        .expect("shared item row");
    let shared: SharedItem = serde_json::from_slice(&sh_row.1).unwrap();
    match shared.value {
        Value::Secret(ct2) => assert_eq!(ct2.dek_v, 2, "共享 secret 已重包"),
        _ => panic!("expected secret"),
    }
    // 草稿中的 secret（尚未发布）也重包
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: BranchName("test".into()),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "password".into(),
                value: fake_ct(1),
            }],
            deletes: vec![],
            shared_bindings: vec![],

            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        64,
    )
    .unwrap();
    let count3 = s
        .rewrap_deks(&|ct| {
            if ct.dek_v >= 2 {
                None
            } else {
                let mut n = ct.clone();
                n.dek_v = 2;
                Some(Ok(n))
            }
        })
        .unwrap();
    assert!(count3 >= 1, "草稿 secret 也被重包，实际 {count3}");
    // 幂等：全部已最新 → 0
    let count2 = s
        .rewrap_deks(&|ct| {
            if ct.dek_v >= 2 {
                None
            } else {
                Some(Ok(ct.clone()))
            }
        })
        .unwrap();
    assert_eq!(count2, 0);
}

// ---------------- P3：限额（LIM-001） + 管理员改密 ----------------

#[test]
fn shared_item_over_limit_rejected() {
    let mut s = sm();
    let big = "x".repeat(dsh_core::limits::MAX_VALUE_BYTES + 1);
    let item = dsh_core::model::SharedItem {
        key: "k".into(),
        ty: dsh_core::model::ValueType::String,
        secret: false,
        required: false,
        value: dsh_core::model::Value::String(big),
        version: 0,
        description: None,
    };
    let err = s
        .apply(
            &dsh_core::command::Command::SharedDraftUpdate {
                item,
                operator: String::new(),
            },
            1,
        )
        .unwrap_err();
    assert_eq!(
        err.kind,
        dsh_core::ErrorKind::LimitExceeded,
        "超限额应 ERR_LIMIT_EXCEEDED"
    );
}

#[test]
fn admin_set_password_persists_and_reads() {
    let mut s = sm();
    let hash = "sha256-hex-of-password";
    s.apply(
        &dsh_core::command::Command::AdminSetPassword {
            password_hash: hash.into(),
        },
        1,
    )
    .unwrap();
    assert_eq!(s.get_admin_password_hash().unwrap().as_deref(), Some(hash));
    // 未设置时返回 None（回退节点配置）
    let s2 = sm();
    assert_eq!(s2.get_admin_password_hash().unwrap(), None);
}

// ---------------- perf 方案② D3：checkpoint/diff 版本存储 ----------------

/// 发布 N 个版本（每次改 host 值），返回最终项目。
/// 注意：setup 已产生 v1（结构发布）；本函数发布 n 次 → 版本号为 v2..v(n+1)。
fn publish_n_versions(s: &mut StateMachine, n: u64) -> (ProjectId, BranchName) {
    let (pid, _) = setup(s); // 结构 v1
    let b = BranchName("dev".into());
    for i in 0..n {
        s.apply(
            &Command::DraftUpdate {
                project: pid.clone(),
                branch: b.clone(),
                updates: vec![DraftUpdateItem {
                    group: "redis".into(),
                    key: "host".into(),
                    value: Value::String(format!("10.0.0.{}", i + 1)),
                }],
                deletes: vec![],
                shared_bindings: vec![],
                operator: String::new(),
                ts: 0,
                expected_draft_rev: None,
            },
            100 + i as i64,
        )
        .unwrap();
        s.apply(
            &Command::Publish {
                project: pid.clone(),
                branch: b.clone(),
                comment: format!("v{}", i + 1),
                request_id: format!("r{}", i + 1),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            200 + i as i64,
        )
        .unwrap();
    }
    (pid, b)
}

/// T1: checkpoint 布局——v1/v100 full，其余 diff（经 VersionRecord.kind 验证）。
#[test]
fn checkpoint_layout_full_vs_diff() {
    let mut s = sm();
    let (pid, b) = publish_n_versions(&mut s, 104); // 产生 v2..v105（v100 为 checkpoint）
    use dsh_core::model::VersionKind;
    assert_eq!(
        s.get_version_record(&pid, &b, 1).unwrap().unwrap().kind,
        VersionKind::Full,
        "v1 必须 full"
    );
    assert_eq!(
        s.get_version_record(&pid, &b, 100).unwrap().unwrap().kind,
        VersionKind::Full,
        "v100 必须 full（checkpoint）"
    );
    assert_eq!(
        s.get_version_record(&pid, &b, 2).unwrap().unwrap().kind,
        VersionKind::Diff,
        "v2 必须 diff"
    );
    assert_eq!(
        s.get_version_record(&pid, &b, 101).unwrap().unwrap().kind,
        VersionKind::Diff,
        "v101 必须 diff"
    );
    assert_eq!(
        s.snapshot_of(&pid, &b, 100).unwrap()["redis"]["host"],
        Value::String("10.0.0.99".into()),
        "v100 内容=第99次发布"
    );
    assert_eq!(
        s.snapshot_of(&pid, &b, 101).unwrap()["redis"]["host"],
        Value::String("10.0.0.100".into()),
        "v101 内容=第100次发布"
    );
}

/// T2: 任意版本快照重建正确（与活动版本 diff 一致）。
#[test]
fn snapshot_rebuild_any_version() {
    let mut s = sm();
    let (pid, b) = publish_n_versions(&mut s, 105); // v2..v106
                                                    // 抽查：v 的内容 = 第 (v-1) 次发布的值（10.0.0.{v-1}）
    for v in [2u64, 50, 100, 101, 105, 106] {
        let snap = s.snapshot_of(&pid, &b, v).unwrap();
        let host = &snap["redis"]["host"];
        assert_eq!(
            host,
            &Value::String(format!("10.0.0.{}", v - 1)),
            "v{v} 重建错误"
        );
    }
    let cfg = s.get_config(&pid, &b, 0).unwrap();
    assert_eq!(cfg.version, 106);
    assert_eq!(
        cfg.groups["redis"]["host"],
        Value::String("10.0.0.105".into())
    );
}

/// T5: 裁剪保留 checkpoint 基座，diff 链可重建。
#[test]
fn prune_keeps_checkpoint_base() {
    let mut s = sm();
    let (pid, b) = publish_n_versions(&mut s, 249); // v2..v250（v200 为 checkpoint）
    let removed = s.prune_versions(&pid, &b, 10).unwrap();
    assert!(removed > 0);
    assert!(
        s.get_version_record(&pid, &b, 200).unwrap().is_some(),
        "v200 checkpoint 必须保留为基座"
    );
    assert!(
        s.get_version_record(&pid, &b, 199).unwrap().is_none(),
        "v199 已裁剪"
    );
    let snap = s.snapshot_of(&pid, &b, 250).unwrap();
    assert_eq!(
        snap["redis"]["host"],
        Value::String("10.0.0.249".into()),
        "v250 重建正确"
    );
}

/// T6: DEK 重包覆盖 diff 中的 secret 密文。
#[test]
fn rewrap_deks_covers_diff_secrets() {
    let mut s = sm();
    let (pid, _) = setup(&mut s);
    let b = BranchName("dev".into());
    // 先设必填 host（结构校验），再发布 secret（v2，非 checkpoint → 存 diff）
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: b.clone(),
            updates: vec![DraftUpdateItem {
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
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: b.clone(),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "password".into(),
                value: Value::Secret(dsh_core::model::Ciphertext {
                    enc: "aes-256-gcm".into(),
                    v: 1,
                    dek_v: 1,
                    nonce: "n".into(),
                    ct: "ct-old".into(),
                    edek: "edek-old".into(),
                    edek_nonce: "en".into(),
                }),
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
    s.apply(
        &Command::Publish {
            project: pid.clone(),
            branch: b.clone(),
            comment: "secret v".into(),
            request_id: "rs1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        7,
    )
    .unwrap();
    // 重包：把所有 ct=ct-old 替换为 ct-new
    let count = s
        .rewrap_deks(&|ct| {
            if ct.ct == "ct-old" {
                let mut n = ct.clone();
                n.ct = "ct-new".into();
                Some(Ok(n))
            } else {
                None
            }
        })
        .unwrap();
    assert!(count >= 1, "diff 中 secret 应被重包");
    // 重建 v2（首个值版本），密文应为新值
    let snap = s.snapshot_of(&pid, &b, 2).unwrap();
    match &snap["redis"]["password"] {
        Value::Secret(ct) => assert_eq!(ct.ct, "ct-new", "重包后密文应更新"),
        other => panic!("应为 Secret: {other:?}"),
    }
}

// ---------------- 多会话（multisession 改造，纯新增变体） ----------------

/// T1: 多会话并存——同账号连续登录多次均成功，多个 key 各自独立。
#[test]
fn multi_session_coexists() {
    let mut s = sm();

    // 管理员多会话
    for i in 0..3 {
        let sid = format!("sid{i}");
        s.apply(
            &Command::MultiSessionLogin {
                token_hash: format!("hash{i}"),
                issued_at: 100 + i,
                expires_at: None,
                session_id: sid.clone(),
            },
            10 + i,
        )
        .unwrap();
        assert!(
            s.get_session_with(&sid).unwrap().is_some(),
            "sess/admin/{sid} 应存在"
        );
    }
    assert_eq!(
        s.list_admin_sessions().unwrap().len(),
        3,
        "3 个管理员会话并存"
    );
    // PA 多会话（先建项目）
    s.apply(
        &Command::ProjectCreate {
            name: "p".into(),
            operator: String::new(),
            ts: 0,
        },
        19,
    )
    .unwrap();
    s.apply(
        &Command::ProjectAdminCreate {
            project: "p".into(),
            username: "alice".into(),
            salt: "salt".into(),
            password_hash: "ph".into(),
            ts: 0,
        },
        20,
    )
    .unwrap();
    for i in 0..2 {
        let sid = format!("pasid{i}");
        s.apply(
            &Command::MultiPaSessionLogin {
                username: "alice".into(),
                token_hash: format!("phash{i}"),
                issued_at: 200 + i,
                expires_at: None,
                device_id: "cli".into(),
                session_id: sid.clone(),
            },
            30 + i,
        )
        .unwrap();
        assert!(
            s.get_pa_session_with("alice", &sid).unwrap().is_some(),
            "sess/pa/alice/{sid} 应存在"
        );
    }
}

/// T2: 每会话独立登出——删一个不影响另一个。
#[test]
fn multi_session_independent_logout() {
    let mut s = sm();
    for i in 0..2 {
        s.apply(
            &Command::MultiSessionLogin {
                token_hash: format!("h{i}"),
                issued_at: 100 + i,
                expires_at: None,
                session_id: format!("s{i}"),
            },
            10 + i,
        )
        .unwrap();
    }
    s.apply(
        &Command::MultiSessionLogout {
            session_id: "s0".into(),
        },
        20,
    )
    .unwrap();
    assert!(s.get_session_with("s0").unwrap().is_none(), "s0 已登出");
    assert!(s.get_session_with("s1").unwrap().is_some(), "s1 不受影响");
}

/// T3: 每会话独立心跳——仅续期指定会话。
#[test]
fn multi_session_independent_heartbeat() {
    let mut s = sm();
    s.apply(
        &Command::MultiSessionLogin {
            token_hash: "h0".into(),
            issued_at: 100,
            expires_at: None,
            session_id: "s0".into(),
        },
        10,
    )
    .unwrap();
    s.apply(
        &Command::MultiSessionLogin {
            token_hash: "h1".into(),
            issued_at: 100,
            expires_at: None,
            session_id: "s1".into(),
        },
        11,
    )
    .unwrap();
    s.apply(
        &Command::MultiSessionHeartbeat {
            session_id: "s0".into(),
            expires_at: Some(9999),
        },
        12,
    )
    .unwrap();
    assert_eq!(
        s.get_session_with("s0").unwrap().unwrap().expires_at,
        Some(9999),
        "s0 已续期"
    );
    assert_eq!(
        s.get_session_with("s1").unwrap().unwrap().expires_at,
        None,
        "s1 未受影响"
    );
    // 不存在会话心跳 → SessionExpired
    let e = s
        .apply(
            &Command::MultiSessionHeartbeat {
                session_id: "ghost".into(),
                expires_at: Some(1),
            },
            13,
        )
        .unwrap_err();
    assert_eq!(e.kind, dsh_core::ErrorKind::SessionExpired);
}

/// T4/T5: force-logout 单个与批量。
#[test]
fn multi_session_force_logout_single_and_all() {
    let mut s = sm();
    for i in 0..3 {
        s.apply(
            &Command::MultiSessionLogin {
                token_hash: format!("h{i}"),
                issued_at: 100 + i,
                expires_at: None,
                session_id: format!("s{i}"),
            },
            10 + i,
        )
        .unwrap();
    }
    // 单个踢
    s.apply(
        &Command::MultiSessionLogout {
            session_id: "s1".into(),
        },
        20,
    )
    .unwrap();
    assert!(s.get_session_with("s1").unwrap().is_none());
    assert!(s.get_session_with("s0").unwrap().is_some());
    // 批量踢全部
    s.apply(&Command::MultiSessionLogoutAll, 21).unwrap();
    assert_eq!(s.list_admin_sessions().unwrap().len(), 0, "全部会话已清");
    // 旧格式单 key 也被批量清（兼容）
    s.apply(
        &Command::SessionLogin {
            token_hash: "old".into(),
            issued_at: 1,
            expires_at: None,
        },
        22,
    )
    .unwrap();
    s.apply(&Command::MultiSessionLogoutAll, 23).unwrap();
    assert!(s.get_session().unwrap().is_none(), "旧格式会话也被清");
}

/// T6: 旧格式兼容——无 sid 的旧命令/旧日志走单会话语义。
#[test]
fn multi_session_legacy_compat() {
    let mut s = sm();
    // 旧 SessionLogin（无 sid）→ 单会话：第二次 409
    s.apply(
        &Command::SessionLogin {
            token_hash: "a".into(),
            issued_at: 1,
            expires_at: None,
        },
        1,
    )
    .unwrap();
    let e = s
        .apply(
            &Command::SessionLogin {
                token_hash: "b".into(),
                issued_at: 2,
                expires_at: None,
            },
            2,
        )
        .unwrap_err();
    assert_eq!(e.kind, dsh_core::ErrorKind::SessionInUse, "旧命令仍单会话");
    // 新 MultiSessionLogin 不受旧会话影响（并存）
    s.apply(
        &Command::MultiSessionLogin {
            token_hash: "c".into(),
            issued_at: 3,
            expires_at: None,
            session_id: "n1".into(),
        },
        3,
    )
    .unwrap();
    assert!(s.get_session().unwrap().is_some(), "旧会话仍在");
    assert!(s.get_session_with("n1").unwrap().is_some(), "新会话并存");
}

/// T7: 改密/删号级联清全部会话（旧+新格式双删）。
#[test]
fn multi_session_cascade_clears_all() {
    let mut s = sm();
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
        &Command::ProjectAdminCreate {
            project: "p".into(),
            username: "bob".into(),
            salt: "s".into(),
            password_hash: "h".into(),
            ts: 0,
        },
        1,
    )
    .unwrap();
    // 旧格式 + 新格式两个会话
    s.apply(
        &Command::PaSessionLogin {
            username: "bob".into(),
            token_hash: "old".into(),
            issued_at: 1,
            expires_at: None,
            device_id: "cli".into(),
        },
        2,
    )
    .unwrap();
    s.apply(
        &Command::MultiPaSessionLogin {
            username: "bob".into(),
            token_hash: "new".into(),
            issued_at: 3,
            expires_at: None,
            device_id: "cli".into(),
            session_id: "n1".into(),
        },
        3,
    )
    .unwrap();
    // 删号 → 全部会话清
    s.apply(
        &Command::ProjectAdminDelete {
            username: "bob".into(),
        },
        4,
    )
    .unwrap();
    assert!(s.get_pa_session("bob").unwrap().is_none(), "旧格式会话已清");
    assert!(
        s.get_pa_session_with("bob", "n1").unwrap().is_none(),
        "新格式会话已清"
    );
}

// ---------------- 草稿乐观锁（并发编辑冲突检测） ----------------

/// 乐观锁：expected_draft_rev 不匹配 → Conflict；匹配 → 保存并推进 rev。
#[test]
fn draft_optimistic_lock_conflict_detection() {
    let mut s = sm();
    let (pid, _) = setup(&mut s); // 结构 v1
    let b = BranchName("dev".into());
    let upd = |v: &str| {
        vec![DraftUpdateItem {
            group: "redis".into(),
            key: "host".into(),
            value: Value::String(v.into()),
        }]
    };
    // A 保存（expected=0 不校验，或首次 rev=0）
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: b.clone(),
            updates: upd("A"),
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: Some(0),
        },
        10,
    )
    .unwrap();
    let st = s.get_branch_state(&pid, &b).unwrap().unwrap();
    assert_eq!(st.draft_rev, 1, "首次保存后 rev=1");

    // A 再保存一次 → rev=2（模拟 A 持续编辑期间 B 读到 rev=1）
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: b.clone(),
            updates: upd("A2"),
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: Some(1),
        },
        11,
    )
    .unwrap();
    // B 基于过期 rev（1）保存 → 冲突 409
    let e = s
        .apply(
            &Command::DraftUpdate {
                project: pid.clone(),
                branch: b.clone(),
                updates: upd("B"),
                deletes: vec![],
                shared_bindings: vec![],
                operator: String::new(),
                ts: 0,
                expected_draft_rev: Some(1), // 过期（当前 rev=2）
            },
            12,
        )
        .unwrap_err();
    assert_eq!(e.kind, dsh_core::ErrorKind::Conflict, "基于过期 rev 应冲突");

    // B 拉取最新 rev（1）后保存 → 成功，rev=2
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: b.clone(),
            updates: upd("B2"),
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: Some(2), // 最新
        },
        13,
    )
    .unwrap();
    let st2 = s.get_branch_state(&pid, &b).unwrap().unwrap();
    assert_eq!(st2.draft_rev, 3, "三次成功保存后 rev=3");
    // 值 = B2（A 的修改被 B 覆盖——但 B 已基于最新值重改，冲突提示过）
    let snap = s.get_config(&pid, &b, 0).unwrap();
    assert!(snap.groups.is_empty(), "草稿未发布，snapshot 仍空");
}

/// 乐观锁兼容：expected_draft_rev=0（旧客户端）不校验，last-write-wins。
#[test]
fn draft_optimistic_lock_legacy_no_check() {
    let mut s = sm();
    let (pid, _) = setup(&mut s);
    let b = BranchName("dev".into());
    let upd = |v: &str| {
        vec![DraftUpdateItem {
            group: "redis".into(),
            key: "host".into(),
            value: Value::String(v.into()),
        }]
    };
    // 旧客户端（expected=0）：连续保存不校验，直接覆盖
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: b.clone(),
            updates: upd("v1"),
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        10,
    )
    .unwrap();
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: b.clone(),
            updates: upd("v2"),
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        11,
    )
    .unwrap();
    let st = s.get_branch_state(&pid, &b).unwrap().unwrap();
    assert_eq!(st.draft_rev, 2, "rev 仍推进（供新客户端锚定）");
    // 草稿值 = v2
    let g = st.value_draft.get("redis").unwrap();
    assert_eq!(
        g.get("host").unwrap().value,
        Value::String("v2".into()),
        "last-write-wins（旧客户端不校验）"
    );
}

// ==================== G2 灰度发布（T1-T8，design/gray-release.md） ====================

fn label_rule(key: &str, value: &str) -> GrayRule {
    GrayRule {
        match_labels: vec![LabelSelector {
            key: key.into(),
            value: value.into(),
        }],
        ip_cidrs: vec![],
        percentage: None,
    }
}

fn ctx(instance: &str, labels: &[(&str, &str)], ip: Option<&str>) -> ClientCtx {
    ClientCtx {
        instance_id: instance.into(),
        labels: labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        ip: ip.map(|s| s.parse().unwrap()),
    }
}

/// 灰度测试基线：项目 + 结构（v2）+ dev 稳定版 v2（host=stable-host）+ 新草稿（host=gray-host）。
fn gray_setup(s: &mut StateMachine) -> (ProjectId, BranchName) {
    let (pid, _branches) = setup(s);
    let dev = BranchName("dev".into());
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "host".into(),
                value: Value::String("stable-host".into()),
            }],
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        10,
    )
    .unwrap();
    s.apply(
        &Command::Publish {
            project: pid.clone(),
            branch: dev.clone(),
            comment: "stable v2".into(),
            request_id: "p1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        11,
    )
    .unwrap();
    // 下一版草稿（将作为灰度内容）
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "host".into(),
                value: Value::String("gray-host".into()),
            }],
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        12,
    )
    .unwrap();
    (pid, dev)
}

/// T1 灰度发布：稳定版不动、灰度快照落地、事件 gray=true、草稿清空。
#[test]
fn gray_publish_creates_gray_snapshot() {
    let mut s = sm();
    let (pid, dev) = gray_setup(&mut s);
    let events = s
        .apply(
            &Command::GrayPublish {
                project: pid.clone(),
                branch: dev.clone(),
                rule: label_rule("zone", "cn-north-1"),
                comment: "先给华北".into(),
                request_id: "g1".into(),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            13,
        )
        .unwrap();
    assert_eq!(events.len(), 1);
    assert!(events[0].gray, "灰度事件 gray=true");
    assert_eq!(events[0].version, 2, "稳定版 active_version 未动");
    assert_eq!(
        events[0].ty,
        EventType::ValuePublish,
        "复用既有 EventType（Q3）"
    );

    let st = s.get_branch_state(&pid, &dev).unwrap().unwrap();
    assert_eq!(st.active_version, 2);
    assert_eq!(st.gray_seq, 1, "独立灰度序号 +1");
    assert!(st.gray_rule.is_some());
    assert!(st.value_draft.is_empty(), "草稿已物化清空");

    // 灰度快照内容 = 草稿物化
    let snap = s.gray_snapshot_of(&pid, &dev, 1).unwrap();
    assert_eq!(snap["redis"]["host"], Value::String("gray-host".into()));

    // 稳定客户端不受影响
    let cfg = s.get_config(&pid, &dev, 0).unwrap();
    assert_eq!(cfg.version, 2);
    assert_eq!(
        cfg.groups["redis"]["host"],
        Value::String("stable-host".into())
    );

    // 解析：命中 → gray_seq；未命中 → active
    assert_eq!(
        s.resolve_version(&pid, &dev, &ctx("web-1", &[("zone", "cn-north-1")], None))
            .unwrap(),
        ResolvedVersion::Gray(1),
        "华北命中 → 灰度快照"
    );
    assert_eq!(
        s.resolve_version(&pid, &dev, &ctx("web-2", &[("zone", "cn-south-1")], None))
            .unwrap(),
        ResolvedVersion::Stable(2),
        "华南未命中 → 稳定版"
    );
}

/// T2 解析三路（labels → IP → percent 固定次序；任一命中）+ 纯函数单元断言。
#[test]
fn gray_resolve_three_paths() {
    let mut s = sm();
    let (pid, dev) = gray_setup(&mut s);
    let rule = GrayRule {
        match_labels: vec![LabelSelector {
            key: "zone".into(),
            value: "cn-north-1".into(),
        }],
        ip_cidrs: vec!["10.0.0.0/8".into()],
        percentage: Some(50),
    };
    s.apply(
        &Command::GrayPublish {
            project: pid.clone(),
            branch: dev.clone(),
            rule: rule.clone(),
            comment: "三路".into(),
            request_id: "g2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();

    // ① labels 命中
    assert!(StateMachine::rule_matches(
        &rule,
        &ctx("web-1", &[("zone", "cn-north-1")], Some("8.8.8.8"))
    ));
    // ② IP 命中（无标签）
    assert!(StateMachine::rule_matches(
        &rule,
        &ctx("web-2", &[], Some("10.1.2.3"))
    ));
    assert!(!StateMachine::rule_matches(
        &rule,
        &ctx("web-2", &[], Some("192.168.1.1"))
    ));
    // ③ percentage 命中（无标签无 IP；用动态阈值保证确定性）
    let h = StateMachine::fnv1a_hash("web-pct");
    let hit_pct = h % 100 + 1; // 恒 < hit_pct
    let miss_pct = h % 100; // 恒 >= miss_pct
    let pct_rule = GrayRule {
        match_labels: vec![],
        ip_cidrs: vec![],
        percentage: Some(hit_pct),
    };
    assert!(StateMachine::rule_matches(
        &pct_rule,
        &ctx("web-pct", &[], None)
    ));
    let miss_rule = GrayRule {
        percentage: Some(miss_pct),
        ..Default::default()
    };
    assert!(!StateMachine::rule_matches(
        &miss_rule,
        &ctx("web-pct", &[], None)
    ));

    // 纯函数确定性：同一输入恒同输出
    assert_eq!(
        StateMachine::fnv1a_hash("web-1"),
        StateMachine::fnv1a_hash("web-1")
    );

    // 非法 CIDR 防御：不 panic、不命中
    let bad_rule = GrayRule {
        match_labels: vec![],
        ip_cidrs: vec!["not-a-cidr".into()],
        percentage: None,
    };
    assert!(!StateMachine::rule_matches(
        &bad_rule,
        &ctx("web-1", &[], Some("10.1.2.3"))
    ));

    // resolve 三路端到端
    assert_eq!(
        s.resolve_version(&pid, &dev, &ctx("web-1", &[("zone", "cn-north-1")], None))
            .unwrap(),
        ResolvedVersion::Gray(1)
    );
    assert_eq!(
        s.resolve_version(&pid, &dev, &ctx("web-2", &[], Some("10.1.2.3")))
            .unwrap(),
        ResolvedVersion::Gray(1)
    );
    assert_eq!(
        s.resolve_version(&pid, &dev, &ctx("web-3", &[], Some("8.8.8.8")))
            .unwrap(),
        ResolvedVersion::Stable(2)
    );
}

/// T3 灰度转正：灰度内容 → 新 active（next=max(active,gray)+1），清灰度，事件携带新版本号。
#[test]
fn gray_promote_makes_gray_active() {
    let mut s = sm();
    let (pid, dev) = gray_setup(&mut s);
    s.apply(
        &Command::GrayPublish {
            project: pid.clone(),
            branch: dev.clone(),
            rule: label_rule("zone", "cn-north-1"),
            comment: "g".into(),
            request_id: "g1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();

    let events = s
        .apply(
            &Command::GrayPromote {
                project: pid.clone(),
                branch: dev.clone(),
                comment: "全量".into(),
                request_id: "prom1".into(),
                operator: String::new(),
                ts: 0,
            },
            14,
        )
        .unwrap();
    assert_eq!(events.len(), 1);
    assert!(events[0].gray, "转正事件 gray=true（Q4 补发语义）");
    assert_eq!(events[0].version, 3, "next = max(2, 1)+1");

    let st = s.get_branch_state(&pid, &dev).unwrap().unwrap();
    assert_eq!(st.active_version, 3);
    assert_eq!(st.gray_seq, 0, "灰度清空");
    assert!(st.gray_rule.is_none());

    // 全量客户端现在读到灰度内容
    let cfg = s.get_config(&pid, &dev, 0).unwrap();
    assert_eq!(cfg.version, 3);
    assert_eq!(
        cfg.groups["redis"]["host"],
        Value::String("gray-host".into())
    );
    // 任何身份解析都回稳定版
    assert_eq!(
        s.resolve_version(&pid, &dev, &ctx("web-1", &[("zone", "cn-north-1")], None))
            .unwrap(),
        ResolvedVersion::Stable(3)
    );
    // 版本记录带 gray 标记（重放保真，Q3）
    let recs = s.version_history(&pid, &dev).unwrap();
    let promoted = recs.iter().find(|r| r.no == 3).unwrap();
    assert!(promoted.gray, "转正版本记录 gray=true");
    assert_eq!(promoted.event_ty, Some(EventType::ValuePublish));
}

/// T4 灰度下量：清灰度、事件携带回落版本号、灰度快照历史可查（Q5）。
#[test]
fn gray_abort_reverts_to_stable() {
    let mut s = sm();
    let (pid, dev) = gray_setup(&mut s);
    s.apply(
        &Command::GrayPublish {
            project: pid.clone(),
            branch: dev.clone(),
            rule: label_rule("zone", "cn-north-1"),
            comment: "g".into(),
            request_id: "g1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();

    let events = s
        .apply(
            &Command::GrayAbort {
                project: pid.clone(),
                branch: dev.clone(),
                comment: "撤回".into(),
                request_id: "ab1".into(),
                operator: String::new(),
                ts: 0,
            },
            14,
        )
        .unwrap();
    assert_eq!(events.len(), 1);
    assert!(events[0].gray, "下量事件 gray=true（Q4 携带回落版本号）");
    assert_eq!(events[0].version, 2, "回落版本号 = active_version");
    assert!(events[0].changes.is_empty(), "下量不产生新版本");

    let st = s.get_branch_state(&pid, &dev).unwrap().unwrap();
    assert_eq!(st.active_version, 2);
    assert_eq!(st.gray_seq, 0);
    assert!(st.gray_rule.is_none());

    // 灰度客户端也回落到稳定版
    assert_eq!(
        s.resolve_version(&pid, &dev, &ctx("web-1", &[("zone", "cn-north-1")], None))
            .unwrap(),
        ResolvedVersion::Stable(2)
    );
    let cfg = s.get_config(&pid, &dev, 0).unwrap();
    assert_eq!(
        cfg.groups["redis"]["host"],
        Value::String("stable-host".into())
    );
    // 回收：下量后灰度快照已删除（「仅存当前灰度，非历史链」语义）
    assert!(s.gray_snapshot_of(&pid, &dev, 1).is_err());
}

/// T5 I10 幂等：三个灰度命令同 request_id 重放 → 空事件、状态不重复推进。
#[test]
fn gray_commands_idempotent() {
    let mut s = sm();
    let (pid, dev) = gray_setup(&mut s);
    // publish 幂等
    s.apply(
        &Command::GrayPublish {
            project: pid.clone(),
            branch: dev.clone(),
            rule: label_rule("zone", "cn-north-1"),
            comment: "g".into(),
            request_id: "g1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    let replay = s
        .apply(
            &Command::GrayPublish {
                project: pid.clone(),
                branch: dev.clone(),
                rule: label_rule("zone", "cn-north-1"),
                comment: "g".into(),
                request_id: "g1".into(),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            14,
        )
        .unwrap();
    assert!(replay.is_empty(), "重放不产生事件");
    let st = s.get_branch_state(&pid, &dev).unwrap().unwrap();
    assert_eq!(st.gray_seq, 1, "gray_seq 不重复递增");

    // promote 幂等
    s.apply(
        &Command::GrayPromote {
            project: pid.clone(),
            branch: dev.clone(),
            comment: "p".into(),
            request_id: "prom1".into(),
            operator: String::new(),
            ts: 0,
        },
        15,
    )
    .unwrap();
    let replay = s
        .apply(
            &Command::GrayPromote {
                project: pid.clone(),
                branch: dev.clone(),
                comment: "p".into(),
                request_id: "prom1".into(),
                operator: String::new(),
                ts: 0,
            },
            16,
        )
        .unwrap();
    assert!(replay.is_empty());
    let st = s.get_branch_state(&pid, &dev).unwrap().unwrap();
    assert_eq!(st.active_version, 3, "active 不重复推进");

    // abort 幂等：promote 已清灰度 → 先再编辑草稿并灰度（g2），再 abort 并重放
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "host".into(),
                value: Value::String("gray-2".into()),
            }],
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        17,
    )
    .unwrap();
    s.apply(
        &Command::GrayPublish {
            project: pid.clone(),
            branch: dev.clone(),
            rule: label_rule("zone", "cn-north-1"),
            comment: "g2".into(),
            request_id: "g2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        18,
    )
    .unwrap();
    s.apply(
        &Command::GrayAbort {
            project: pid.clone(),
            branch: dev.clone(),
            comment: "a".into(),
            request_id: "ab1".into(),
            operator: String::new(),
            ts: 0,
        },
        19,
    )
    .unwrap();
    let replay = s
        .apply(
            &Command::GrayAbort {
                project: pid.clone(),
                branch: dev.clone(),
                comment: "a".into(),
                request_id: "ab1".into(),
                operator: String::new(),
                ts: 0,
            },
            20,
        )
        .unwrap();
    assert!(replay.is_empty());
    let st = s.get_branch_state(&pid, &dev).unwrap().unwrap();
    assert_eq!(st.gray_seq, 0, "abort 幂等重放不重复推进");
}

/// T5b 错误路径：空草稿发布 / 空规则 / 无灰度 promote / 无灰度 abort。
#[test]
fn gray_command_error_paths() {
    let mut s = sm();
    let (pid, _branches) = setup(&mut s);
    let dev = BranchName("dev".into());
    // 空草稿 → NoDraft
    let e = s
        .apply(
            &Command::GrayPublish {
                project: pid.clone(),
                branch: dev.clone(),
                rule: label_rule("zone", "cn-north-1"),
                comment: "x".into(),
                request_id: "g1".into(),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            10,
        )
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::NoDraft);
    // 空规则 → Validation
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![DraftUpdateItem {
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
        11,
    )
    .unwrap();
    let e = s
        .apply(
            &Command::GrayPublish {
                project: pid.clone(),
                branch: dev.clone(),
                rule: GrayRule::default(),
                comment: "x".into(),
                request_id: "g2".into(),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            12,
        )
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Validation, "空规则拒绝");
    // 百分比 > 100 → Validation
    let e = s
        .apply(
            &Command::GrayPublish {
                project: pid.clone(),
                branch: dev.clone(),
                rule: GrayRule {
                    percentage: Some(101),
                    ..Default::default()
                },
                comment: "x".into(),
                request_id: "g3".into(),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            13,
        )
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Validation, "百分比 >100 拒绝");
    // 非法 CIDR → Validation
    let e = s
        .apply(
            &Command::GrayPublish {
                project: pid.clone(),
                branch: dev.clone(),
                rule: GrayRule {
                    ip_cidrs: vec!["nope".into()],
                    ..Default::default()
                },
                comment: "x".into(),
                request_id: "g4".into(),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            14,
        )
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Validation, "非法 CIDR 拒绝");
    // 无灰度 promote → Validation
    let e = s
        .apply(
            &Command::GrayPromote {
                project: pid.clone(),
                branch: dev.clone(),
                comment: "x".into(),
                request_id: "pr1".into(),
                operator: String::new(),
                ts: 0,
            },
            15,
        )
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Validation);
    // 无灰度 abort → Validation
    let e = s
        .apply(
            &Command::GrayAbort {
                project: pid.clone(),
                branch: dev.clone(),
                comment: "x".into(),
                request_id: "ab1".into(),
                operator: String::new(),
                ts: 0,
            },
            16,
        )
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Validation);
}

/// T6 结构发布 × 灰度（D23/Q1）：灰度活跃时一次分配两个不同号，灰度快照同步 bump 不失效。
#[test]
fn structure_publish_with_active_gray_bumps_both() {
    let mut s = sm();
    let (pid, dev) = gray_setup(&mut s);
    s.apply(
        &Command::GrayPublish {
            project: pid.clone(),
            branch: dev.clone(),
            rule: label_rule("zone", "cn-north-1"),
            comment: "g".into(),
            request_id: "g1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    // 结构草稿（当前结构版本 2 → base_version 2）+ 发布
    let structure = s.get_structure(&pid).unwrap().unwrap();
    assert_eq!(structure.version, 2);
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: structure.version,
            groups: structure.groups.clone(),
            operator: String::new(),
        },
        14,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "struct v3".into(),
            request_id: "s3".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        15,
    )
    .unwrap();

    let st = s.get_branch_state(&pid, &dev).unwrap().unwrap();
    // Q1：stable_next = max(2,1)+1 = 3；gray_next = 3+1 = 4（两个不同号）
    assert_eq!(st.active_version, 3);
    assert_eq!(st.gray_seq, 4, "灰度快照同步 bump 分配不同号");
    assert_eq!(st.structure_version, 3, "结构版本已推进");
    // 灰度内容保留（D23 不失效）
    let snap = s.gray_snapshot_of(&pid, &dev, 4).unwrap();
    assert_eq!(snap["redis"]["host"], Value::String("gray-host".into()));
    // 回收：结构发布灰度 bump 后旧灰度快照已删除（仅存当前 gray_seq 快照）
    assert!(s.gray_snapshot_of(&pid, &dev, 1).is_err());
    // 灰度客户端解析到新灰度号；稳定客户端读到新稳定版
    assert_eq!(
        s.resolve_version(&pid, &dev, &ctx("web-1", &[("zone", "cn-north-1")], None))
            .unwrap(),
        ResolvedVersion::Gray(4)
    );
    assert_eq!(
        s.resolve_version(&pid, &dev, &ctx("web-2", &[], None))
            .unwrap(),
        ResolvedVersion::Stable(3)
    );
    // 无灰度的分支照旧（active = 1+1 = 2）
    let test = BranchName("test".into());
    let tst = s.get_branch_state(&pid, &test).unwrap().unwrap();
    assert_eq!(tst.active_version, 2);
    assert_eq!(tst.gray_seq, 0);
}

/// 回收：gray-snap/ 孤儿快照不累积——重复灰度发布回收旧序号、promote/abort 删除当前快照。
#[test]
fn gray_snapshot_recycled_on_lifecycle() {
    fn gray_key_count(s: &StateMachine) -> usize {
        s.dump_all()
            .unwrap()
            .iter()
            .filter(|(k, _)| String::from_utf8_lossy(k).contains("/gray-snap/"))
            .count()
    }
    let mut s = sm();
    let (pid, dev) = gray_setup(&mut s);

    // 首次灰度发布（gray_seq=1）
    s.apply(
        &Command::GrayPublish {
            project: pid.clone(),
            branch: dev.clone(),
            rule: label_rule("zone", "cn-north-1"),
            comment: "g1".into(),
            request_id: "g1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    assert_eq!(gray_key_count(&s), 1, "首次灰度发布仅 1 个灰度快照");

    // 再编辑草稿并二次灰度发布（gray_seq=2）→ 旧 seq=1 被回收
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "host".into(),
                value: Value::String("gray-2".into()),
            }],
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        14,
    )
    .unwrap();
    s.apply(
        &Command::GrayPublish {
            project: pid.clone(),
            branch: dev.clone(),
            rule: label_rule("zone", "cn-north-1"),
            comment: "g2".into(),
            request_id: "g2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        15,
    )
    .unwrap();
    assert_eq!(gray_key_count(&s), 1, "重复灰度发布回收旧快照");
    assert!(
        s.gray_snapshot_of(&pid, &dev, 1).is_err(),
        "旧灰度快照已删除"
    );
    assert!(s.gray_snapshot_of(&pid, &dev, 2).is_ok());

    // 转正：灰度快照并入 v/ 后删除
    s.apply(
        &Command::GrayPromote {
            project: pid.clone(),
            branch: dev.clone(),
            comment: "prom".into(),
            request_id: "prom1".into(),
            operator: String::new(),
            ts: 0,
        },
        16,
    )
    .unwrap();
    assert_eq!(gray_key_count(&s), 0, "转正后灰度快照删除");

    // 再灰度 + 下量：下量删除当前灰度快照
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "host".into(),
                value: Value::String("gray-3".into()),
            }],
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        17,
    )
    .unwrap();
    s.apply(
        &Command::GrayPublish {
            project: pid.clone(),
            branch: dev.clone(),
            rule: label_rule("zone", "cn-north-1"),
            comment: "g3".into(),
            request_id: "g3".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        18,
    )
    .unwrap();
    assert_eq!(gray_key_count(&s), 1);
    s.apply(
        &Command::GrayAbort {
            project: pid.clone(),
            branch: dev.clone(),
            comment: "ab".into(),
            request_id: "ab1".into(),
            operator: String::new(),
            ts: 0,
        },
        19,
    )
    .unwrap();
    assert_eq!(gray_key_count(&s), 0, "下量后灰度快照删除");
}

/// T7 Q2：无身份（instance_id 空）永不进灰度——即使标签命中。
#[test]
fn gray_resolve_no_identity_never_gray() {
    let mut s = sm();
    let (pid, dev) = gray_setup(&mut s);
    s.apply(
        &Command::GrayPublish {
            project: pid.clone(),
            branch: dev.clone(),
            rule: label_rule("zone", "cn-north-1"),
            comment: "g".into(),
            request_id: "g1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    // 无 instance_id、标签命中 → 仍解析稳定版
    assert_eq!(
        s.resolve_version(
            &pid,
            &dev,
            &ClientCtx {
                instance_id: String::new(),
                labels: [("zone".to_string(), "cn-north-1".to_string())].into(),
                ip: None,
            }
        )
        .unwrap(),
        ResolvedVersion::Stable(2),
        "Q2：无身份永不进灰度"
    );
    // 纯函数防御：空身份 + 100% 放量也不命中（rule_matches 内部守卫）
    let rule = GrayRule {
        percentage: Some(100),
        ..Default::default()
    };
    assert!(!StateMachine::rule_matches(&rule, &ClientCtx::default()));

    // R2（审核）钉死文档化行为：纯 IP 规则 + 无 instance_id → 同样不进灰度
    // （Q2 门闩在 rule_matches 之前；IP 规则实际要求上报 instance_id，design g3-dataplane.md D26）
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "host".into(),
                value: Value::String("ip-gray".into()),
            }],
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        14,
    )
    .unwrap();
    s.apply(
        &Command::GrayPublish {
            project: pid.clone(),
            branch: dev.clone(),
            rule: GrayRule {
                ip_cidrs: vec!["10.0.0.0/8".into()],
                ..Default::default()
            },
            comment: "ip-rule".into(),
            request_id: "g-ip".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        15,
    )
    .unwrap();
    // 空 instance_id + IP 在段内 → 仍 Stable（Q2 门闩优先于 IP 判据）
    assert_eq!(
        s.resolve_version(
            &pid,
            &dev,
            &ClientCtx {
                instance_id: String::new(),
                labels: BTreeMap::new(),
                ip: Some("10.1.2.3".parse().unwrap()),
            }
        )
        .unwrap(),
        ResolvedVersion::Stable(2),
        "R2：纯 IP 规则对无 instance_id 客户端不生效（Q2 门闩）"
    );
    // 对照组：有 instance_id + IP 命中 → 进灰度（IP 规则对已标识客户端生效）
    assert_eq!(
        s.resolve_version(
            &pid,
            &dev,
            &ClientCtx {
                instance_id: "web-ip".into(),
                labels: BTreeMap::new(),
                ip: Some("10.1.2.3".parse().unwrap()),
            }
        )
        .unwrap(),
        ResolvedVersion::Gray(2),
        "IP 规则对已上报 instance_id 的客户端生效"
    );
}

/// T8 Q5 保留策略：prune_versions 不触碰灰度快照（gray-snap/ 前缀独立于 v/）。
#[test]
fn prune_keeps_gray_snapshot() {
    let mut s = sm();
    let (pid, dev) = gray_setup(&mut s); // active=2
                                         // 冲到 v102（越过 checkpoint 100），使 prune 真正能裁掉早期 v/ 历史
    for i in 3..=102u64 {
        s.apply(
            &Command::DraftUpdate {
                project: pid.clone(),
                branch: dev.clone(),
                updates: vec![DraftUpdateItem {
                    group: "redis".into(),
                    key: "host".into(),
                    value: Value::String(format!("h{i}")),
                }],
                deletes: vec![],
                shared_bindings: vec![],
                operator: String::new(),
                ts: 0,
                expected_draft_rev: None,
            },
            100 + i as i64,
        )
        .unwrap();
        s.apply(
            &Command::Publish {
                project: pid.clone(),
                branch: dev.clone(),
                comment: format!("v{i}"),
                request_id: format!("p{i}"),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            200 + i as i64,
        )
        .unwrap();
    }
    // 新草稿作为灰度内容（循环最后一次 Publish 已清空草稿）
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "host".into(),
                value: Value::String("gray-host".into()),
            }],
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        300,
    )
    .unwrap();
    s.apply(
        &Command::GrayPublish {
            project: pid.clone(),
            branch: dev.clone(),
            rule: label_rule("zone", "cn-north-1"),
            comment: "g".into(),
            request_id: "g1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        500,
    )
    .unwrap();
    // keep=1：最新保留 = v101 → 基座对齐 checkpoint 100 → v1..v99 被裁剪
    let removed = s.prune_versions(&pid, &dev, 1).unwrap();
    assert!(removed >= 90, "早期 v/ 历史被裁剪（实际移除 {removed}）");
    // 灰度快照可读、解析仍工作（Q5：灰度目标不被裁）
    let snap = s.gray_snapshot_of(&pid, &dev, 1).unwrap();
    assert_eq!(snap["redis"]["host"], Value::String("gray-host".into()));
    assert_eq!(
        s.resolve_version(&pid, &dev, &ctx("web-1", &[("zone", "cn-north-1")], None))
            .unwrap(),
        ResolvedVersion::Gray(1),
        "灰度客户端 resolve 目标未被裁掉（Q5）"
    );
    // 稳定版仍可读（active 保留）
    let cfg = s.get_config(&pid, &dev, 0).unwrap();
    assert_eq!(cfg.version, 102);
    assert_eq!(cfg.groups["redis"]["host"], Value::String("h102".into()));
}

// ==================== G3 数据面解析（design/g3-dataplane.md，D24/D27/D28） ====================

/// G3-D1：get_config_resolved 三路——灰度命中读 gray-snap/、未命中读稳定、无身份读稳定。
/// 响应带 gray + resolved_version（D27）。
#[test]
fn g3_get_config_resolved_three_paths() {
    let mut s = sm();
    let (pid, dev) = gray_setup(&mut s); // active=2（host=stable-host）
    s.apply(
        &Command::GrayPublish {
            project: pid.clone(),
            branch: dev.clone(),
            rule: label_rule("zone", "cn-north-1"),
            comment: "g".into(),
            request_id: "g1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap(); // gray_seq=1（host=gray-host）

    // ① 灰度命中：读 gray-snap/1 内容；version 保持 active（v/ 空间，R1），resolved_version=gray_seq
    let gray = s
        .get_config_resolved(
            &pid,
            &dev,
            0,
            &ctx("web-1", &[("zone", "cn-north-1")], None),
        )
        .unwrap();
    assert!(gray.gray, "灰度命中 → gray=true");
    assert_eq!(
        gray.version, 2,
        "R1：version 保持 active_version（watch 游标不错位）"
    );
    assert_eq!(gray.resolved_version, 1, "resolved_version = gray_seq");
    assert_eq!(
        gray.groups["redis"]["host"],
        Value::String("gray-host".into())
    );

    // ② 未命中：稳定版（active=2）
    let stable = s
        .get_config_resolved(
            &pid,
            &dev,
            0,
            &ctx("web-2", &[("zone", "cn-south-1")], None),
        )
        .unwrap();
    assert!(!stable.gray, "未命中 → gray=false");
    assert_eq!(stable.version, 2);
    assert_eq!(stable.resolved_version, 2);
    assert_eq!(
        stable.groups["redis"]["host"],
        Value::String("stable-host".into())
    );

    // ③ 无身份（Q2）：永不进灰度
    let noid = s
        .get_config_resolved(&pid, &dev, 0, &ctx("", &[("zone", "cn-north-1")], None))
        .unwrap();
    assert!(!noid.gray);
    assert_eq!(noid.version, 2);

    // 普通 get_config 恒为稳定 + gray=false（管理面/旧路径语义不变）
    let plain = s.get_config(&pid, &dev, 0).unwrap();
    assert!(!plain.gray);
    assert_eq!(plain.resolved_version, 2);
}

/// G3-D2（D24 关键）：gray_seq 与 active_version 数值巧合时仍读对快照。
/// 构造：仅结构发布（active=1）后直接灰度发布 → gray_seq=1 与 active=1 数值相等。
#[test]
fn g3_numeric_coincidence_gray_seq_eq_active() {
    let mut s = sm();
    let (pid, _branches) = setup(&mut s); // 结构发布后各分支 active=1
    let dev = BranchName("dev".into());
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "host".into(),
                value: Value::String("gray-host".into()),
            }],
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        10,
    )
    .unwrap();
    s.apply(
        &Command::GrayPublish {
            project: pid.clone(),
            branch: dev.clone(),
            rule: label_rule("zone", "cn-north-1"),
            comment: "g".into(),
            request_id: "g1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        11,
    )
    .unwrap();
    // 数值巧合：active=1（结构发布）、gray_seq=1（首次灰度）
    let st = s.get_branch_state(&pid, &dev).unwrap().unwrap();
    assert_eq!(st.active_version, 1);
    assert_eq!(st.gray_seq, 1, "数值巧合构造成立");
    // 命中客户端读灰度快照（gray-snap/1 = gray-host），不是 v/1（空值）
    let gray = s
        .get_config_resolved(
            &pid,
            &dev,
            0,
            &ctx("web-1", &[("zone", "cn-north-1")], None),
        )
        .unwrap();
    assert!(gray.gray);
    assert_eq!(gray.version, 1);
    assert_eq!(
        gray.groups["redis"]["host"],
        Value::String("gray-host".into()),
        "D24：数值巧合时必须读 gray-snap/ 而非 v/"
    );
    // 稳定客户端读 v/1（空值）
    let stable = s
        .get_config_resolved(&pid, &dev, 0, &ctx("web-2", &[], None))
        .unwrap();
    assert!(!stable.gray);
    assert!(stable.groups.is_empty(), "v/1 是结构发布的空值快照");
}

/// G3-D3（D28）：显式 version≠0 不 resolve——灰度活跃 + 身份命中时，
/// version=N 恒读 v/N（管理面/历史/reveal 语义）。
#[test]
fn g3_explicit_version_bypasses_resolve() {
    let mut s = sm();
    let (pid, dev) = gray_setup(&mut s); // active=2
    s.apply(
        &Command::GrayPublish {
            project: pid.clone(),
            branch: dev.clone(),
            rule: label_rule("zone", "cn-north-1"),
            comment: "g".into(),
            request_id: "g1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    // 身份命中灰度，但显式请求 version=2 → 恒读 v/2（稳定内容）
    let snap = s
        .get_config_resolved(
            &pid,
            &dev,
            2,
            &ctx("web-1", &[("zone", "cn-north-1")], None),
        )
        .unwrap();
    assert!(!snap.gray, "显式版本不 resolve");
    assert_eq!(snap.version, 2);
    assert_eq!(
        snap.groups["redis"]["host"],
        Value::String("stable-host".into())
    );
}

// ==================== G1 发布策略（design/g1-policy.md，D35/D36） ====================

/// G1-D1：publish-policy=warn——缺 required 项时校验失败仅记录继续发布（active 推进）。
#[test]
fn g1_warn_policy_publishes_incomplete() {
    let mut s = sm();
    let (pid, _branches) = setup(&mut s); // 结构含 redis.host(required)
    let dev = BranchName("dev".into());
    // 草稿缺 required 的 host（只有可选 port）
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "port".into(),
                value: Value::Int(6379),
            }],
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        10,
    )
    .unwrap();
    // Block（默认）→ 拒绝
    let e = s
        .apply(
            &Command::Publish {
                project: pid.clone(),
                branch: dev.clone(),
                comment: "block".into(),
                request_id: "b1".into(),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Block,
            },
            11,
        )
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::PublishBlocked, "Block 拒绝缺 required");
    // Warn → 继续发布
    let events = s
        .apply(
            &Command::Publish {
                project: pid.clone(),
                branch: dev.clone(),
                comment: "warn".into(),
                request_id: "w1".into(),
                operator: String::new(),
                ts: 0,
                policy: PublishPolicy::Warn,
            },
            12,
        )
        .unwrap();
    assert_eq!(events.len(), 1);
    let cfg = s.get_config(&pid, &dev, 0).unwrap();
    assert_eq!(cfg.version, 2, "warn 放行：active 推进");
    assert!(cfg.groups["redis"].contains_key("port"));
}

/// G1-D2：shared-cascade=manual——共享发布只更共享版本，不级联引用分支；
/// 引用分支下次发布时经物化读取新共享值。
#[test]
fn g1_manual_cascade_shared_publish() {
    let mut s = sm();
    let (pid, _branches) = setup(&mut s);
    let dev = BranchName("dev".into());
    // 共享项草稿 + 发布（v1）
    s.apply(
        &Command::SharedDraftUpdate {
            item: SharedItem {
                key: "timeout".into(),
                ty: ValueType::Int,
                secret: false,
                required: false,
                value: Value::Int(30),
                version: 0,
                description: None,
            },
            operator: String::new(),
        },
        10,
    )
    .unwrap();
    s.apply(
        &Command::SharedPublish {
            comment: "v1".into(),
            request_id: "sp1".into(),
            operator: String::new(),
            ts: 0,
            cascade: SharedCascadeMode::Auto,
            policy: PublishPolicy::Block,
        },
        11,
    )
    .unwrap();
    // 结构 v2：redis.port 标记为引用共享
    let mut groups = redis_structure();
    for g in &mut groups {
        for item in &mut g.items {
            if item.key == "port" {
                item.shared = true;
            }
        }
    }
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
            base_version: 2,
            groups,
            operator: String::new(),
        },
        12,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "ref port".into(),
            request_id: "s2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        13,
    )
    .unwrap();
    // 发布 dev（草稿只填 host + 绑定 redis/port → timeout）→ port 由共享物化 30
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "host".into(),
                value: Value::String("h".into()),
            }],
            deletes: vec![],
            shared_bindings: vec![SharedBinding {
                group: "redis".into(),
                key: "port".into(),
                shared_key: "timeout".into(),
            }],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        14,
    )
    .unwrap();
    s.apply(
        &Command::Publish {
            project: pid.clone(),
            branch: dev.clone(),
            comment: "v2".into(),
            request_id: "p1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        15,
    )
    .unwrap();
    let cfg = s.get_config(&pid, &dev, 0).unwrap();
    assert_eq!(cfg.groups["redis"]["port"], Value::Int(30), "物化共享值");
    let st_before = s.get_branch_state(&pid, &dev).unwrap().unwrap();
    let active_before = st_before.active_version;

    // manual 共享发布（值 60）→ 共享版本推进，但引用分支版本不推进
    s.apply(
        &Command::SharedDraftUpdate {
            item: SharedItem {
                key: "timeout".into(),
                ty: ValueType::Int,
                secret: false,
                required: false,
                value: Value::Int(60),
                version: 1,
                description: None,
            },
            operator: String::new(),
        },
        16,
    )
    .unwrap();
    let events = s
        .apply(
            &Command::SharedPublish {
                comment: "v2".into(),
                request_id: "sp2".into(),
                operator: String::new(),
                ts: 0,
                cascade: SharedCascadeMode::Manual,
                policy: PublishPolicy::Block,
            },
            17,
        )
        .unwrap();
    assert!(events.is_empty(), "manual 不级联 → 无分支事件");
    let st = s.get_branch_state(&pid, &dev).unwrap().unwrap();
    assert_eq!(
        st.active_version, active_before,
        "manual：引用分支版本不推进"
    );
    // 共享版本已更新
    assert_eq!(s.get_shared("timeout").unwrap().unwrap().version, 2);
    // 引用分支下次发布 → 物化新共享值 60
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![DraftUpdateItem {
                group: "redis".into(),
                key: "host".into(),
                value: Value::String("h2".into()),
            }],
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        18,
    )
    .unwrap();
    s.apply(
        &Command::Publish {
            project: pid.clone(),
            branch: dev.clone(),
            comment: "v3".into(),
            request_id: "p2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        19,
    )
    .unwrap();
    let cfg = s.get_config(&pid, &dev, 0).unwrap();
    assert_eq!(
        cfg.groups["redis"]["port"],
        Value::Int(60),
        "下次发布物化新共享值"
    );
}

#[test]
fn required_secret_is_preserved_when_not_in_next_draft() {
    let mut s = sm();
    let pid: ProjectId = "order-service".into();
    assert!(s
        .apply(
            &Command::ProjectCreate {
                name: "order-service".into(),
                operator: String::new(),
                ts: 0
            },
            1
        )
        .is_ok());
    let branches = s.list_branches(&pid).unwrap();
    let dev = BranchName("dev".into());
    assert_eq!(branches.len(), 3);
    s.apply(
        &Command::StructureDraftSet {
            project: pid.clone(),
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
                        required: true,
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
    s.apply(
        &Command::PublishStructure {
            project: pid.clone(),
            comment: "s".into(),
            request_id: "s1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        3,
    )
    .unwrap();
    let secret = Value::Secret(dsh_core::model::Ciphertext {
        enc: "aes-256-gcm".into(),
        v: 1,
        dek_v: 1,
        nonce: "n".into(),
        ct: "c".into(),
        edek: "e".into(),
        edek_nonce: "en".into(),
    });
    // 首次发布：host + secret
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![
                DraftUpdateItem {
                    group: "g".into(),
                    key: "host".into(),
                    value: Value::String("h1".into()),
                },
                DraftUpdateItem {
                    group: "g".into(),
                    key: "pass".into(),
                    value: secret.clone(),
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
    s.apply(
        &Command::Publish {
            project: pid.clone(),
            branch: dev.clone(),
            comment: "v1".into(),
            request_id: "r1".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        5,
    )
    .unwrap();
    // 第二次只改 host，secret 留空（UI 的“不修改”路径不会写入 pass 草稿）
    s.apply(
        &Command::DraftUpdate {
            project: pid.clone(),
            branch: dev.clone(),
            updates: vec![DraftUpdateItem {
                group: "g".into(),
                key: "host".into(),
                value: Value::String("h2".into()),
            }],
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        6,
    )
    .unwrap();
    s.apply(
        &Command::Publish {
            project: pid.clone(),
            branch: dev.clone(),
            comment: "v2".into(),
            request_id: "r2".into(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        7,
    )
    .unwrap();
    let cfg = s.get_config(&pid, &dev, 0).unwrap();
    assert_eq!(cfg.version, 3);
    assert_eq!(cfg.groups["g"]["host"], Value::String("h2".into()));
    assert_eq!(
        cfg.groups["g"]["pass"], secret,
        "未重填的必填 secret 应保留旧密文"
    );
}
