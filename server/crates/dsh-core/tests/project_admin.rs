//! 项目管理员（Project Admin）状态机测试。
//! 设计文档: dev_docs/design/project-admin.md §8 M1。

use dsh_core::command::Command;
use dsh_core::model::{AdminSession, Principal};
use dsh_core::model::{BranchName, ProjectId, PublishPolicy};
use dsh_core::{token_hash, ErrorKind, InMemoryStore, StateMachine};

fn sm() -> StateMachine {
    StateMachine::new(Box::new(InMemoryStore::new()))
}

/// 读取项目当前结构版本（无结构 → 0）。
fn current_structure_version(s: &StateMachine, project: &str) -> u64 {
    // 通过公开读取接口；若无公开接口则默认 1（项目创建自带初始结构 v1）
    let st = s.get_structure(&ProjectId(project.to_string()));
    match st {
        Ok(Some(v)) => v.version,
        _ => 1,
    }
}

/// 建项目 + 发布结构，返回状态机（参照 setup() 最小化）。
fn setup_project(name: &str) -> StateMachine {
    let mut s = sm();
    s.apply(
        &Command::ProjectCreate {
            name: name.to_string(),
            operator: String::new(),
            ts: 0,
            clone_from: None,
        },
        1_000,
    )
    .unwrap();
    s
}

/// 建一个 PA 账号（项目须先存在）。
fn create_pa(s: &mut StateMachine, project: &str, username: &str) {
    s.apply(
        &Command::ProjectAdminCreate {
            project: ProjectId(project.to_string()),
            username: username.to_string(),
            salt: "s16bytesalt00".to_string(),
            password_hash: "hash".to_string(),
            ts: 0,
        },
        1_000,
    )
    .unwrap();
}

#[test]
fn pa_create_and_get() {
    let mut s = setup_project("alpha");
    create_pa(&mut s, "alpha", "alice");

    let acct = s
        .get_project_admin("alice")
        .unwrap()
        .expect("account exists");
    assert_eq!(acct.username, "alice");
    assert_eq!(acct.project.0, "alpha");
    assert!(!acct.salt.is_empty());
    assert!(!acct.password_hash.is_empty());

    let list = s.list_project_admins("alpha").unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].username, "alice");
}

#[test]
fn pa_create_duplicate_rejected() {
    let mut s = setup_project("alpha");
    create_pa(&mut s, "alpha", "alice");
    let err = s
        .apply(
            &Command::ProjectAdminCreate {
                project: ProjectId("alpha".to_string()),
                username: "alice".to_string(),
                salt: "x".to_string(),
                password_hash: "h".to_string(),
                ts: 0,
            },
            2_000,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Conflict);
}

#[test]
fn pa_create_missing_project_rejected() {
    let mut s = sm();
    let err = s
        .apply(
            &Command::ProjectAdminCreate {
                project: ProjectId("ghost".to_string()),
                username: "bob".to_string(),
                salt: "x".to_string(),
                password_hash: "h".to_string(),
                ts: 0,
            },
            1_000,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::NotFound);
}

#[test]
fn pa_create_bad_username_rejected() {
    let mut s = setup_project("alpha");
    for bad in ["a", "bad name", "bad/name", "管理员管理员", "admin"] {
        let err = s.apply(
            &Command::ProjectAdminCreate {
                project: ProjectId("alpha".to_string()),
                username: bad.to_string(),
                salt: "x".to_string(),
                password_hash: "h".to_string(),
                ts: 0,
            },
            1_000,
        );
        assert!(err.is_err(), "username {bad:?} 应被拒绝");
    }
}

#[test]
fn pa_delete_cascades_session() {
    let mut s = setup_project("alpha");
    create_pa(&mut s, "alpha", "alice");

    s.apply(
        &Command::PaSessionLogin {
            username: "alice".to_string(),
            token_hash: token_hash("tok-1"),
            issued_at: 1_000,
            expires_at: Some(9_000),
            device_id: "cli".to_string(),
        },
        2_000,
    )
    .unwrap();
    assert!(s.get_pa_session("alice").unwrap().is_some());

    s.apply(
        &Command::ProjectAdminDelete {
            username: "alice".to_string(),
        },
        3_000,
    )
    .unwrap();
    assert!(s.get_project_admin("alice").unwrap().is_none());
    assert!(
        s.get_pa_session("alice").unwrap().is_none(),
        "删号必须级联删会话"
    );

    let err = s
        .apply(
            &Command::ProjectAdminDelete {
                username: "alice".to_string(),
            },
            4_000,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::NotFound);
}

#[test]
fn pa_set_password_cascades_session() {
    let mut s = setup_project("alpha");
    create_pa(&mut s, "alpha", "alice");

    s.apply(
        &Command::PaSessionLogin {
            username: "alice".to_string(),
            token_hash: token_hash("tok-1"),
            issued_at: 1_000,
            expires_at: Some(9_000),
            device_id: "cli".to_string(),
        },
        2_000,
    )
    .unwrap();

    s.apply(
        &Command::ProjectAdminSetPassword {
            username: "alice".to_string(),
            salt: "new-salt".to_string(),
            password_hash: "new-hash".to_string(),
        },
        3_000,
    )
    .unwrap();
    assert!(
        s.get_pa_session("alice").unwrap().is_none(),
        "改密必须级联删会话"
    );
    let acct = s.get_project_admin("alice").unwrap().expect("账号仍在");
    assert_eq!(acct.password_hash, "new-hash");
}

#[test]
fn pa_session_single_per_account_and_independent_from_admin() {
    let mut s = setup_project("alpha");
    create_pa(&mut s, "alpha", "alice");
    create_pa(&mut s, "alpha", "bob");

    // 全局 admin 会话 + alice 会话并存
    s.apply(
        &Command::SessionLogin {
            token_hash: token_hash("adm-tok"),
            issued_at: 1_000,
            expires_at: Some(9_000),
        },
        1_000,
    )
    .unwrap();
    s.apply(
        &Command::PaSessionLogin {
            username: "alice".to_string(),
            token_hash: token_hash("alice-tok"),
            issued_at: 1_100,
            expires_at: Some(9_000),
            device_id: "cli".to_string(),
        },
        1_100,
    )
    .unwrap();
    assert!(s.get_session().unwrap().is_some());
    assert!(s.get_pa_session("alice").unwrap().is_some());

    // alice 已在线 → 再登 409（is_some 判定，不涉时钟）
    let err = s
        .apply(
            &Command::PaSessionLogin {
                username: "alice".to_string(),
                token_hash: token_hash("alice-tok-2"),
                issued_at: 1_200,
                expires_at: Some(9_000),
                device_id: "cli".to_string(),
            },
            1_200,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::SessionInUse);

    // bob 不受 alice 影响（每账号单会话，互不踢线）
    s.apply(
        &Command::PaSessionLogin {
            username: "bob".to_string(),
            token_hash: token_hash("bob-tok"),
            issued_at: 1_300,
            expires_at: Some(9_000),
            device_id: "cli".to_string(),
        },
        1_300,
    )
    .unwrap();

    // alice 登出后可重登
    s.apply(
        &Command::PaSessionLogout {
            username: "alice".to_string(),
        },
        1_400,
    )
    .unwrap();
    assert!(s.get_pa_session("alice").unwrap().is_none());
    s.apply(
        &Command::PaSessionLogin {
            username: "alice".to_string(),
            token_hash: token_hash("alice-tok-3"),
            issued_at: 1_500,
            expires_at: Some(9_000),
            device_id: "cli".to_string(),
        },
        1_500,
    )
    .unwrap();
}

#[test]
fn pa_session_login_writes_principal() {
    let mut s = setup_project("alpha");
    create_pa(&mut s, "alpha", "alice");

    s.apply(
        &Command::PaSessionLogin {
            username: "alice".to_string(),
            token_hash: token_hash("tok"),
            issued_at: 1_000,
            expires_at: Some(9_000),
            device_id: "cli".to_string(),
        },
        1_000,
    )
    .unwrap();

    let sess: AdminSession = s.get_pa_session("alice").unwrap().unwrap();
    assert_eq!(
        sess.principal,
        Principal::ProjectAdmin {
            username: "alice".to_string(),
            project: ProjectId("alpha".to_string()),
        }
    );
}

#[test]
fn pa_session_heartbeat_extends_expiry() {
    let mut s = setup_project("alpha");
    create_pa(&mut s, "alpha", "alice");

    s.apply(
        &Command::PaSessionLogin {
            username: "alice".to_string(),
            token_hash: token_hash("tok"),
            issued_at: 1_000,
            expires_at: Some(9_000),
            device_id: "cli".to_string(),
        },
        1_000,
    )
    .unwrap();

    s.apply(
        &Command::PaSessionHeartbeat {
            username: "alice".to_string(),
            expires_at: Some(20_000),
        },
        2_000,
    )
    .unwrap();
    assert_eq!(
        s.get_pa_session("alice").unwrap().unwrap().expires_at,
        Some(20_000)
    );

    // None = 永不过期（照抄现有 SessionHeartbeat 语义）
    s.apply(
        &Command::PaSessionHeartbeat {
            username: "alice".to_string(),
            expires_at: None,
        },
        3_000,
    )
    .unwrap();
    assert_eq!(s.get_pa_session("alice").unwrap().unwrap().expires_at, None);

    // 无会话 → 错误
    s.apply(
        &Command::PaSessionLogout {
            username: "alice".to_string(),
        },
        4_000,
    )
    .unwrap();
    let err = s
        .apply(
            &Command::PaSessionHeartbeat {
                username: "alice".to_string(),
                expires_at: Some(1),
            },
            5_000,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::SessionExpired);
}

#[test]
fn project_delete_cascades_pa_accounts_and_sessions() {
    let mut s = setup_project("alpha");
    let mut other = sm();
    other
        .apply(
            &Command::ProjectCreate {
                name: "beta".to_string(),
                operator: String::new(),
                ts: 0,
                clone_from: None,
            },
            1_000,
        )
        .unwrap();
    create_pa(&mut other, "beta", "keepme");

    create_pa(&mut s, "alpha", "alice");
    create_pa(&mut s, "alpha", "bob");
    s.apply(
        &Command::PaSessionLogin {
            username: "alice".to_string(),
            token_hash: token_hash("t"),
            issued_at: 1_000,
            expires_at: Some(9_000),
            device_id: "cli".to_string(),
        },
        2_000,
    )
    .unwrap();

    s.apply(
        &Command::ProjectDelete {
            id: ProjectId("alpha".to_string()),
            operator: String::new(),
        },
        3_000,
    )
    .unwrap();

    assert!(
        s.get_project_admin("alice").unwrap().is_none(),
        "删项目必须级联删 PA 账号"
    );
    assert!(s.get_project_admin("bob").unwrap().is_none());
    assert!(
        s.get_pa_session("alice").unwrap().is_none(),
        "删项目必须级联删 PA 会话"
    );
    assert!(s.list_project_admins("alpha").unwrap().is_empty());

    // 其他项目账号不受影响
    assert!(other.get_project_admin("keepme").unwrap().is_some());
}

#[test]
fn publish_operator_recorded_in_version() {
    let mut s = setup_project("alpha");
    s.apply(
        &Command::StructureDraftSet {
            project: ProjectId("alpha".to_string()),
            base_version: current_structure_version(&s, "alpha"),
            groups: vec![dsh_core::model::GroupDef {
                name: "g".to_string(),
                items: vec![dsh_core::model::ItemDef {
                    key: "k".to_string(),
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
        1_500,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: ProjectId("alpha".to_string()),
            comment: "init".to_string(),
            request_id: "req-test".to_string(),
            operator: "pa:alice".to_string(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        1_600,
    )
    .unwrap();
    s.apply(
        &Command::DraftUpdate {
            project: ProjectId("alpha".to_string()),
            branch: BranchName("dev".to_string()),
            updates: vec![dsh_core::command::DraftUpdateItem {
                group: "g".to_string(),
                key: "k".to_string(),
                value: dsh_core::Value::String("v1".to_string()),
            }],
            deletes: vec![],
            shared_bindings: vec![],
            operator: "pa:alice".to_string(),
            ts: 0,
            expected_draft_rev: None,
        },
        1_700,
    )
    .unwrap();
    s.apply(
        &Command::Publish {
            project: ProjectId("alpha".to_string()),
            branch: BranchName("dev".to_string()),
            comment: "c".to_string(),
            request_id: "req-test".to_string(),
            operator: "pa:alice".to_string(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        1_800,
    )
    .unwrap();

    let versions = s
        .version_history(
            &ProjectId("alpha".to_string()),
            &BranchName("dev".to_string()),
        )
        .unwrap();
    let last = versions.last().expect("至少一个版本");
    assert_eq!(last.operator, "pa:alice", "发布版本必须记录命令 operator");
}

#[test]
fn publish_operator_defaults_to_admin() {
    let mut s = setup_project("alpha");
    s.apply(
        &Command::StructureDraftSet {
            project: ProjectId("alpha".to_string()),
            base_version: current_structure_version(&s, "alpha"),
            groups: vec![dsh_core::model::GroupDef {
                name: "g".to_string(),
                items: vec![dsh_core::model::ItemDef {
                    key: "k".to_string(),
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
        1_500,
    )
    .unwrap();
    s.apply(
        &Command::PublishStructure {
            project: ProjectId("alpha".to_string()),
            comment: "init".to_string(),
            request_id: "req-test".to_string(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        1_600,
    )
    .unwrap();
    s.apply(
        &Command::DraftUpdate {
            project: ProjectId("alpha".to_string()),
            branch: BranchName("dev".to_string()),
            updates: vec![dsh_core::command::DraftUpdateItem {
                group: "g".to_string(),
                key: "k".to_string(),
                value: dsh_core::Value::String("v1".to_string()),
            }],
            deletes: vec![],
            shared_bindings: vec![],
            operator: String::new(),
            ts: 0,
            expected_draft_rev: None,
        },
        1_700,
    )
    .unwrap();
    s.apply(
        &Command::Publish {
            project: ProjectId("alpha".to_string()),
            branch: BranchName("dev".to_string()),
            comment: "c".to_string(),
            request_id: "req-test".to_string(),
            operator: String::new(),
            ts: 0,
            policy: PublishPolicy::Block,
        },
        1_800,
    )
    .unwrap();

    let versions = s
        .version_history(
            &ProjectId("alpha".to_string()),
            &BranchName("dev".to_string()),
        )
        .unwrap();
    assert_eq!(
        versions.last().unwrap().operator,
        "admin",
        "空 operator 应回退为 admin"
    );
}

#[test]
fn operator_field_serde_backward_compatible() {
    // 旧格式 JSON（无 operator 字段）必须能反序列化（Raft 日志重放兼容）
    let old_publish =
        r#"{"Publish":{"project":"alpha","branch":"dev","comment":"c","request_id":"r1"}}"#;
    let cmd: Command = serde_json::from_str(old_publish).expect("旧 Publish JSON 必须兼容");
    match cmd {
        Command::Publish { operator, .. } => assert_eq!(operator, ""),
        other => panic!("反序列化类型错误: {other:?}"),
    }

    let old_create = r#"{"ProjectCreate":{"name":"alpha"}}"#;
    let cmd: Command = serde_json::from_str(old_create).expect("旧 ProjectCreate JSON 必须兼容");
    match cmd {
        Command::ProjectCreate {
            operator,
            clone_from,
            ..
        } => {
            assert_eq!(operator, "");
            assert!(
                clone_from.is_none(),
                "旧日志无 clone_from 字段 → 默认 None（普通创建）"
            );
        }
        other => panic!("反序列化类型错误: {other:?}"),
    }
}

#[test]
fn admin_session_serde_backward_compatible() {
    // 旧会话 JSON（无 principal 字段）→ Principal::Admin
    let old = r#"{"token_hash":"abc","issued_at":1,"expires_at":9,"device_id":"cli"}"#;
    let sess: AdminSession = serde_json::from_str(old).expect("旧会话 JSON 必须兼容");
    assert_eq!(sess.principal, Principal::Admin);
}
