//! 全局管理员（Global Admin）状态机测试。
//! 设计文档: dev_docs/design/global-admin.md §4 / §6 / §13。
//!
//! 覆盖：账号增删改、用户名保留名与大小写口径、GA↔PA 双向互斥、
//! 会话多开/登出/心跳、键空间隔离（改密与全量收口不误伤）、
//! 全量收口对损坏记录的健壮性、Principal 与 Command 的 serde 兼容。

use std::sync::Arc;

use dsh_core::command::Command;
use dsh_core::model::{AdminSession, GlobalAdminAccount, Principal, ProjectId};
use dsh_core::store::KeyValuePairs;
use dsh_core::{Error, ErrorKind, InMemoryStore, StateMachine, Store};

fn sm() -> StateMachine {
    StateMachine::new(Box::new(InMemoryStore::new()))
}

/// 建项目（GA 侧无需项目，但互斥用例要建 PA）。
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

fn create_ga(s: &mut StateMachine, username: &str) {
    s.apply(
        &Command::GlobalAdminCreate {
            username: username.to_string(),
            salt: String::new(),
            password_hash: "hash".to_string(),
            ts: 0,
        },
        1_000,
    )
    .unwrap();
}

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

fn ga_login(s: &mut StateMachine, username: &str, sid: &str) {
    s.apply(
        &Command::MultiGaSessionLogin {
            username: username.to_string(),
            token_hash: format!("h-{username}-{sid}"),
            issued_at: 1_000,
            expires_at: Some(9_999_999),
            device_id: "cli".to_string(),
            session_id: sid.to_string(),
        },
        1_000,
    )
    .unwrap();
}

fn admin_login(s: &mut StateMachine, sid: &str) {
    s.apply(
        &Command::MultiSessionLogin {
            token_hash: format!("h-admin-{sid}"),
            issued_at: 1_000,
            expires_at: Some(9_999_999),
            session_id: sid.to_string(),
        },
        1_000,
    )
    .unwrap();
}

// ---------------------------------------------------------------- 账号

#[test]
fn ga_create_get_list() {
    let mut s = sm();
    create_ga(&mut s, "alice");
    create_ga(&mut s, "bob");

    let acct = s.get_global_admin("alice").unwrap().expect("账号存在");
    assert_eq!(acct.username, "alice");
    assert_eq!(acct.created_at, 1_000);

    let list: Vec<String> = s
        .list_global_admins()
        .unwrap()
        .into_iter()
        .map(|a| a.username)
        .collect();
    assert_eq!(list, vec!["alice".to_string(), "bob".to_string()]);
    assert!(s.has_global_admins().unwrap());
}

#[test]
fn ga_create_duplicate_rejected() {
    let mut s = sm();
    create_ga(&mut s, "alice");
    let err = s
        .apply(
            &Command::GlobalAdminCreate {
                username: "alice".to_string(),
                salt: String::new(),
                password_hash: "h2".to_string(),
                ts: 0,
            },
            1_000,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Conflict);
}

#[test]
fn ga_delete_and_not_found() {
    let mut s = sm();
    create_ga(&mut s, "alice");
    s.apply(
        &Command::GlobalAdminDelete {
            username: "alice".to_string(),
        },
        1_000,
    )
    .unwrap();
    assert!(s.get_global_admin("alice").unwrap().is_none());
    assert!(!s.has_global_admins().unwrap());

    // 删不存在 / 改密不存在 → NotFound
    let e1 = s
        .apply(
            &Command::GlobalAdminDelete {
                username: "alice".to_string(),
            },
            1_000,
        )
        .unwrap_err();
    assert_eq!(e1.kind, ErrorKind::NotFound);
    let e2 = s
        .apply(
            &Command::GlobalAdminSetPassword {
                username: "alice".to_string(),
                salt: String::new(),
                password_hash: "h".to_string(),
            },
            1_000,
        )
        .unwrap_err();
    assert_eq!(e2.kind, ErrorKind::NotFound);
}

#[test]
fn ga_bad_username_rejected() {
    for bad in ["a", "admin", "Admin", "ADMIN", "a.b", ""] {
        let mut s = sm();
        let err = s
            .apply(
                &Command::GlobalAdminCreate {
                    username: bad.to_string(),
                    salt: String::new(),
                    password_hash: "h".to_string(),
                    ts: 0,
                },
                1_000,
            )
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::Validation, "应拒绝用户名 {bad:?}");
    }
}

/// D3（设计 §4）：GA↔PA 互斥**不区分大小写**，双向。
#[test]
fn ga_pa_mutual_exclusion_case_insensitive() {
    // PA alice → GA alice / Alice 均 Conflict
    let mut s = setup_project("p1");
    create_pa(&mut s, "p1", "alice");
    for ga_name in ["alice", "Alice"] {
        let err = s
            .apply(
                &Command::GlobalAdminCreate {
                    username: ga_name.to_string(),
                    salt: String::new(),
                    password_hash: "h".to_string(),
                    ts: 0,
                },
                1_000,
            )
            .unwrap_err();
        assert_eq!(
            err.kind,
            ErrorKind::Conflict,
            "GA {ga_name:?} 应被 PA 占用拒绝"
        );
    }

    // GA bob → PA bob / Bob 均 Conflict（反向）
    let mut s2 = setup_project("p1");
    create_ga(&mut s2, "bob");
    for pa_name in ["bob", "Bob"] {
        let err = s2
            .apply(
                &Command::ProjectAdminCreate {
                    project: ProjectId("p1".to_string()),
                    username: pa_name.to_string(),
                    salt: "s".to_string(),
                    password_hash: "h".to_string(),
                    ts: 0,
                },
                1_000,
            )
            .unwrap_err();
        assert_eq!(
            err.kind,
            ErrorKind::Conflict,
            "PA {pa_name:?} 应被 GA 占用拒绝"
        );
    }
}

/// 设计 §4 的显式取舍：**GA 内部是精确键唯一**，大小写变体可并存。
#[test]
fn ga_internal_case_variants_coexist() {
    let mut s = sm();
    create_ga(&mut s, "carol");
    create_ga(&mut s, "Carol");
    assert!(s.get_global_admin("carol").unwrap().is_some());
    assert!(s.get_global_admin("Carol").unwrap().is_some());
}

// ---------------------------------------------------------------- 会话

#[test]
fn ga_delete_cascades_sessions() {
    let mut s = sm();
    create_ga(&mut s, "alice");
    ga_login(&mut s, "alice", "s1");
    ga_login(&mut s, "alice", "s2");
    assert!(s.get_ga_session_with("alice", "s1").unwrap().is_some());

    s.apply(
        &Command::GlobalAdminDelete {
            username: "alice".to_string(),
        },
        1_000,
    )
    .unwrap();
    assert!(s.get_ga_session_with("alice", "s1").unwrap().is_none());
    assert!(s.get_ga_session_with("alice", "s2").unwrap().is_none());
}

#[test]
fn ga_set_password_cascades_sessions() {
    let mut s = sm();
    create_ga(&mut s, "alice");
    ga_login(&mut s, "alice", "s1");

    s.apply(
        &Command::GlobalAdminSetPassword {
            username: "alice".to_string(),
            salt: String::new(),
            password_hash: "newhash".to_string(),
        },
        1_000,
    )
    .unwrap();
    assert!(s.get_ga_session_with("alice", "s1").unwrap().is_none());
    assert_eq!(
        s.get_global_admin("alice").unwrap().unwrap().password_hash,
        "newhash"
    );
}

#[test]
fn ga_multi_session_logout_and_heartbeat() {
    let mut s = sm();
    create_ga(&mut s, "alice");
    ga_login(&mut s, "alice", "s1");
    ga_login(&mut s, "alice", "s2");

    // 只登出一个
    s.apply(
        &Command::MultiGaSessionLogout {
            username: "alice".to_string(),
            session_id: "s1".to_string(),
        },
        1_000,
    )
    .unwrap();
    assert!(s.get_ga_session_with("alice", "s1").unwrap().is_none());
    assert!(s.get_ga_session_with("alice", "s2").unwrap().is_some());

    // 心跳续期
    s.apply(
        &Command::MultiGaSessionHeartbeat {
            username: "alice".to_string(),
            session_id: "s2".to_string(),
            expires_at: Some(1_234_567),
        },
        1_000,
    )
    .unwrap();
    assert_eq!(
        s.get_ga_session_with("alice", "s2")
            .unwrap()
            .unwrap()
            .expires_at,
        Some(1_234_567)
    );

    // 无会话 → SessionExpired
    let err = s
        .apply(
            &Command::MultiGaSessionHeartbeat {
                username: "alice".to_string(),
                session_id: "nope".to_string(),
                expires_at: None,
            },
            1_000,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::SessionExpired);

    // 会话主体是 GlobalAdmin（身份不可由 token 伪造，以状态机为准）
    assert_eq!(
        s.get_ga_session_with("alice", "s2")
            .unwrap()
            .unwrap()
            .principal,
        Principal::GlobalAdmin {
            username: "alice".to_string()
        }
    );
}

/// 登录要求账号存在（不得复用 PA 的读取路径，否则 GA 登录恒 NotFound）。
#[test]
fn ga_session_login_requires_account() {
    let mut s = sm();
    let err = s
        .apply(
            &Command::MultiGaSessionLogin {
                username: "ghost".to_string(),
                token_hash: "h".to_string(),
                issued_at: 1_000,
                expires_at: None,
                device_id: "cli".to_string(),
                session_id: "s1".to_string(),
            },
            1_000,
        )
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::NotFound);
}

/// 键空间隔离：MultiSessionLogoutAll 只清 sess/admin*，不碰 sess/ga/*。
#[test]
fn ga_sessions_survive_admin_logout_all() {
    let mut s = sm();
    create_ga(&mut s, "alice");
    ga_login(&mut s, "alice", "s1");
    admin_login(&mut s, "a1");

    s.apply(&Command::MultiSessionLogoutAll, 1_000).unwrap();

    assert!(s.get_ga_session_with("alice", "s1").unwrap().is_some());
    assert!(s.get_session_with("a1").unwrap().is_none());
}

/// 全量收口：GlobalAdminSessionsRevokeAll 清全部 GA 会话，且不碰 super 会话。
#[test]
fn ga_revoke_all_sessions() {
    let mut s = sm();
    create_ga(&mut s, "alice");
    create_ga(&mut s, "bob");
    ga_login(&mut s, "alice", "s1");
    ga_login(&mut s, "bob", "s1");
    admin_login(&mut s, "a1");

    s.apply(&Command::GlobalAdminSessionsRevokeAll, 1_000)
        .unwrap();

    assert!(s.get_ga_session_with("alice", "s1").unwrap().is_none());
    assert!(s.get_ga_session_with("bob", "s1").unwrap().is_none());
    assert!(s.get_session_with("a1").unwrap().is_some());
}

// ------------------------- 全量收口必须按 key 空间，不能解析值

/// 共享句柄：包一层 InMemoryStore，让测试能直接写「损坏记录」。
struct SharedStore(Arc<InMemoryStore>);

impl Store for SharedStore {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.0.get(key)
    }
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.0.put(key, value)
    }
    fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.0.delete(key)
    }
    fn get_prefix(&self, prefix: &[u8]) -> Result<KeyValuePairs, Error> {
        self.0.get_prefix(prefix)
    }
    fn put_batch(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Error> {
        self.0.put_batch(pairs)
    }
    fn write_batch(&self, puts: &[(Vec<u8>, Vec<u8>)], deletes: &[Vec<u8>]) -> Result<(), Error> {
        self.0.write_batch(puts, deletes)
    }
}

/// 一条无法反序列化的 adm/ga/{u} 记录：list_global_admins 会静默丢弃它，
/// 但 has_global_admins（键空间判定）与 GlobalAdminSessionsRevokeAll（扫 key 后缀）都必须看见它，
/// 否则「全量收口」会漏掉一个仍有活会话的账号。
#[test]
fn ga_revoke_all_covers_corrupt_records() {
    let handle = Arc::new(InMemoryStore::new());
    let mut s = StateMachine::new(Box::new(SharedStore(handle.clone())));

    // 合法账号 + 会话，然后把账号记录写成坏 JSON（模拟半写/损坏）
    create_ga(&mut s, "broken");
    ga_login(&mut s, "broken", "s1");
    assert!(s.get_ga_session_with("broken", "s1").unwrap().is_some());
    handle.put(b"adm/ga/broken", b"{not json").unwrap();

    assert!(
        s.list_global_admins().unwrap().is_empty(),
        "损坏记录被静默丢弃"
    );
    assert!(s.has_global_admins().unwrap(), "键空间判定必须仍看见它");

    s.apply(&Command::GlobalAdminSessionsRevokeAll, 1_000)
        .unwrap();
    assert!(
        s.get_ga_session_with("broken", "s1").unwrap().is_none(),
        "全量收口必须按 key 空间扫，不能因损坏记录漏删会话"
    );
}

// ---------------------------------------------------------------- serde 兼容

#[test]
fn principal_serde_roundtrip_and_old_data() {
    // 新变体往返（wire 形式为 PascalCase，枚举无 rename_all）
    let p = Principal::GlobalAdmin {
        username: "alice".to_string(),
    };
    let js = serde_json::to_string(&p).unwrap();
    assert_eq!(js, r#"{"kind":"GlobalAdmin","username":"alice"}"#);
    assert_eq!(serde_json::from_str::<Principal>(&js).unwrap(), p);

    // 既有形态不变
    assert_eq!(
        serde_json::from_str::<Principal>(r#"{"kind":"Admin"}"#).unwrap(),
        Principal::Admin
    );
    assert_eq!(
        serde_json::from_str::<Principal>(
            r#"{"kind":"ProjectAdmin","username":"a","project":"p"}"#
        )
        .unwrap(),
        Principal::ProjectAdmin {
            username: "a".to_string(),
            project: ProjectId("p".to_string())
        }
    );

    // 旧 AdminSession（无 principal 字段）→ 默认 Admin
    let old = r#"{"token_hash":"h","issued_at":1,"expires_at":null,"device_id":"cli"}"#;
    let sess: AdminSession = serde_json::from_str(old).unwrap();
    assert_eq!(sess.principal, Principal::Admin);
}

#[test]
fn command_serde_compat_and_account_roundtrip() {
    // 新命令变体可往返
    let cmd = Command::GlobalAdminDelete {
        username: "a".to_string(),
    };
    let js = serde_json::to_string(&cmd).unwrap();
    assert_eq!(js, r#"{"GlobalAdminDelete":{"username":"a"}}"#);
    assert_eq!(serde_json::from_str::<Command>(&js).unwrap(), cmd);

    // unit 变体
    assert_eq!(
        serde_json::from_str::<Command>(r#""GlobalAdminSessionsRevokeAll""#).unwrap(),
        Command::GlobalAdminSessionsRevokeAll
    );

    // ts 缺省 → 0（旧日志重放兼容）
    let c: Command = serde_json::from_str(
        r#"{"GlobalAdminCreate":{"username":"a","salt":"","password_hash":"h"}}"#,
    )
    .unwrap();
    match c {
        Command::GlobalAdminCreate { ts, .. } => assert_eq!(ts, 0),
        _ => panic!("变体不匹配"),
    }

    // 既有 PA 变体形状不变
    assert!(serde_json::from_str::<Command>(r#"{"ProjectAdminDelete":{"username":"a"}}"#).is_ok());

    // 账号实体可 serde 往返（load/save_pending 依赖）
    let acct = GlobalAdminAccount {
        username: "a".to_string(),
        salt: String::new(),
        password_hash: "h".to_string(),
        created_at: 7,
    };
    let js = serde_json::to_string(&acct).unwrap();
    assert_eq!(
        serde_json::from_str::<GlobalAdminAccount>(&js).unwrap(),
        acct
    );
}
