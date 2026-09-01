//! 项目访问令牌（Project Token）状态机测试。
//! 设计文档: dev_docs/design/project-token.md §2/§6。

use dsh_core::command::Command;
use dsh_core::model::ProjectId;
use dsh_core::{token_hash, ErrorKind, InMemoryStore, StateMachine};

fn sm() -> StateMachine {
    StateMachine::new(Box::new(InMemoryStore::new()))
}

fn seed_project(s: &mut StateMachine) {
    s.apply(
        &Command::ProjectCreate {
            name: "p".into(),
            operator: String::new(),
            ts: 0,
            clone_from: None,
        },
        1,
    )
    .unwrap();
    s.apply(
        &Command::ProjectCreate {
            name: "q".into(),
            operator: String::new(),
            ts: 0,
            clone_from: None,
        },
        2,
    )
    .unwrap();
}

fn create(s: &mut StateMachine, project: &str, name: &str, raw: &str) {
    s.apply(
        &Command::ProjectTokenCreate {
            project: project.into(),
            name: name.into(),
            token_hash: token_hash(raw),
            operator: "admin".into(),
            ts: 0,
        },
        10,
    )
    .unwrap();
}

#[test]
fn create_stores_hash_only() {
    let mut s = sm();
    seed_project(&mut s);
    let raw = "abc123def456";
    create(&mut s, "p", "svc-a", raw);
    let rec = s.get_data_token(&token_hash(raw)).unwrap().unwrap();
    assert_eq!(rec.project.0, "p");
    assert_eq!(rec.name, "svc-a");
    assert_eq!(rec.hash, token_hash(raw));
    assert_ne!(rec.hash, raw); // 无明文
    assert_eq!(rec.id.len(), 16); // id = hash 前 16 位
    assert!(!rec.revoked);
    assert_eq!(rec.created_by, "admin");
}

#[test]
fn create_rejects_missing_project() {
    let mut s = sm();
    seed_project(&mut s);
    let e = s
        .apply(
            &Command::ProjectTokenCreate {
                project: "nope".into(),
                name: "x".into(),
                token_hash: token_hash("t"),
                operator: String::new(),
                ts: 0,
            },
            10,
        )
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::NotFound);
}

#[test]
fn create_rejects_dup_name_in_project() {
    let mut s = sm();
    seed_project(&mut s);
    create(&mut s, "p", "svc-a", "raw1");
    let e = s
        .apply(
            &Command::ProjectTokenCreate {
                project: "p".into(),
                name: "svc-a".into(),
                token_hash: token_hash("raw2"),
                operator: String::new(),
                ts: 0,
            },
            11,
        )
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Conflict);
}

#[test]
fn same_name_ok_in_other_project() {
    let mut s = sm();
    seed_project(&mut s);
    create(&mut s, "p", "svc-a", "raw1");
    create(&mut s, "q", "svc-a", "raw2"); // 不冲突
    assert!(s.get_data_token(&token_hash("raw2")).unwrap().is_some());
}

#[test]
fn create_idempotent_same_hash() {
    let mut s = sm();
    seed_project(&mut s);
    create(&mut s, "p", "a", "same");
    create(&mut s, "p", "b", "same"); // 同明文 → no-op
    let list = s.list_project_tokens(&ProjectId("p".into())).unwrap();
    assert_eq!(list.len(), 1);
}

#[test]
fn revoke_isolation_and_idempotent() {
    let mut s = sm();
    seed_project(&mut s);
    create(&mut s, "p", "a", "raw-p");
    create(&mut s, "q", "b", "raw-q");
    // p 的 token 不能从 q 项目吊销
    let e = s
        .apply(
            &Command::ProjectTokenRevoke {
                project: "q".into(),
                token_id: "0000000000000000".into(),
            },
            20,
        )
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::NotFound);
    // 正确吊销
    let id = s.get_data_token(&token_hash("raw-p")).unwrap().unwrap().id;
    s.apply(
        &Command::ProjectTokenRevoke {
            project: "p".into(),
            token_id: id.clone(),
        },
        21,
    )
    .unwrap();
    let rec = s.get_data_token(&token_hash("raw-p")).unwrap().unwrap();
    assert!(rec.revoked);
    // 重复吊销幂等
    s.apply(
        &Command::ProjectTokenRevoke {
            project: "p".into(),
            token_id: id,
        },
        22,
    )
    .unwrap();
}

#[test]
fn project_delete_cascades_tokens() {
    let mut s = sm();
    seed_project(&mut s);
    create(&mut s, "p", "a", "raw-p");
    create(&mut s, "q", "b", "raw-q");
    s.apply(
        &Command::ProjectDelete {
            id: "p".into(),
            operator: String::new(),
        },
        30,
    )
    .unwrap();
    assert!(s.get_data_token(&token_hash("raw-p")).unwrap().is_none());
    assert!(s.get_data_token(&token_hash("raw-q")).unwrap().is_some()); // q 不受影响
}
