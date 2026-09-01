//! 新建项目「从现有项目克隆结构」（project-clone）HTTP 集成测试。
//! 覆盖：克隆成功（结构逐项相等 / 无结构草稿 / 分支无版本）、负例（源不存在 /
//! 自克隆 / 非法源名 → 422）、空串归一 None、普通创建回归、PA 仍 403、审计可追溯。

use std::sync::{Arc, RwLock};

use dsh_api::{build_router, ApiState};
use dsh_core::command::Command;
use dsh_core::model::ProjectId;
use dsh_core::{token_hash, InMemoryStore, StateMachine};
use dsh_watch::WatchHub;

struct TestServer {
    base: String,
    _state: ApiState,
}

async fn start() -> TestServer {
    let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(
        InMemoryStore::new(),
    ))));
    // PA 账号（p1: alice）
    {
        let mut g = sm.write().unwrap();
        g.apply(
            &Command::ProjectCreate {
                name: "p1".to_string(),
                operator: String::new(),
                ts: 0,
                clone_from: None,
            },
            1,
        )
        .unwrap();
        g.apply(
            &Command::ProjectAdminCreate {
                project: ProjectId("p1".into()),
                username: "alice".into(),
                salt: "s1".into(),
                password_hash: token_hash("s1alicepw"),
                ts: 0,
            },
            2,
        )
        .unwrap();
    }
    let state = ApiState::new(
        sm,
        WatchHub::new(),
        None,
        None,
        None,
        std::time::Duration::from_secs(86400),
        "admin-pw".into(),
        None,
    );
    let app = build_router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    TestServer {
        base: format!("http://{addr}"),
        _state: state,
    }
}

async fn req(
    base: &str,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<serde_json::Value>,
) -> (u16, serde_json::Value) {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let mut r = client.request(
        reqwest::Method::from_bytes(method.as_bytes()).unwrap(),
        format!("{base}{path}"),
    );
    if let Some(t) = token {
        r = r.bearer_auth(t);
    }
    if let Some(b) = body {
        r = r.json(&b);
    }
    let resp = r.send().await.unwrap();
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    let json = serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text));
    (status, json)
}

async fn admin_login(base: &str) -> String {
    let (code, body) = req(
        base,
        "POST",
        "/api/v1/login",
        None,
        Some(serde_json::json!({"password": "admin-pw"})),
    )
    .await;
    assert_eq!(code, 200, "admin login: {body}");
    body["token"].as_str().unwrap().to_string()
}

/// 源项目结构：string(required+description) / secret / shared 引用项 / int。
/// 注：ItemDef 的 required/secret 恒序列化（false 也输出）、shared 仅 true 输出——
/// 期望值与 GET /structure 往返后的实际形状一致。
fn src_groups() -> serde_json::Value {
    serde_json::json!([
        {
            "name": "app",
            "items": [
                { "key": "host", "type": "string", "required": true, "secret": false, "description": "主机" },
                { "key": "token", "type": "secret", "required": false, "secret": true },
                { "key": "timeout", "type": "int", "required": false, "secret": false, "shared": true },
                { "key": "port", "type": "int", "required": false, "secret": false }
            ]
        },
        { "name": "cache", "items": [ { "key": "ttl", "type": "int", "required": false, "secret": false } ] }
    ])
}

/// 建源项目 src 并发布结构（admin token）。
async fn setup_src_project(base: &str, admin: &str) {
    let (code, body) = req(
        base,
        "POST",
        "/api/v1/projects",
        Some(admin),
        Some(serde_json::json!({ "name": "src" })),
    )
    .await;
    assert_eq!(code, 200, "create src: {body}");
    let (code, body) = req(
        base,
        "PUT",
        "/api/v1/projects/src/structure-draft",
        Some(admin),
        Some(serde_json::json!({ "base_version": 1, "groups": src_groups() })),
    )
    .await;
    assert_eq!(code, 200, "src structure-draft: {body}");
    let (code, body) = req(
        base,
        "POST",
        "/api/v1/projects/src/structure-draft/publish",
        Some(admin),
        Some(serde_json::json!({ "comment": "init", "request_id": "r-src-1" })),
    )
    .await;
    assert_eq!(code, 200, "src structure publish: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_clone_project_structure() {
    let s = start().await;
    let admin = admin_login(&s.base).await;
    setup_src_project(&s.base, &admin).await;

    // 克隆创建
    let (code, body) = req(
        &s.base,
        "POST",
        "/api/v1/projects",
        Some(&admin),
        Some(serde_json::json!({ "name": "dst", "clone_from": "src" })),
    )
    .await;
    assert_eq!(code, 200, "clone create: {body}");
    assert_eq!(body["id"], "dst");
    assert_eq!(body["branches"], serde_json::json!(["dev", "test", "prod"]));

    // 已发布结构逐项等于源（version=1 落地）
    let (code, body) = req(
        &s.base,
        "GET",
        "/api/v1/projects/dst/structure",
        Some(&admin),
        None,
    )
    .await;
    assert_eq!(code, 200, "dst structure: {body}");
    assert_eq!(body["version"], 1);
    assert_eq!(body["groups"], src_groups(), "克隆结构与源逐项一致");

    // 无结构草稿（未人工编辑过）
    let (code, body) = req(
        &s.base,
        "GET",
        "/api/v1/projects/dst/structure-draft",
        Some(&admin),
        None,
    )
    .await;
    assert_eq!(code, 200, "dst structure-draft: {body}");
    assert_eq!(body["base_version"], serde_json::Value::Null);
    assert_eq!(body["groups"], serde_json::json!([]));

    // 分支照旧创建且无版本记录
    let (code, body) = req(
        &s.base,
        "GET",
        "/api/v1/projects/dst/branches",
        Some(&admin),
        None,
    )
    .await;
    assert_eq!(code, 200, "dst branches: {body}");
    let names: Vec<&str> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["name"].as_str().unwrap())
        .collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(
        sorted,
        vec!["dev", "prod", "test"],
        "默认 dev/test/prod 分支"
    );
    for b in body.as_array().unwrap() {
        assert_eq!(b["active_version"], 0, "分支 {} 无版本", b["name"]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clone_negative_cases() {
    let s = start().await;
    let admin = admin_login(&s.base).await;
    setup_src_project(&s.base, &admin).await;

    // 源不存在 → 422（ErrorKind::Validation）
    let (code, body) = req(
        &s.base,
        "POST",
        "/api/v1/projects",
        Some(&admin),
        Some(serde_json::json!({ "name": "e1", "clone_from": "nope" })),
    )
    .await;
    assert_eq!(code, 422, "clone source not found: {body}");

    // 自克隆（新项目尚不存在 → validation 而非 conflict）
    let (code, body) = req(
        &s.base,
        "POST",
        "/api/v1/projects",
        Some(&admin),
        Some(serde_json::json!({ "name": "e2", "clone_from": "e2" })),
    )
    .await;
    assert_eq!(code, 422, "self clone: {body}");

    // 非法源名 → 422
    let (code, body) = req(
        &s.base,
        "POST",
        "/api/v1/projects",
        Some(&admin),
        Some(serde_json::json!({ "name": "e3", "clone_from": "a/b" })),
    )
    .await;
    assert_eq!(code, 422, "invalid clone source: {body}");

    // 失败后项目未创建
    let (code, body) = req(&s.base, "GET", "/api/v1/projects/e1", Some(&admin), None).await;
    assert_eq!(code, 404, "failed clone must not create project: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clone_empty_string_and_plain_create_unchanged() {
    let s = start().await;
    let admin = admin_login(&s.base).await;
    setup_src_project(&s.base, &admin).await;

    // clone_from 空串 → 归一 None（普通创建，空结构）
    let (code, body) = req(
        &s.base,
        "POST",
        "/api/v1/projects",
        Some(&admin),
        Some(serde_json::json!({ "name": "blank", "clone_from": "" })),
    )
    .await;
    assert_eq!(code, 200, "blank clone_from: {body}");
    let (code, body) = req(
        &s.base,
        "GET",
        "/api/v1/projects/blank/structure",
        Some(&admin),
        None,
    )
    .await;
    assert_eq!(code, 200, "blank structure: {body}");
    assert_eq!(body["version"], 1);
    assert_eq!(
        body["groups"],
        serde_json::json!([]),
        "空串 = 空结构普通创建"
    );

    // 普通创建（不传 clone_from）行为不变
    let (code, body) = req(
        &s.base,
        "POST",
        "/api/v1/projects",
        Some(&admin),
        Some(serde_json::json!({ "name": "plain" })),
    )
    .await;
    assert_eq!(code, 200, "plain create: {body}");
    let (code, body) = req(
        &s.base,
        "GET",
        "/api/v1/projects/plain/structure",
        Some(&admin),
        None,
    )
    .await;
    assert_eq!(code, 200, "plain structure: {body}");
    assert_eq!(body["groups"], serde_json::json!([]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clone_from_empty_structure_source_ok() {
    let s = start().await;
    let admin = admin_login(&s.base).await;
    // src2 从未发布结构（空 groups）
    let (code, body) = req(
        &s.base,
        "POST",
        "/api/v1/projects",
        Some(&admin),
        Some(serde_json::json!({ "name": "src2" })),
    )
    .await;
    assert_eq!(code, 200, "create src2: {body}");
    // 克隆空结构源 → 等价普通创建
    let (code, body) = req(
        &s.base,
        "POST",
        "/api/v1/projects",
        Some(&admin),
        Some(serde_json::json!({ "name": "dst2", "clone_from": "src2" })),
    )
    .await;
    assert_eq!(code, 200, "clone empty source: {body}");
    let (code, body) = req(
        &s.base,
        "GET",
        "/api/v1/projects/dst2/structure",
        Some(&admin),
        None,
    )
    .await;
    assert_eq!(code, 200);
    assert_eq!(body["version"], 1);
    assert_eq!(body["groups"], serde_json::json!([]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pa_cannot_create_project_with_clone() {
    let s = start().await;
    // PA 登录（p1: alice）
    let (code, body) = req(
        &s.base,
        "POST",
        "/api/v1/login",
        None,
        Some(serde_json::json!({"username": "alice", "password": "alicepw"})),
    )
    .await;
    assert_eq!(code, 200, "pa login: {body}");
    let pa = body["token"].as_str().unwrap().to_string();
    // 含 clone_from 的创建同样 403（权限矩阵不变）
    let (code, body) = req(
        &s.base,
        "POST",
        "/api/v1/projects",
        Some(&pa),
        Some(serde_json::json!({ "name": "pa-proj", "clone_from": "p1" })),
    )
    .await;
    assert_eq!(code, 403, "PA 创建项目（含克隆）必须 403: {body}");
    assert_eq!(body["code"], "ERR_FORBIDDEN");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clone_create_is_audited_with_source() {
    let s = start().await;
    let admin = admin_login(&s.base).await;
    setup_src_project(&s.base, &admin).await;
    let (code, body) = req(
        &s.base,
        "POST",
        "/api/v1/projects",
        Some(&admin),
        Some(serde_json::json!({ "name": "audit-dst", "clone_from": "src" })),
    )
    .await;
    assert_eq!(code, 200, "clone create: {body}");
    // 审计含 clone_from 来源（可追溯；audit_list 返回裸数组）
    let (code, body) = req(&s.base, "GET", "/api/v1/audit?limit=50", Some(&admin), None).await;
    assert_eq!(code, 200, "audit: {body}");
    let entries = body.as_array().expect("audit 返回数组");
    let create = entries
        .iter()
        .find(|e| e["action"] == "project_create" && e["project"] == "audit-dst");
    assert!(
        create.is_some(),
        "审计应含 project_create/audit-dst: {body}"
    );
    assert_eq!(create.unwrap()["detail"]["clone_from"], "src");
}
