//! 项目访问令牌（Project Token）HTTP 集成测试。
//! 设计文档: dev_docs/design/project-token.md §3。

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
    {
        let mut g = sm.write().unwrap();
        for name in ["p1", "p2"] {
            g.apply(
                &Command::ProjectCreate {
                    name: name.to_string(),
                    operator: String::new(),
                    ts: 0,
                },
                1,
            )
            .unwrap();
        }
        // PA 账号（alice 只管 p1）：验证 tokens 端点对 PA 403
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

async fn pa_login(base: &str) -> String {
    let (code, body) = req(
        base,
        "POST",
        "/api/v1/login",
        None,
        Some(serde_json::json!({"username": "alice", "password": "alicepw"})),
    )
    .await;
    assert_eq!(code, 200, "pa login: {body}");
    body["token"].as_str().unwrap().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_list_revoke_lifecycle() {
    let ts = start().await;
    let admin = admin_login(&ts.base).await;

    // 创建：201，明文仅此一次
    let (code, body) = req(
        &ts.base,
        "POST",
        "/api/v1/projects/p1/tokens",
        Some(&admin),
        Some(serde_json::json!({"name": "svc-a"})),
    )
    .await;
    assert_eq!(code, 201, "create: {body}");
    let raw = body["token"].as_str().unwrap().to_string();
    let id = body["id"].as_str().unwrap().to_string();
    assert_eq!(id.len(), 16);
    assert_eq!(body["name"], "svc-a");

    // 列表：含 id/name/revoked，不含 hash/token 明文
    let (code, body) = req(
        &ts.base,
        "GET",
        "/api/v1/projects/p1/tokens",
        Some(&admin),
        None,
    )
    .await;
    assert_eq!(code, 200, "list: {body}");
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["id"], id);
    assert_eq!(arr[0]["name"], "svc-a");
    assert_eq!(arr[0]["revoked"], false);
    assert!(arr[0].get("token").is_none());
    assert!(arr[0].get("hash").is_none());

    // 数据面联动：正确 token 通过鉴权（非 401；具体内容取决于项目结构，不断言）
    let (code, body) = req(
        &ts.base,
        "GET",
        "/v1/projects/p1/branches/dev/snapshot",
        Some(&raw),
        None,
    )
    .await;
    assert_ne!(code, 401, "data-plane with token should pass auth: {body}");

    // 吊销：204；重复吊销幂等 204
    let (code, _) = req(
        &ts.base,
        "DELETE",
        &format!("/api/v1/projects/p1/tokens/{id}"),
        Some(&admin),
        None,
    )
    .await;
    assert_eq!(code, 204, "revoke");
    let (code, _) = req(
        &ts.base,
        "DELETE",
        &format!("/api/v1/projects/p1/tokens/{id}"),
        Some(&admin),
        None,
    )
    .await;
    assert_eq!(code, 204, "revoke idempotent");

    // 吊销后数据面 401
    let (code, _) = req(
        &ts.base,
        "GET",
        "/v1/projects/p1/branches/dev/snapshot",
        Some(&raw),
        None,
    )
    .await;
    assert_eq!(code, 401, "revoked token rejected");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_dup_name_conflict() {
    let ts = start().await;
    let admin = admin_login(&ts.base).await;
    let (code, _) = req(
        &ts.base,
        "POST",
        "/api/v1/projects/p1/tokens",
        Some(&admin),
        Some(serde_json::json!({"name": "svc-a"})),
    )
    .await;
    assert_eq!(code, 201);
    let (code, body) = req(
        &ts.base,
        "POST",
        "/api/v1/projects/p1/tokens",
        Some(&admin),
        Some(serde_json::json!({"name": "svc-a"})),
    )
    .await;
    assert_eq!(code, 409, "dup name: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revoke_missing_token_404() {
    let ts = start().await;
    let admin = admin_login(&ts.base).await;
    let (code, _) = req(
        &ts.base,
        "DELETE",
        "/api/v1/projects/p1/tokens/deadbeefdeadbeef",
        Some(&admin),
        None,
    )
    .await;
    assert_eq!(code, 404);
}

/// 数据面 token 化回归：Admin UI 配置预览用管理会话访问 /v1/.../config（掩码渲染）。
/// 会话豁免：Admin 全项目 200；PA 仅自己项目 200；PA 跨项目 → 401（无项目 token）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_render_session_exemption() {
    let ts = start().await;
    let admin = admin_login(&ts.base).await;
    let pa = pa_login(&ts.base).await;
    // Admin 会话访问任意项目掩码渲染 → 鉴权通过（非 401；测试项目无 dev 分支 → 404 属内容缺失）
    let (code, _) = req(
        &ts.base,
        "GET",
        "/v1/projects/p1/branches/dev/config?format=env",
        Some(&admin),
        None,
    )
    .await;
    assert_ne!(
        code, 401,
        "admin session can render masked config, got {code}"
    );
    // PA 会话访问自己项目 → 鉴权通过
    let (code, _) = req(
        &ts.base,
        "GET",
        "/v1/projects/p1/branches/dev/config?format=yaml",
        Some(&pa),
        None,
    )
    .await;
    assert_ne!(
        code, 401,
        "PA session can render own project config, got {code}"
    );
    // PA 会话访问其他项目 → 401（会话豁免拒绝 + 无项目 token）
    let (code, _) = req(
        &ts.base,
        "GET",
        "/v1/projects/p2/branches/dev/config?format=yaml",
        Some(&pa),
        None,
    )
    .await;
    assert_eq!(code, 401, "PA cross-project render denied");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn project_admin_forbidden() {
    let ts = start().await;
    let pa = pa_login(&ts.base).await;
    // POST / GET / DELETE 全部 403
    let (code, _) = req(
        &ts.base,
        "POST",
        "/api/v1/projects/p1/tokens",
        Some(&pa),
        Some(serde_json::json!({"name": "x"})),
    )
    .await;
    assert_eq!(code, 403);
    let (code, _) = req(
        &ts.base,
        "GET",
        "/api/v1/projects/p1/tokens",
        Some(&pa),
        None,
    )
    .await;
    assert_eq!(code, 403);
    let (code, _) = req(
        &ts.base,
        "DELETE",
        "/api/v1/projects/p1/tokens/aaaaaaaaaaaaaaaa",
        Some(&pa),
        None,
    )
    .await;
    assert_eq!(code, 403);
}
