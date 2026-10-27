//! 全局管理员（Global Admin）HTTP 集成测试（设计 global-admin §13 / 计划 §5）。
//!
//! 覆盖：创建/列表/重复、GA 登录与防枚举、**授权矩阵逐行**（允许项含灰度两轮与 rollback、
//! 自建分支删除、logout 置尾；拒绝项含全局面）、路径绕过与空段守卫、token 负形、
//! 自助改密与登出有效性、super 重置/删除 GA、恢复通道（ops token）会话范围、
//! super 改密不影响 GA 会话、force-logout 三步、GA↔PA 大小写互斥、审计 operator、
//! /audit 不过滤语义、密码下限与保留名。
//!
//! 夹具共三个：start_a（单节点内存，raft=None）；start_join_token（内存 + 已配置
//! --join-token）；start_raft_with_join_token（真 raft + join-token，仅 join 负形用）。
//! **每个 `#[tokio::test]` 各自起一个服务器**：登录节流是 per-ApiState 计数，
//! 共用夹具会把后续 401 断言变成 429。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use dsh_api::{build_router, ApiState};
use dsh_core::command::Command;
use dsh_core::model::ProjectId;
use dsh_core::{token_hash, InMemoryStore, StateMachine};
use dsh_watch::WatchHub;

const ADMIN_PW: &str = "admin-pw";
const GA_PW: &str = "pw12345";
const PA_PW: &str = "alicepw";

static SEQ: AtomicU64 = AtomicU64::new(0);

struct TestServer {
    base: String,
    _state: ApiState,
}

/// 夹具 (a)：单节点内存（raft=None）+ 项目 p1/p2 + PA alice（口令 alicepw）。
async fn start_a() -> TestServer {
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
                    clone_from: None,
                },
                1,
            )
            .unwrap();
        }
        // PA 口令是 "alicepw"（seed 写的是 salt:"s1" + token_hash("s1alicepw")，
        // 而 legacy 校验算 token_hash(salt||password)）
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
        Duration::from_secs(86400),
        ADMIN_PW.into(),
        None,
    );
    let app = build_router(state.clone());
    start_server(state, app).await
}

/// 夹具 (b)：内存 + 已配置 --join-token（恢复通道）；**自带 seed**（start_with_join_token 不 seed）。
async fn start_join_token() -> TestServer {
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
                    clone_from: None,
                },
                1,
            )
            .unwrap();
        }
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
    let state = ApiState::with_retention(
        sm,
        WatchHub::new(),
        None,
        None,
        None,
        Duration::from_secs(86400),
        ADMIN_PW.into(),
        None,
        0,
        0,
        Some(Arc::from("ops-secret")),
        Arc::new(dsh_api::TrustedProxies::empty()),
        None,
    );
    let app = build_router(state.clone());
    start_server(state, app).await
}

/// 夹具 (c)：单节点**真 raft** + 已配置 join-token（仅第 6 条的 join 负形用）。
async fn start_raft_with_join_token() -> TestServer {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("dsh-ga-join-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let storage = dsh_storage::RedbStorage::open(&dir.display().to_string()).unwrap();
    let db = storage.raw_db();
    let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage))));
    let sm_store = Arc::new(dsh_raft::StateMachineStore::new(sm.clone(), db.clone()));
    let log_store = dsh_raft::LogStore::new(db.clone());
    let network = dsh_raft::NetworkFactory::new();
    let node = dsh_raft::NodeInfo {
        grpc_addr: "127.0.0.1:8009".into(),
        http_addr: "127.0.0.1:9009".into(),
        raft_addr: "127.0.0.1:7009".into(),
    };
    let raft = dsh_raft::new_raft_node(
        1,
        node.clone(),
        log_store,
        sm_store,
        &network,
        dsh_raft::dev_config(),
    )
    .await
    .unwrap();
    network.register(1, raft.clone());
    dsh_raft::initialize_single(&raft, 1, node.clone())
        .await
        .unwrap();
    assert!(
        dsh_raft::wait_for_leader(&raft, Duration::from_secs(5))
            .await
            .is_some(),
        "node1 应成为 leader"
    );
    let state = ApiState::with_retention(
        sm,
        WatchHub::new(),
        Some(raft),
        Some(1),
        None,
        Duration::from_secs(86400),
        ADMIN_PW.into(),
        None,
        0,
        0,
        Some(Arc::from("ops-secret")),
        Arc::new(dsh_api::TrustedProxies::empty()),
        None,
    );
    let app = build_router(state.clone());
    start_server(state, app).await
}

async fn start_server(state: ApiState, app: axum::Router) -> TestServer {
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

/// super 登录（空 username）。
async fn super_login(base: &str) -> String {
    let (code, body) = req(
        base,
        "POST",
        "/api/v1/login",
        None,
        Some(serde_json::json!({"password": ADMIN_PW})),
    )
    .await;
    assert_eq!(code, 200, "super 登录: {body}");
    body["token"].as_str().unwrap().to_string()
}

/// 经 HTTP 建 GA（super token），返回状态码。
async fn create_ga(base: &str, super_t: &str, username: &str, pw: &str) -> u16 {
    let (code, _) = req(
        base,
        "POST",
        "/api/v1/admins",
        Some(super_t),
        Some(serde_json::json!({"username": username, "password": pw})),
    )
    .await;
    code
}

/// GA 登录，返回 (状态码, token/错误体)。
async fn ga_login(base: &str, username: &str, pw: &str) -> (u16, serde_json::Value) {
    req(
        base,
        "POST",
        "/api/v1/login",
        None,
        Some(serde_json::json!({"username": username, "password": pw})),
    )
    .await
}

/// 把某项目 seed 到「dev 已发布 + 有一份新草稿」（GA token 发出）。
async fn seed_project(base: &str, ga_t: &str, pid: &str) {
    let (code, body) = req(
        base,
        "PUT",
        &format!("/api/v1/projects/{pid}/structure-draft"),
        Some(ga_t),
        Some(serde_json::json!({
            "base_version": 1,
            "groups": [{"name": "g", "items": [{"key": "k", "type": "string", "required": true}]}]
        })),
    )
    .await;
    assert_eq!(code, 200, "structure-draft({pid}): {body}");
    let (code, body) = req(
        base,
        "POST",
        &format!("/api/v1/projects/{pid}/structure-draft/publish"),
        Some(ga_t),
        Some(serde_json::json!({"comment": "init"})),
    )
    .await;
    assert_eq!(code, 200, "structure publish({pid}): {body}");
    write_draft(base, ga_t, pid, "v1").await;
    let (code, body) = req(
        base,
        "POST",
        &format!("/api/v1/projects/{pid}/branches/dev/publish"),
        Some(ga_t),
        Some(serde_json::json!({"comment": "p"})),
    )
    .await;
    assert_eq!(code, 200, "branch publish({pid}): {body}");
    // publish 会清空值草稿 ⇒ 灰度断言前必须重新落一份草稿
    write_draft(base, ga_t, pid, "v2").await;
}

async fn write_draft(base: &str, token: &str, pid: &str, val: &str) {
    let (code, body) = req(
        base,
        "PUT",
        &format!("/api/v1/projects/{pid}/branches/dev/draft"),
        Some(token),
        Some(serde_json::json!({
            "updates": [{"group": "g", "key": "k", "value": {"type": "string", "str_value": val}}],
            "deletes": []
        })),
    )
    .await;
    assert_eq!(code, 200, "draft({pid}): {body}");
}

// ---------------------------------------------------------------- 1 + 2

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ga_create_login_list_and_duplicate() {
    let s = start_a().await;
    let sup = super_login(&s.base).await;

    let (code, body) = req(
        &s.base,
        "POST",
        "/api/v1/admins",
        Some(&sup),
        Some(serde_json::json!({"username": "ga1", "password": GA_PW})),
    )
    .await;
    assert_eq!(code, 201, "创建 GA: {body}");
    assert_eq!(body["username"], "ga1");

    // 列表：不含 salt / password_hash
    let (code, list) = req(&s.base, "GET", "/api/v1/admins", Some(&sup), None).await;
    assert_eq!(code, 200);
    let arr = list.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["username"], "ga1");
    assert!(arr[0].get("salt").is_none(), "列表不得返回 salt");
    assert!(
        arr[0].get("password_hash").is_none(),
        "列表不得返回 password_hash"
    );

    // 重复创建 → 409 ERR_ACCOUNT_EXISTS
    let (code, body) = req(
        &s.base,
        "POST",
        "/api/v1/admins",
        Some(&sup),
        Some(serde_json::json!({"username": "ga1", "password": GA_PW})),
    )
    .await;
    assert_eq!(code, 409, "重复创建: {body}");

    // GA 登录：role=global_admin + ga.ga1. 前缀 + username 回填
    let (code, body) = ga_login(&s.base, "ga1", GA_PW).await;
    assert_eq!(code, 200, "GA 登录: {body}");
    assert_eq!(body["role"], "global_admin");
    assert_eq!(body["username"], "ga1");
    let tok = body["token"].as_str().unwrap();
    assert!(tok.starts_with("ga.ga1."), "token 前缀: {tok}");

    // 错密码 → 401 ERR_BAD_CREDENTIALS（与不存在账号同码同文案，防枚举）
    let (code, body) = ga_login(&s.base, "ga1", "wrong-pw").await;
    assert_eq!(code, 401);
    assert_eq!(body["code"], "ERR_BAD_CREDENTIALS");
    let (code2, body2) = ga_login(&s.base, "ghost", GA_PW).await;
    assert_eq!(code2, 401);
    assert_eq!(body2["code"], "ERR_BAD_CREDENTIALS");
    assert_eq!(body["message"], body2["message"], "不得泄露账号是否存在");

    // login_failed 审计存在
    let (_, audit) = req(&s.base, "GET", "/api/v1/audit", Some(&sup), None).await;
    assert!(
        audit
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["action"] == "login_failed"),
        "应有 login_failed 审计"
    );
}

// ---------------------------------------------------------------- 3

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ga_allowed_matrix_with_gray_and_rollback() {
    let s = start_a().await;
    let sup = super_login(&s.base).await;
    assert_eq!(create_ga(&s.base, &sup, "ga1", GA_PW).await, 201);
    let (_, body) = ga_login(&s.base, "ga1", GA_PW).await;
    let ga = body["token"].as_str().unwrap().to_string();

    // seed 两个项目（全部用 GA token —— seed 成功本身即覆盖 /structure-draft 的 GA 可写面）
    seed_project(&s.base, &ga, "p1").await;
    seed_project(&s.base, &ga, "p2").await;

    // 项目详情（任意项目）+ 结构读取
    for pid in ["p1", "p2"] {
        let (c, b) = req(
            &s.base,
            "GET",
            &format!("/api/v1/projects/{pid}"),
            Some(&ga),
            None,
        )
        .await;
        assert_eq!(c, 200, "GET /projects/{pid}: {b}");
        let (c, b) = req(
            &s.base,
            "GET",
            &format!("/api/v1/projects/{pid}/structure"),
            Some(&ga),
            None,
        )
        .await;
        assert_eq!(c, 200, "GET structure: {b}");
    }
    // 分支列表/对比/版本/promote
    let (c, _) = req(
        &s.base,
        "GET",
        "/api/v1/projects/p2/branches",
        Some(&ga),
        None,
    )
    .await;
    assert_eq!(c, 200);
    let (c, b) = req(
        &s.base,
        "GET",
        "/api/v1/projects/p2/diff?branch_a=dev&branch_b=test",
        Some(&ga),
        None,
    )
    .await;
    assert_eq!(c, 200, "diff: {b}");
    let (c, _) = req(
        &s.base,
        "GET",
        "/api/v1/projects/p2/branches/dev/versions",
        Some(&ga),
        None,
    )
    .await;
    assert_eq!(c, 200);
    let (c, b) = req(
        &s.base,
        "POST",
        "/api/v1/projects/p2/promote",
        Some(&ga),
        Some(serde_json::json!({"from": "dev", "to": "test"})),
    )
    .await;
    assert_eq!(c, 200, "promote: {b}");

    // branch_detail（GET /projects/{p}/branches/{b}）与 structure-draft 读取的显式 GA 放行断：
    // 不得只靠 seed_project 的隐调用面，避免 future 的 regression 里 these 两条 paths 里 lock in 200。
    // dev 分支已 publish（seed）；structure-draft 读取 empty 时 handler 里 200 里（base_version:null）。
    let (c, b) = req(
        &s.base,
        "GET",
        "/api/v1/projects/p2/branches/dev",
        Some(&ga),
        None,
    )
    .await;
    assert_eq!(c, 200, "branch_detail(GA 显 面): {b}");
    let (c, b) = req(
        &s.base,
        "GET",
        "/api/v1/projects/p2/structure-draft",
        Some(&ga),
        None,
    )
    .await;
    assert_eq!(c, 200, "structure-draft(GA 显 面): {b}");

    // admin_config（/api/v1 的 config）与数据面渲染端点（掩码 / reveal）
    let (c, b) = req(
        &s.base,
        "GET",
        "/api/v1/projects/p2/branches/dev/config",
        Some(&ga),
        None,
    )
    .await;
    assert_eq!(c, 200, "admin_config: {b}");
    let (c, _) = req(
        &s.base,
        "GET",
        "/v1/projects/p2/branches/dev/config?format=json",
        Some(&ga),
        None,
    )
    .await;
    assert_eq!(c, 200);
    let (c, _) = req(
        &s.base,
        "GET",
        "/v1/projects/p2/branches/dev/config?format=json&reveal=true",
        Some(&ga),
        None,
    )
    .await;
    assert_eq!(c, 200, "GA 可 reveal 任意项目");

    // 全量列表 / 全量审计 / 集群成员（无 raft → assert_ne!(403)，dev-single 为 404）
    let (c, list) = req(&s.base, "GET", "/api/v1/projects", Some(&ga), None).await;
    assert_eq!(c, 200);
    assert_eq!(list.as_array().unwrap().len(), 2, "GA 看到全部项目");
    let (c, _) = req(&s.base, "GET", "/api/v1/audit", Some(&ga), None).await;
    assert_eq!(c, 200);
    let (c, _) = req(&s.base, "GET", "/api/v1/cluster/members", Some(&ga), None).await;
    assert_ne!(c, 403, "无 raft 夹具下应为 404（集群模式为 200）");

    // 共享面：读 + 写 + 发布 + 删除
    let (c, _) = req(&s.base, "GET", "/api/v1/shared", Some(&ga), None).await;
    assert_eq!(c, 200);
    let (c, _) = req(&s.base, "GET", "/api/v1/shared-draft", Some(&ga), None).await;
    assert_eq!(c, 200);
    let (c, b) = req(
        &s.base,
        "POST",
        "/api/v1/shared",
        Some(&ga),
        Some(serde_json::json!({"key": "sk", "type": "string", "value": {"type": "string", "str_value": "sv"}, "secret": false, "required": false})),
    )
    .await;
    assert_eq!(c, 200, "GA 建共享草稿: {b}");
    let (c, b) = req(
        &s.base,
        "POST",
        "/api/v1/shared/publish",
        Some(&ga),
        Some(serde_json::json!({"comment": "sp"})),
    )
    .await;
    assert_eq!(c, 200, "GA 发布共享: {b}");
    let (c, _) = req(&s.base, "DELETE", "/api/v1/shared/sk", Some(&ga), None).await;
    assert_eq!(c, 204, "删除共享项应为 204");

    // 灰度：必须分两轮（promote 与 abort 互斥）
    write_draft(&s.base, &ga, "p2", "g1").await;
    let (c, b) = req(&s.base, "POST", "/api/v1/projects/p2/branches/dev/gray-publish", Some(&ga),
        Some(serde_json::json!({"rule": {"match_labels": [{"key": "zone", "value": "cn-north-1"}]}, "comment": "g1", "request_id": "ga-g1"}))).await;
    assert_eq!(c, 200, "gray-publish #1: {b}");
    let (c, _) = req(
        &s.base,
        "GET",
        "/api/v1/projects/p2/branches/dev/gray-status",
        Some(&ga),
        None,
    )
    .await;
    assert_eq!(c, 200);
    let (c, b) = req(
        &s.base,
        "POST",
        "/api/v1/projects/p2/branches/dev/gray-promote",
        Some(&ga),
        Some(serde_json::json!({"comment": "p", "request_id": "ga-p1"})),
    )
    .await;
    assert_eq!(c, 200, "gray-promote: {b}");

    // rollback（dev：active 已 ≥2，to_version=1 合法）
    let (c, b) = req(
        &s.base,
        "POST",
        "/api/v1/projects/p2/branches/dev/rollback",
        Some(&ga),
        Some(serde_json::json!({"to_version": 1, "comment": "rb"})),
    )
    .await;
    assert_eq!(c, 200, "rollback: {b}");

    // 第二轮灰度 → abort
    write_draft(&s.base, &ga, "p2", "g2").await;
    let (c, b) = req(&s.base, "POST", "/api/v1/projects/p2/branches/dev/gray-publish", Some(&ga),
        Some(serde_json::json!({"rule": {"percentage": 100}, "comment": "g2", "request_id": "ga-g2"}))).await;
    assert_eq!(c, 200, "gray-publish #2: {b}");
    let (c, b) = req(
        &s.base,
        "POST",
        "/api/v1/projects/p2/branches/dev/gray-abort",
        Some(&ga),
        Some(serde_json::json!({"comment": "a", "request_id": "ga-a1"})),
    )
    .await;
    assert_eq!(c, 200, "gray-abort: {b}");

    // 自建且未发布的分支：建 → 删 → 204（勿用 dev：已发布分支恒 409）
    let (c, b) = req(
        &s.base,
        "POST",
        "/api/v1/projects/p2/branches",
        Some(&ga),
        Some(serde_json::json!({"name": "tmp"})),
    )
    .await;
    assert_eq!(c, 200, "建自建分支: {b}");
    let (c, b) = req(
        &s.base,
        "DELETE",
        "/api/v1/projects/p2/branches/tmp",
        Some(&ga),
        None,
    )
    .await;
    assert_eq!(c, 204, "删自建分支应为 204: {b}");
    let (c, _) = req(
        &s.base,
        "DELETE",
        "/api/v1/projects/p2/branches/dev",
        Some(&ga),
        None,
    )
    .await;
    assert_eq!(c, 409, "已发布分支删除应为 409");

    // heartbeat 放前面；logout 唯一破坏性、置尾
    let (c, b) = req(&s.base, "POST", "/api/v1/heartbeat", Some(&ga), None).await;
    assert_eq!(c, 200, "heartbeat: {b}");
    let (c, _) = req(&s.base, "POST", "/api/v1/logout", Some(&ga), None).await;
    assert_eq!(c, 204, "logout 应为 204");
    let (c, _) = req(&s.base, "GET", "/api/v1/projects", Some(&ga), None).await;
    assert_eq!(c, 401, "登出后旧 token 必须 401");
}

// ---------------------------------------------------------------- 4

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ga_denied_matrix_is_403() {
    let s = start_a().await;
    let sup = super_login(&s.base).await;
    assert_eq!(create_ga(&s.base, &sup, "ga1", GA_PW).await, 201);
    let (_, body) = ga_login(&s.base, "ga1", GA_PW).await;
    let ga = body["token"].as_str().unwrap().to_string();
    seed_project(&s.base, &ga, "p1").await;

    let denied: Vec<(&str, &str, Option<serde_json::Value>)> = vec![
        ("GET", "/api/v1/admins", None),
        (
            "POST",
            "/api/v1/admins",
            Some(serde_json::json!({"username": "x", "password": "pw123456"})),
        ),
        (
            "PUT",
            "/api/v1/admins/ga2",
            Some(serde_json::json!({"password": "pw123456"})),
        ),
        ("DELETE", "/api/v1/admins/ga2", None),
        (
            "POST",
            "/api/v1/admin/set-password",
            Some(serde_json::json!({"password": "pw123456"})),
        ),
        (
            "POST",
            "/api/v1/admin/force-logout",
            Some(serde_json::json!({})),
        ),
        (
            "POST",
            "/api/v1/admin/rotate-master-key",
            Some(serde_json::json!({"new_key": "AAAA"})),
        ),
        ("GET", "/api/v1/admin/snapshot", None),
        ("GET", "/api/v1/admin/retention-status", None),
        (
            "POST",
            "/api/v1/cluster/promote",
            Some(serde_json::json!({"node_id": 2})),
        ),
        (
            "POST",
            "/api/v1/cluster/remove",
            Some(serde_json::json!({"node_id": 2})),
        ),
        (
            "POST",
            "/api/v1/projects",
            Some(serde_json::json!({"name": "nope"})),
        ),
        ("DELETE", "/api/v1/projects/p1", None),
        ("GET", "/api/v1/projects/p1/admins", None),
        (
            "POST",
            "/api/v1/projects/p1/admins",
            Some(serde_json::json!({"username": "bob", "password": "pw123456"})),
        ),
        (
            "PUT",
            "/api/v1/projects/p1/admins/alice",
            Some(serde_json::json!({"password": "pw123456"})),
        ),
        ("DELETE", "/api/v1/projects/p1/admins/alice", None),
        ("GET", "/api/v1/projects/p1/tokens", None),
        (
            "POST",
            "/api/v1/projects/p1/tokens",
            Some(serde_json::json!({"name": "t"})),
        ),
        ("DELETE", "/api/v1/projects/p1/tokens/abc", None),
    ];
    for (m, p, b) in denied {
        let (code, body) = req(&s.base, m, p, Some(&ga), b).await;
        assert_eq!(code, 403, "{m} {p} 应 403，实得 {code}: {body}");
        assert_eq!(body["message"], "无权访问该资源", "403 文案统一");
    }
}

// ---------------------------------------------------------------- 5

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ga_path_bypass_and_construction() {
    let s = start_a().await;
    let sup = super_login(&s.base).await;
    assert_eq!(create_ga(&s.base, &sup, "ga1", GA_PW).await, 201);
    let (_, body) = ga_login(&s.base, "ga1", GA_PW).await;
    let ga = body["token"].as_str().unwrap().to_string();

    // 全局拒绝面的写法变体：一律 403（中间件包住 fallback）
    for p in [
        "/api/v1/admins/",
        "/api/v1/ADMINS",
        "/api/v1//admins",
        "/api/v1/admin/set-password/",
    ] {
        let (code, body) = req(&s.base, "GET", p, Some(&ga), None).await;
        assert_ne!(code, 200, "不得因写法绕过: {p} → {code} {body}");
    }
    // 项目作用域拒绝面：编码/大写/尾斜杠/空段 → 403
    for p in [
        "/api/v1/projects/%70/admins",
        "/api/v1/projects/P1/admins",
        "/api/v1/projects/p1/admins/",
        "/api/v1/projects/p1//admins",
        "/api/v1/projects/p1/tokens/",
    ] {
        let (code, body) = req(&s.base, "GET", p, Some(&ga), None).await;
        assert_eq!(code, 403, "{p} 应 403，实得 {code}: {body}");
    }
    // `..` **不得断言 403**：reqwest/url 按 WHATWG 先归一化为 /api/v1/projects/admins，
    // project_segment 得到合法项目名 → GA 放行 → 命中 GET /projects/{p} → 项目不存在 → 404。
    // （已实测：断言 403 会红。要锁中间件的 `..` 守卫只能用不归一化的客户端。）
    let (code, body) = req(
        &s.base,
        "GET",
        "/api/v1/projects/p1/../admins",
        Some(&ga),
        None,
    )
    .await;
    assert_eq!(code, 404, "归一化后应为 404（非 403、非 200）: {body}");
    assert_ne!(code, 200, "无论如何不得放行到 admins 端点");
    // %61dmins：中间件放行、路由 404 兜底（**不得断言 403**，也不得是 200）
    let (code, body) = req(
        &s.base,
        "GET",
        "/api/v1/projects/p1/%61dmins",
        Some(&ga),
        None,
    )
    .await;
    assert_ne!(code, 200, "%61 编码不得命中 admins: {body}");
    assert_ne!(code, 403, "该条由路由 404 兜底");
}

// ---------------------------------------------------------------- 6

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ga_token_negative_forms() {
    let s = start_a().await;
    let sup = super_login(&s.base).await;
    assert_eq!(create_ga(&s.base, &sup, "ga1", GA_PW).await, 201);
    let (_, body) = ga_login(&s.base, "ga1", GA_PW).await;
    let real = body["token"].as_str().unwrap().to_string();

    for bad in [
        "ga.".to_string(),
        "ga.ga1".to_string(),
        "ga.ga1.sid".to_string(),
        format!("ga.ghost.{}", real.rsplit('.').next().unwrap()),
        real[..real.len() - 3].to_string(),
        format!("adm.{}", real.rsplit('.').next().unwrap()),
    ] {
        let (code, _) = req(&s.base, "GET", "/api/v1/projects", Some(&bad), None).await;
        assert_eq!(code, 401, "畸形 token 应 401: {bad}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_negative_needs_raft_and_join_token() {
    // 必须「真 raft + 已配置 --join-token」：无 raft 时路由不存在（404），
    // 未配 join-token 时 join_token_ok 直接放行（不会 401）
    let s = start_raft_with_join_token().await;
    let sup = super_login(&s.base).await;
    assert_eq!(create_ga(&s.base, &sup, "ga1", GA_PW).await, 201);
    let (code, body) = ga_login(&s.base, "ga1", GA_PW).await;
    assert_eq!(code, 200, "GA 登录（raft 夹具）: {body}");
    let ga = body["token"].as_str().unwrap().to_string();

    // 带 GA token 打 join：body 必须可解析（JoinReq.node_id 必填）
    let (code, body) = req(
        &s.base,
        "POST",
        "/api/v1/cluster/join",
        Some(&ga),
        Some(serde_json::json!({"node_id": 9, "http_addr": "127.0.0.1:9009", "raft_addr": "127.0.0.1:7009"})),
    )
    .await;
    assert_eq!(code, 401, "非 join token 应 401 ERR_UNAUTHORIZED: {body}");
    assert_eq!(body["code"], "ERR_UNAUTHORIZED");

    // 缺 body → 415/422（提取器先拒），也绝不是 200
    let (code, _) = req(&s.base, "POST", "/api/v1/cluster/join", Some(&ga), None).await;
    assert_ne!(code, 200);
    assert_ne!(code, 401, "缺 body 时提取器先拒，到不了 join_token_ok");
}

// ---------------------------------------------------------------- 7

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ga_self_password_change_and_logout_validity() {
    let s = start_a().await;
    let sup = super_login(&s.base).await;
    assert_eq!(create_ga(&s.base, &sup, "ga1", GA_PW).await, 201);
    let (_, b1) = ga_login(&s.base, "ga1", GA_PW).await;
    let t1 = b1["token"].as_str().unwrap().to_string();
    let (_, b2) = ga_login(&s.base, "ga1", GA_PW).await;
    let t2 = b2["token"].as_str().unwrap().to_string();

    // 字段名必须是 current_password/new_password
    let (c, b) = req(
        &s.base,
        "POST",
        "/api/v1/me/password",
        Some(&t1),
        Some(serde_json::json!({"current_password": GA_PW, "new_password": "pw123456"})),
    )
    .await;
    assert_eq!(c, 200, "自助改密: {b}");

    // 该账号全部会话失效；super 会话不受影响
    for t in [&t1, &t2] {
        let (c, _) = req(&s.base, "GET", "/api/v1/projects", Some(t), None).await;
        assert_eq!(c, 401, "改密后旧会话必须失效");
    }
    let (c, _) = req(&s.base, "GET", "/api/v1/projects", Some(&sup), None).await;
    assert_eq!(c, 200, "super 会话不受 GA 改密影响");

    // 新密码可登录
    let (c, b) = ga_login(&s.base, "ga1", "pw123456").await;
    assert_eq!(c, 200, "新密码登录: {b}");
    let fresh = b["token"].as_str().unwrap().to_string();

    // logout → 204；随后同一 token → 401（只断言 204 测不出「登出无效」）
    let (c, _) = req(&s.base, "POST", "/api/v1/logout", Some(&fresh), None).await;
    assert_eq!(c, 204);
    let (c, _) = req(&s.base, "GET", "/api/v1/projects", Some(&fresh), None).await;
    assert_eq!(c, 401);
}

// ---------------------------------------------------------------- 8 + 9 + 10

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn super_resets_ga_password_then_deletes_account() {
    let s = start_a().await;
    let sup = super_login(&s.base).await;
    assert_eq!(create_ga(&s.base, &sup, "ga1", GA_PW).await, 201);
    let (_, b) = ga_login(&s.base, "ga1", GA_PW).await;
    let old = b["token"].as_str().unwrap().to_string();

    // PUT /api/v1/admins/{u} → 200 {"changed":true}；该账号会话全失效
    let (c, body) = req(
        &s.base,
        "PUT",
        "/api/v1/admins/ga1",
        Some(&sup),
        Some(serde_json::json!({"password": "pw654321"})),
    )
    .await;
    assert_eq!(c, 200, "重置 GA 密码: {body}");
    assert_eq!(body["changed"], true);
    let (c, _) = req(&s.base, "GET", "/api/v1/projects", Some(&old), None).await;
    assert_eq!(c, 401, "重置后 GA 旧会话必须失效");
    let (c, _) = ga_login(&s.base, "ga1", GA_PW).await;
    assert_eq!(c, 401, "旧密码不可登录");
    let (c, _) = ga_login(&s.base, "ga1", "pw654321").await;
    assert_eq!(c, 200, "新密码可登录");

    // DELETE → 204；此后登录 401、列表不含
    let (c, _) = req(&s.base, "DELETE", "/api/v1/admins/ga1", Some(&sup), None).await;
    assert_eq!(c, 204);
    let (c, _) = ga_login(&s.base, "ga1", "pw654321").await;
    assert_eq!(c, 401);
    let (_, list) = req(&s.base, "GET", "/api/v1/admins", Some(&sup), None).await;
    assert!(list.as_array().unwrap().is_empty());
    // 删不存在 → 404
    let (c, body) = req(&s.base, "DELETE", "/api/v1/admins/ga1", Some(&sup), None).await;
    assert_eq!(c, 404, "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn super_password_change_keeps_ga_sessions() {
    let s = start_a().await;
    let sup = super_login(&s.base).await;
    assert_eq!(create_ga(&s.base, &sup, "ga1", GA_PW).await, 201);
    let (_, b) = ga_login(&s.base, "ga1", GA_PW).await;
    let ga = b["token"].as_str().unwrap().to_string();

    let (c, _) = req(
        &s.base,
        "POST",
        "/api/v1/admin/set-password",
        Some(&sup),
        Some(serde_json::json!({"password": "admin-pw2"})),
    )
    .await;
    assert_eq!(c, 200);

    // GA 会话存活（键空间隔离：MultiSessionLogoutAll 只清 sess/admin*）
    let (c, _) = req(&s.base, "GET", "/api/v1/projects", Some(&ga), None).await;
    assert_eq!(c, 200, "super 改密不得影响 GA 会话");
    // super 旧会话已失效
    let (c, _) = req(&s.base, "GET", "/api/v1/projects", Some(&sup), None).await;
    assert_eq!(c, 401);
}

// ---------------------------------------------------------------- 8b

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ops_token_reset_password_covers_super_and_ga_sessions() {
    let s = start_join_token().await;
    let sup = super_login(&s.base).await;
    assert_eq!(create_ga(&s.base, &sup, "ga1", GA_PW).await, 201);
    let (_, b) = ga_login(&s.base, "ga1", GA_PW).await;
    let ga = b["token"].as_str().unwrap().to_string();
    let (c, pb) = req(
        &s.base,
        "POST",
        "/api/v1/login",
        None,
        Some(serde_json::json!({"username": "alice", "password": PA_PW})),
    )
    .await;
    assert_eq!(c, 200, "PA 登录（口令 alicepw）: {pb}");
    let pa = pb["token"].as_str().unwrap().to_string();

    // body 必带 password（SetPasswordReq 无 serde default）
    let (c, body) = req(
        &s.base,
        "POST",
        "/api/v1/admin/reset-password",
        Some("ops-secret"),
        Some(serde_json::json!({"password": "pw123456"})),
    )
    .await;
    assert_eq!(c, 200, "恢复通道必须成功（否则后续断言假红）: {body}");
    assert_eq!(body["changed"], true);

    let (c, _) = req(&s.base, "GET", "/api/v1/projects", Some(&sup), None).await;
    assert_eq!(c, 401, "super 旧会话应被回收");
    let (c, _) = req(&s.base, "GET", "/api/v1/projects", Some(&ga), None).await;
    assert_eq!(c, 401, "GA 会话应被回收（任务 4.6b）");
    let (c, _) = req(&s.base, "GET", "/api/v1/projects", Some(&pa), None).await;
    assert_eq!(c, 200, "PA 会话不在该通道内");
}

// ---------------------------------------------------------------- 11

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn force_logout_three_steps() {
    let s = start_a().await;
    let sup = super_login(&s.base).await;
    assert_eq!(create_ga(&s.base, &sup, "ga1", GA_PW).await, 201);
    let (_, b) = ga_login(&s.base, "ga1", GA_PW).await;
    let ga = b["token"].as_str().unwrap().to_string();
    // PA 会话必须**在 {} 之前**登录，否则「PA 不受影响」恒真无鉴别力
    let (c, pb) = req(
        &s.base,
        "POST",
        "/api/v1/login",
        None,
        Some(serde_json::json!({"username": "alice", "password": PA_PW})),
    )
    .await;
    assert_eq!(c, 200, "PA 登录: {pb}");
    let pa = pb["token"].as_str().unwrap().to_string();

    // ① 单账号踢
    let (c, _) = req(
        &s.base,
        "POST",
        "/api/v1/admin/force-logout",
        Some(&sup),
        Some(serde_json::json!({"username": "ga1"})),
    )
    .await;
    assert_eq!(c, 200);
    let (c, _) = req(&s.base, "GET", "/api/v1/projects", Some(&ga), None).await;
    assert_eq!(c, 401, "GA 会话应被踢");
    let (_, list) = req(&s.base, "GET", "/api/v1/admins", Some(&sup), None).await;
    assert_eq!(list.as_array().unwrap().len(), 1, "踢会话不删账号");

    // ② {} → 连调用者自己的 super 会话一起清；PA 仍在
    let (c, _) = req(
        &s.base,
        "POST",
        "/api/v1/admin/force-logout",
        Some(&sup),
        Some(serde_json::json!({})),
    )
    .await;
    assert_eq!(c, 200);
    let (c, _) = req(&s.base, "GET", "/api/v1/projects", Some(&sup), None).await;
    assert_eq!(c, 401, "空对象请求体会清掉调用者自己的会话");
    let (c, _) = req(&s.base, "GET", "/api/v1/projects", Some(&pa), None).await;
    assert_eq!(c, 200, "PA 会话不在此列");

    // ③ 必须重新登录拿 t2 再测 admin 保留名 no-op
    let t2 = super_login(&s.base).await;
    let (c, body) = req(
        &s.base,
        "POST",
        "/api/v1/admin/force-logout",
        Some(&t2),
        Some(serde_json::json!({"username": "admin"})),
    )
    .await;
    assert_eq!(c, 200, "admin 是保留名 → 静默 no-op: {body}");
    let (c, _) = req(&s.base, "GET", "/api/v1/projects", Some(&t2), None).await;
    assert_eq!(c, 200, "no-op 之后 t2 仍有效");
}

// ---------------------------------------------------------------- 12

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ga_pa_mutual_exclusion_over_http_including_case() {
    let s = start_a().await;
    let sup = super_login(&s.base).await;
    assert_eq!(create_ga(&s.base, &sup, "ga1", GA_PW).await, 201);

    // super 建同名 PA（大小写变体也算占用）→ 409
    for name in ["ga1", "GA1"] {
        let (c, body) = req(
            &s.base,
            "POST",
            "/api/v1/projects/p1/admins",
            Some(&sup),
            Some(serde_json::json!({"username": name, "password": "pw123456"})),
        )
        .await;
        assert_eq!(c, 409, "PA {name} 应因 GA 占用而 409: {body}");
        assert_eq!(body["code"], "ERR_ACCOUNT_EXISTS");
    }
    // 反向：PA alice 存在 → 建 GA "Alice" → 409
    let (c, body) = req(
        &s.base,
        "POST",
        "/api/v1/admins",
        Some(&sup),
        Some(serde_json::json!({"username": "Alice", "password": GA_PW})),
    )
    .await;
    assert_eq!(c, 409, "GA Alice 应因 PA alice 占用而 409: {body}");

    // 负形：同一请求换成 GA token → 403（中间件先拦，body 无关）
    let (_, b) = ga_login(&s.base, "ga1", GA_PW).await;
    let ga = b["token"].as_str().unwrap().to_string();
    let (c, _) = req(
        &s.base,
        "POST",
        "/api/v1/projects/p1/admins",
        Some(&ga),
        Some(serde_json::json!({"username": "bob", "password": "pw123456"})),
    )
    .await;
    assert_eq!(c, 403);
}

// ---------------------------------------------------------------- 13 + 16

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_operator_attribution_and_unfiltered_visibility() {
    let s = start_a().await;
    let sup = super_login(&s.base).await;
    assert_eq!(create_ga(&s.base, &sup, "ga1", GA_PW).await, 201);
    let (_, b) = ga_login(&s.base, "ga1", GA_PW).await;
    let ga = b["token"].as_str().unwrap().to_string();

    seed_project(&s.base, &ga, "p2").await;
    let (c, _) = req(&s.base, "POST", "/api/v1/shared", Some(&ga),
        Some(serde_json::json!({"key": "sk2", "type": "string", "value": {"type": "string", "str_value": "sv"}, "secret": false, "required": false}))).await;
    assert_eq!(c, 200);

    // 继续以 GA token：发布共享草稿 + 删除共享项——两条写审计归因路径此前仅评审-guarded。
    // 审计读必须发生在这两写之后（保证 shared_publish / shared_delete 条目已落）。
    let (c, body) = req(
        &s.base,
        "POST",
        "/api/v1/shared/publish",
        Some(&ga),
        Some(serde_json::json!({"comment": "audit-sp"})),
    )
    .await;
    assert_eq!(c, 200, "GA 共享 publish: {body}");

    let (c, body) = req(&s.base, "DELETE", "/api/v1/shared/sk2", Some(&ga), None).await;
    assert_eq!(c, 204, "GA 共享 delete 应 204 (NO_CONTENT): {body}");

    let (_, audit) = req(&s.base, "GET", "/api/v1/audit", Some(&ga), None).await;
    let entries = audit.as_array().unwrap();

    // GA 的动作用 ga:ga1 记账（含共享 publish / delete 归因）
    for action in [
        "login",
        "publish",
        "shared_draft_update",
        "shared_publish",
        "shared_delete",
    ] {
        assert!(
            entries
                .iter()
                .any(|e| e["action"] == action && e["operator"] == "ga:ga1"),
            "action={action} 应以 ga:ga1 记账"
        );
    }
    // 建 GA 本身记为 admin
    assert!(
        entries
            .iter()
            .any(|e| e["action"] == "global_admin_create" && e["operator"] == "admin"),
        "global_admin_create 的 operator 应为 admin"
    );

    // GA 看到 p2 的条目（不强制过滤）——对照 PA 只看自己项目
    let (c, pb) = req(
        &s.base,
        "POST",
        "/api/v1/login",
        None,
        Some(serde_json::json!({"username": "alice", "password": PA_PW})),
    )
    .await;
    assert_eq!(c, 200);
    let pa = pb["token"].as_str().unwrap().to_string();
    let (_, pa_audit) = req(&s.base, "GET", "/api/v1/audit", Some(&pa), None).await;
    assert!(
        entries.iter().any(|e| e["project"] == "p2"),
        "GA 应能看到 p2 的审计条目"
    );
    assert!(
        pa_audit
            .as_array()
            .unwrap()
            .iter()
            .all(|e| e["project"].is_null() || e["project"] == "p1"),
        "PA 只能看到自己项目的条目"
    );
}

// ---------------------------------------------------------------- 17

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn password_floor_and_reserved_name() {
    let s = start_a().await;
    let sup = super_login(&s.base).await;

    // body 必须带 username（否则被 Json 提取器拒成纯文本 422，拿不到 ERR_VALIDATION）
    let (c, b) = req(
        &s.base,
        "POST",
        "/api/v1/admins",
        Some(&sup),
        Some(serde_json::json!({"username": "ga2", "password": "12345"})),
    )
    .await;
    assert_eq!(c, 422, "5 位密码: {b}");
    assert_eq!(b["code"], "ERR_VALIDATION");

    assert_eq!(create_ga(&s.base, &sup, "ga3", GA_PW).await, 201);
    let (c, b) = req(
        &s.base,
        "PUT",
        "/api/v1/admins/ga3",
        Some(&sup),
        Some(serde_json::json!({"password": "12345"})),
    )
    .await;
    assert_eq!(c, 422, "改密 5 位: {b}");
    assert_eq!(b["code"], "ERR_VALIDATION");

    // 保留名 admin 不区分大小写 → 422（不得 201）
    for name in ["admin", "Admin", "ADMIN"] {
        let (c, b) = req(
            &s.base,
            "POST",
            "/api/v1/admins",
            Some(&sup),
            Some(serde_json::json!({"username": name, "password": GA_PW})),
        )
        .await;
        assert_eq!(c, 422, "保留名 {name}: {b}");
    }
}

// ---------------------------------------------------------------- M5

/// M5: `change_my_password`(POST /api/v1/me/password) 按**主体**节流
/// (key = `me:{operator}`: super `me:admin`、GA `me:ga:{u}`、PA `me:pa:{u}`;
/// 窗口 600s / 上限 5 错误 → 429 `ERR_TOO_MANY_ATTEMPTS`;改密成功 → 清空计数)。
/// 独立 `start_a` 夹具: 节流是 per-ApiState 计数,与其余测试的夹具互相不受影响。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn me_password_current_password_throttled_per_subject() {
    let s = start_a().await;
    let sup = super_login(&s.base).await;
    assert_eq!(create_ga(&s.base, &sup, "ga1", GA_PW).await, 201);
    let (_, b) = ga_login(&s.base, "ga1", GA_PW).await;
    let ga = b["token"].as_str().unwrap().to_string();

    // ① GA 按主体 key(me:ga:ga1):第 1..5 次错误 current_password → 401
    for i in 1..=5 {
        let (c, b) = req(
            &s.base,
            "POST",
            "/api/v1/me/password",
            Some(&ga),
            Some(serde_json::json!({
                "current_password": "wrong-pw",
                "new_password": "newpw123",
            })),
        )
        .await;
        assert_eq!(
            c, 401,
            "GA 第 {i} 次错误 current_password 应 401(未达上限): {b}"
        );
        assert_eq!(
            b["code"], "ERR_BAD_CREDENTIALS",
            "GA 第 {i} 次错误 body 应 BAD_CREDENTIALS: {b}",
        );
    }

    // ② 第 6 次(**对** current_password)→ 429:窗口内 5 错已锁(节流先于密码校验)
    let (c, b) = req(
        &s.base,
        "POST",
        "/api/v1/me/password",
        Some(&ga),
        Some(serde_json::json!({
            "current_password": GA_PW,
            "new_password": "newpw123",
        })),
    )
    .await;
    assert_eq!(c, 429, "GA 第 6 次(对密码)应 429(窗口内 5 错已锁): {b}");
    assert_eq!(
        b["code"], "ERR_TOO_MANY_ATTEMPTS",
        "GA 第 6 次 body 应 TOO_MANY_ATTEMPTS: {b}",
    );

    // ③ 主体隔离:PA key(me:pa:alice)未受 GA 计数 → 401(不是 429)
    let (c, pb) = req(
        &s.base,
        "POST",
        "/api/v1/login",
        None,
        Some(serde_json::json!({"username": "alice", "password": PA_PW})),
    )
    .await;
    assert_eq!(c, 200, "PA alice 登录: {pb}");
    let pa = pb["token"].as_str().unwrap().to_string();
    let (c, b) = req(
        &s.base,
        "POST",
        "/api/v1/me/password",
        Some(&pa),
        Some(serde_json::json!({
            "current_password": "wrong-pw",
            "new_password": "newpw123",
        })),
    )
    .await;
    assert_eq!(
        c, 401,
        "PA 第 1 次错误应 401(GA 计数不得触 PA key,不是 429): {b}"
    );
    assert_eq!(
        b["code"], "ERR_BAD_CREDENTIALS",
        "PA 第 1 次错误 body 应 BAD_CREDENTIALS: {b}",
    );

    // super key(me:admin)同样独立 → 401(不是 429)
    let (c, b) = req(
        &s.base,
        "POST",
        "/api/v1/me/password",
        Some(&sup),
        Some(serde_json::json!({
            "current_password": "wrong-pw",
            "new_password": "newpw123",
        })),
    )
    .await;
    assert_eq!(
        c, 401,
        "super 第 1 次错误应 401(GA 计数不得触 super key,不是 429): {b}",
    );
    assert_eq!(
        b["code"], "ERR_BAD_CREDENTIALS",
        "super 第 1 次错误 body 应 BAD_CREDENTIALS: {b}",
    );

    // ④ 改密成功清空计数(独立主体 ga2,不受上面断言影响):
    //   4 错(未达上限)→ 对改密 200 → 后 2 错仍 401(成功已清空;否则第 2 会 429)
    assert_eq!(create_ga(&s.base, &sup, "ga2", GA_PW).await, 201, "ga2 建");
    let (_, b2) = ga_login(&s.base, "ga2", GA_PW).await;
    let g2 = b2["token"].as_str().unwrap().to_string();
    for i in 1..=4 {
        let (c, b) = req(
            &s.base,
            "POST",
            "/api/v1/me/password",
            Some(&g2),
            Some(serde_json::json!({
                "current_password": "wrong-pw",
                "new_password": "newpw123",
            })),
        )
        .await;
        assert_eq!(c, 401, "ga2 第 {i} 次错误应 401(4 错未锁): {b}");
    }
    let (c, b) = req(
        &s.base,
        "POST",
        "/api/v1/me/password",
        Some(&g2),
        Some(serde_json::json!({
            "current_password": GA_PW,
            "new_password": "pw999999",
        })),
    )
    .await;
    assert_eq!(c, 200, "ga2 4 错 + 对改密应 200(未达上限): {b}");
    assert_eq!(b["changed"], true, "ga2 改密 body 应 changed=true: {b}");

    // 改密即重登: ga2 旧会话失效
    let (c, _) = req(&s.base, "GET", "/api/v1/projects", Some(&g2), None).await;
    assert_eq!(c, 401, "ga2 改密后旧会话应失效");

    // 重新登录(新口令)+ 第 1..2 次错误 → 2 应 401(成功已清空计数)
    let (c, b) = ga_login(&s.base, "ga2", "pw999999").await;
    assert_eq!(c, 200, "ga2 改密后新口令登录: {b}");
    let g3 = b["token"].as_str().unwrap().to_string();
    for i in 1..=2 {
        let (c, b) = req(
            &s.base,
            "POST",
            "/api/v1/me/password",
            Some(&g3),
            Some(serde_json::json!({
                "current_password": "wrong-pw",
                "new_password": "newpw123",
            })),
        )
        .await;
        assert_eq!(
            c, 401,
            "改密成功后第 {i} 次错误应 401(成功已清空计数,不是 429): {b}",
        );
    }
}
