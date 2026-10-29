//! P4-A A3：restore 失败后的 fail-stop 对外语义（HTTP）。
//!
//! dsh-raft 的 `StateMachineStore` 在 restore 失败后置位 poison；dsh-api 必须：
//! - `/api/v1/*`、`/v1/*` 一律 503（含鉴权前，避免用错误凭据试探混合状态）；
//! - `/healthz` 继续 200（liveness 探针不因 fail-stop 触发重启风暴）；
//! - `/readyz` 503（从 LB 摘除）；
//! - `linearized_read`（gRPC 数据面入口）返回 Err；
//! - 解除标记后恢复原语义（未鉴权 → 401，而不是 503）。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use dsh_api::{build_router, ApiState, POISONED_MESSAGE};
use dsh_core::{InMemoryStore, StateMachine};
use dsh_watch::WatchHub;

struct Server {
    base: String,
    poison: Arc<AtomicBool>,
    state: ApiState,
}

async fn start() -> Server {
    let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(
        InMemoryStore::new(),
    ))));
    let mut state = ApiState::new(
        sm,
        WatchHub::new(),
        None,
        None,
        None,
        std::time::Duration::from_secs(86400),
        "admin-pw".into(),
        None,
    );
    let poison = Arc::new(AtomicBool::new(true));
    state.set_poison(poison.clone());
    let app = build_router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server {
        base: format!("http://{addr}"),
        poison,
        state,
    }
}

async fn req(
    base: &str,
    method: &str,
    path: &str,
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
    if let Some(b) = body {
        r = r.json(&b);
    }
    let resp = r.send().await.unwrap();
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    let json = serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text));
    (status, json)
}

#[tokio::test]
async fn poisoned_node_returns_503_but_stays_live() {
    let s = start().await;

    // 管理面读：鉴权前就 503（不是 401）
    let (code, body) = req(&s.base, "GET", "/api/v1/projects", None).await;
    assert_eq!(code, 503, "poisoned 管理面读必须 503：{body}");
    assert_eq!(body["code"], "ERR_RESTORE_POISONED");
    assert_eq!(body["message"], POISONED_MESSAGE);

    // 管理面写/登录
    let (code, _) = req(
        &s.base,
        "POST",
        "/api/v1/login",
        Some(serde_json::json!({"password": "admin-pw"})),
    )
    .await;
    assert_eq!(code, 503);

    // 数据面
    let (code, _) = req(&s.base, "GET", "/v1/projects/p/branches/main/config", None).await;
    assert_eq!(code, 503);

    // liveness 必须继续成功；readiness 必须失败
    let (code, _) = req(&s.base, "GET", "/healthz", None).await;
    assert_eq!(code, 200);
    let (code, _) = req(&s.base, "GET", "/readyz", None).await;
    assert_eq!(code, 503);

    // gRPC 数据面入口（linearized_read）
    assert!(
        s.state.linearized_read().await.is_err(),
        "gRPC 读取入口在 poison 时必须拒绝"
    );

    // 解除后语义恢复：未鉴权 → 401（说明请求已到达鉴权层，不再被 fail-stop 门拦截）
    s.poison.store(false, Ordering::SeqCst);
    let (code, body) = req(&s.base, "GET", "/api/v1/projects", None).await;
    assert_eq!(code, 401, "解除后应回到正常鉴权语义：{body}");
    assert!(s.state.linearized_read().await.is_ok());
}
