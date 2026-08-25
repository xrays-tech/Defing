//! HTTP 管理面 + 数据面（模块 05 / 09）：axum 路由、鉴权中间件、错误映射、Admin UI。
//! 职责：请求/响应编解码、会话校验（I7）、写路径（经 dsh-raft::write_command）、
//! 渲染端点（模块 08）、数据面快照/watch（模块 06）。
//! 不做：状态机业务（dsh-core/dsh-publish）、事件扇出（dsh-watch）、可观测（dsh-observability）。

pub mod grpc;

/// 构建元数据（部署版本标记）：由 build.rs 注入 git 短哈希与构建时间（UNIX 秒）。
/// 暴露于 /healthz、/readyz 与 Admin UI 页脚；docker 构建可用
/// `--build-arg DEFING_GIT_COMMIT=$(git rev-parse --short HEAD)` 覆盖。
pub const BUILD_COMMIT: &str = env!("DEFING_GIT_COMMIT");
pub const BUILD_TIME: &str = env!("DEFING_BUILD_TIME");

use std::convert::Infallible;
use std::sync::{Arc, RwLock};

use axum::extract::{ConnectInfo, Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::sse::{Event as SseEvent, Sse};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use base64::Engine as _;
use dsh_core::command::{Command, DraftUpdateItem};
use dsh_core::model::{
    BranchName, GrayRule, GroupDef, ProjectId, SharedItem, Value, ValueType,
};
use dsh_core::wire::masked_value;
use dsh_core::{ErrorKind, StateMachine};
use dsh_crypto::Cipher;
use dsh_observability::{cluster_members_json, is_ready, metrics_text, AuditLog};
use dsh_publish::PublishService;
use dsh_raft::{NodeInfo as RaftNodeInfo, RaftHandle};
use dsh_render::{Format, Renderer};
use dsh_watch::{watch_sse, WatchHub};
use futures::stream::Stream;
use serde::{Deserialize, Serialize};

// ---------------- 状态 ----------------

/// HTTP 服务共享状态（axum State）。
#[derive(Clone)]
pub struct ApiState {
    pub sm: Arc<RwLock<StateMachine>>,
    /// 发布事件广播（watch SSE；dev-single 直发，集群由 raft apply 转发）
    pub hub: WatchHub,
    /// 集群模式下的 Raft 句柄
    pub raft: Option<RaftHandle>,
    pub node_id: Option<u64>,
    /// 加密器（secret 项加密/解密；未配置主密钥时 secret 写入被拒）
    pub cipher: Option<Arc<Cipher>>,
    /// 会话 TTL（I7）
    pub session_ttl: std::time::Duration,
    /// 管理员密码（--admin-password 或首启生成打印）
    pub admin_password: Arc<str>,
    /// 发布编排（模块 04）
    pub publish: PublishService,
    /// 审计落库（模块 10）
    pub audit: AuditLog,
    /// 主密钥环文件路径（{master-key-file}.ring.json；轮换后持久化，重启可加载）
    pub ring_path: Option<std::path::PathBuf>,
    /// 版本保留数（0=全量保留；admin/retention-status 展示）
    pub version_retention: u64,
    /// 审计保留条数（0=不裁剪）
    pub audit_retention: u64,
    /// 集群 join 引导令牌（/api/v1/cluster/join 需 Bearer 匹配；None=不校验）
    pub join_token: Option<std::sync::Arc<str>>,
    /// 登录失败节流（S6：进程内、按节点独立；集群需前置 LB 层限流）
    login_throttle: std::sync::Arc<LoginThrottle>,
    /// 可信代理 CIDR（F4）：仅来自这些网段的请求才信任 X-Forwarded-For；空 = 一律忽略 XFF。
    trusted_proxies: std::sync::Arc<TrustedProxies>,
    /// dev-single 开发数据面 token（全局有效，仅 dev 模式注入；集群模式恒 None）
    dev_token: Option<std::sync::Arc<str>>,
    /// 读取模式（G1/D37：Linear=ReadIndex 门控 / Stale=本地直读；CLI --read-mode 注入，默认 Linear）
    pub read_mode: dsh_core::model::ReadMode,
}

impl ApiState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sm: Arc<RwLock<StateMachine>>,
        hub: WatchHub,
        raft: Option<RaftHandle>,
        node_id: Option<u64>,
        cipher: Option<Arc<Cipher>>,
        session_ttl: std::time::Duration,
        admin_password: Arc<str>,
        ring_path: Option<std::path::PathBuf>,
    ) -> Self {
        Self::with_retention(
            sm,
            hub,
            raft,
            node_id,
            cipher,
            session_ttl,
            admin_password,
            ring_path,
            0,
            0,
            None,
            std::sync::Arc::new(TrustedProxies::empty()),
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_retention(
        sm: Arc<RwLock<StateMachine>>,
        hub: WatchHub,
        raft: Option<RaftHandle>,
        node_id: Option<u64>,
        cipher: Option<Arc<Cipher>>,
        session_ttl: std::time::Duration,
        admin_password: Arc<str>,
        ring_path: Option<std::path::PathBuf>,
        version_retention: u64,
        audit_retention: u64,
        join_token: Option<std::sync::Arc<str>>,
        trusted_proxies: std::sync::Arc<TrustedProxies>,
        dev_token: Option<std::sync::Arc<str>>,
    ) -> Self {
        let publish = PublishService::new(
            sm.clone(),
            cipher.clone(),
            raft.clone(),
            Some(hub.sender().clone()),
        );
        let audit = AuditLog::new(sm.clone(), raft.clone());
        Self {
            sm,
            hub,
            raft,
            node_id,
            cipher,
            session_ttl,
            admin_password,
            publish,
            audit,
            ring_path,
            version_retention,
            audit_retention,
            join_token,
            login_throttle: std::sync::Arc::new(LoginThrottle::new()),
            trusted_proxies,
            dev_token,
            read_mode: dsh_core::model::ReadMode::Stale,
        }
    }

    /// 通用写（dev-single 直 apply；集群经 Raft client_write）。
    /// G1/D37：线性读门控——Linear 模式 + 集群下先做 ReadIndex（ensure_linearizable），
    /// 保证读已提交；Stale 或 dev-single 直接返回（本地读）。
    pub async fn linearized_read(&self) -> Result<(), ApiError> {
        if self.read_mode != dsh_core::model::ReadMode::Linear {
            return Ok(());
        }
        let Some(raft) = &self.raft else {
            return Ok(()); // dev-single：无 raft，恒满足
        };
        match raft.ensure_linearizable().await {
            Ok(_) => Ok(()),
            Err(e) => {
                // 修订（openraft 0.9 无 follower 侧 ReadIndex）：follower 返回
                // ForwardToLeader → 复用写路径重定向（ERR_LEADER_REDIRECT + leader http）
                if let Some(hint) = leader_http_hint(&e) {
                    Err(ApiError(
                        dsh_core::Error::new(
                            dsh_core::ErrorKind::LeaderRedirect,
                            "linear read: forward to leader",
                        )
                        .with_leader_hint(hint),
                    ))
                } else {
                    Err(ApiError(dsh_core::Error::internal(format!(
                        "linearized read failed: {e}"
                    ))))
                }
            }
        }
    }

    async fn write(&self, cmd: &Command, now_ms: i64) -> Result<dsh_raft::WriteOutcome, ApiError> {
        dsh_raft::write_command(
            &self.sm,
            self.raft.as_ref(),
            cmd,
            now_ms,
            Some(self.hub.sender()),
        )
        .await
        .map_err(ApiError::from)
    }
}

// ---------------- 错误 ----------------

#[derive(Debug, Serialize)]
pub struct ApiErrorBody {
    pub code: String,
    pub message: String,
    pub detail: Option<serde_json::Value>,
}

pub struct ApiError(pub dsh_core::Error);

/// F13：状态机锁中毒 → 500 错误而非请求级 panic。
fn lock_err<E>(_: std::sync::PoisonError<E>) -> ApiError {
    ApiError(dsh_core::Error::internal("state machine lock poisoned"))
}

impl From<dsh_core::Error> for ApiError {
    fn from(e: dsh_core::Error) -> Self {
        Self(e)
    }
}

impl From<ApiError> for (StatusCode, Json<ApiErrorBody>) {
    fn from(e: ApiError) -> Self {
        let status = match e.0.kind {
            ErrorKind::NotFound => StatusCode::NOT_FOUND,
            ErrorKind::Conflict | ErrorKind::NoDraft | ErrorKind::SessionInUse => {
                StatusCode::CONFLICT
            }
            ErrorKind::SessionExpired => StatusCode::UNAUTHORIZED,
            ErrorKind::Forbidden => StatusCode::FORBIDDEN,
            ErrorKind::Validation | ErrorKind::PublishBlocked | ErrorKind::CycleRef => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            ErrorKind::LimitExceeded => StatusCode::TOO_MANY_REQUESTS,
            // D-STATUS：LeaderRedirect 使用独立状态码 428（Precondition Required），
            // 不再与真实 409 Conflict 混淆；响应体仍携带 ERR_LEADER_REDIRECT + leader_hint，
            // SDK/CLI 按 body code 判断，不受状态码变化影响。
            ErrorKind::LeaderRedirect => StatusCode::PRECONDITION_REQUIRED,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (
            status,
            Json(ApiErrorBody {
                code: e.0.kind.code().into(),
                message: e.0.message,
                detail: e.0.detail.or_else(|| {
                    e.0.leader_hint
                        .map(|h| serde_json::json!({ "leader_hint": h }))
                }),
            }),
        )
    }
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiErrorBody>)>;

// ---------------- 请求/响应体 ----------------

#[derive(Deserialize)]
struct CreateProjectReq {
    name: String,
}

#[derive(Deserialize)]
struct CreateBranchReq {
    name: String,
    #[serde(default)]
    source: Option<String>,
}

#[derive(Deserialize)]
struct StructureDraftReq {
    base_version: u64,
    groups: Vec<GroupDef>,
}

#[derive(Deserialize)]
struct PublishReq {
    comment: String,
    #[serde(default)]
    request_id: Option<String>,
}

#[derive(Deserialize)]
struct RollbackReq {
    to_version: u64,
    comment: String,
    #[serde(default)]
    request_id: Option<String>,
}

/// G3 最小管理面（G4 UI tab 前置）：灰度发布请求体。
#[derive(Deserialize)]
struct GrayPublishReq {
    rule: GrayRule,
    comment: String,
    #[serde(default)]
    request_id: Option<String>,
}

/// G3 最小管理面：灰度转正/下量请求体。
#[derive(Deserialize)]
struct GrayReq {
    comment: String,
    #[serde(default)]
    request_id: Option<String>,
}

#[derive(Deserialize)]
struct DraftUpdateReq {
    #[serde(default)]
    updates: Vec<DraftUpdateItem>,
    /// "group/key" 列表
    #[serde(default)]
    deletes: Vec<String>,
    /// 分支级共享引用绑定 upsert/解除（shared_key 空串 = 解除绑定）
    #[serde(default)]
    shared_bindings: Vec<SharedBindingReq>,
    /// 乐观锁：期望的草稿修订号（缺省 None = 不校验，兼容旧客户端）。
    /// 不匹配 → 409 Conflict（并发编辑冲突，客户端刷新最新草稿后重试）。
    #[serde(default)]
    expected_draft_rev: Option<u64>,
}

#[derive(Deserialize)]
struct SharedBindingReq {
    group: String,
    key: String,
    shared_key: String,
}

#[derive(Deserialize)]
struct JoinReq {
    node_id: u64,
    /// 仅 cluster/join 需要；promote 可缺省
    #[serde(default)]
    http_addr: String,
    #[serde(default)]
    raft_addr: String,
}

#[derive(Serialize)]
struct ConfigResp {
    project: String,
    branch: String,
    version: u64,
    /// G3/D27：服务端按身份 resolve 的版本号（= version 的语义别名；稳定=active，灰度=gray_seq）。
    resolved_version: u64,
    structure_version: u64,
    groups: serde_json::Value,
    /// G3/D27：true = 本次返回灰度快照内容（客户端可见自己在灰度）。
    #[serde(default)]
    gray: bool,
}

// ---------------- handlers ----------------

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "build": { "commit": BUILD_COMMIT, "time": BUILD_TIME.parse::<u64>().unwrap_or(0) },
    }))
}

// ---------------- Admin UI（模块 09：内嵌静态页） ----------------

#[derive(rust_embed::Embed)]
#[folder = "admin/"]
struct AdminAssets;

async fn admin_index() -> axum::response::Response {
    match AdminAssets::get("index.html") {
        Some(f) => axum::response::Response::builder()
            .header("content-type", "text/html; charset=utf-8")
            // admin 资源禁用缓存：UI 随二进制版本演进，浏览器缓存旧 JS 会造成「更新后仍显示旧界面」
            .header("cache-control", "no-store")
            .body(axum::body::Body::from(f.data.into_owned()))
            .expect("admin index"),
        None => axum::response::Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(axum::body::Body::empty())
            .expect("404"),
    }
}

async fn admin_static(
    axum::extract::Path(path): axum::extract::Path<String>,
) -> axum::response::Response {
    match AdminAssets::get(&path) {
        Some(f) => {
            // D-CSP：按扩展名设 content-type（app.js 需正确 MIME 才会执行）
            let ct = if path.ends_with(".js") {
                "application/javascript; charset=utf-8"
            } else if path.ends_with(".css") {
                "text/css; charset=utf-8"
            } else if path.ends_with(".html") {
                "text/html; charset=utf-8"
            } else if path.ends_with(".svg") {
                "image/svg+xml"
            } else if path.ends_with(".png") {
                "image/png"
            } else {
                "application/octet-stream"
            };
            axum::response::Response::builder()
                .header("content-type", ct)
                // admin 资源禁用缓存（同上：避免浏览器缓存旧 JS 导致界面与二进制版本脱节）
                .header("cache-control", "no-store")
                .body(axum::body::Body::from(f.data.into_owned()))
                .expect("asset")
        }
        None => axum::response::Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(axum::body::Body::empty())
            .expect("404"),
    }
}

/// 安全响应头（M4 加固）。
async fn security_headers(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let mut resp = next.run(req).await;
    let h = resp.headers_mut();
    h.insert(
        "x-content-type-options",
        axum::http::HeaderValue::from_static("nosniff"),
    );
    h.insert(
        "x-frame-options",
        axum::http::HeaderValue::from_static("DENY"),
    );
    h.insert(
        "content-security-policy",
        // D-CSP：Admin 脚本已外置（app.js），移除 script-src 'unsafe-inline'
        //（内联 style 属性仍存在，style-src 保留 unsafe-inline；XSS 即 RCE 纵深被消除）
        axum::http::HeaderValue::from_static(
            "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; font-src 'self'",
        ),
    );
    resp
}

/// 会话主体解析结果（中间件与 render_config 共用，N15：禁止第三份解析实现）。
fn resolve_principal(app: &ApiState, auth_header: Option<&str>) -> Result<dsh_core::Principal, ()> {
    let token = auth_header
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(())?;
    let hash = dsh_core::token_hash(token);
    let sm = app.sm.read().map_err(|_| ())?;
    // token 前缀路由（multisession 改造）：adm.{sid}.{secret} → sess/admin/{sid}；
    // pa.{username}.{sid}.{secret} → sess/pa/{username}/{sid}；旧格式（无 sid）fallback 旧 key。
    let (_, session) = if let Some(rest) = token.strip_prefix("pa.") {
        let parts: Vec<&str> = rest.split('.').collect();
        let username = parts.first().copied().unwrap_or("");
        let session = if parts.len() >= 3 {
            // 新格式 pa.{username}.{sid}.{secret}
            sm.get_pa_session_with(username, parts[1]).ok().flatten()
        } else {
            // 旧格式 pa.{username}.{secret}
            sm.get_pa_session(username).ok().flatten()
        };
        (
            dsh_core::Principal::ProjectAdmin {
                username: username.to_string(),
                project: dsh_core::ProjectId(String::new()),
            },
            session,
        )
    } else if let Some(rest) = token.strip_prefix("adm.") {
        let parts: Vec<&str> = rest.split('.').collect();
        let session = if parts.len() >= 2 {
            // 新格式 adm.{sid}.{secret}
            sm.get_session_with(parts[0]).ok().flatten()
        } else {
            // 旧格式 adm.{secret}
            sm.get_session().ok().flatten()
        };
        (dsh_core::Principal::Admin, session)
    } else {
        // 无前缀旧格式 fallback
        (dsh_core::Principal::Admin, sm.get_session().ok().flatten())
    };
    match session {
        Some(s) => {
            let hash_ok = s.token_hash == hash;
            let not_expired = s.expires_at.map(|e| now_ms() < e).unwrap_or(true);
            if !(hash_ok && not_expired) {
                return Err(());
            }
            // 以状态机中存储的 principal 为准（PA 的 project 归属不可由 token 伪造）
            Ok(s.principal)
        }
        None => Err(()),
    }
}

/// 从 Authorization 头提取当前会话的 session_id（multisession 改造）：
/// 新格式 adm.{sid}.{secret} / pa.{username}.{sid}.{secret} → 第 2 段；
/// 旧格式（无 sid）→ None（走旧单会话路径）。
/// 与 resolve_principal 同一 token 解析纪律（N15），logout/heartbeat 用。
fn token_session_id(auth_header: Option<&str>) -> Option<String> {
    let token = auth_header?.strip_prefix("Bearer ")?;
    if let Some(rest) = token.strip_prefix("pa.") {
        let parts: Vec<&str> = rest.split('.').collect();
        if parts.len() >= 3 {
            return Some(parts[1].to_string());
        }
    } else if let Some(rest) = token.strip_prefix("adm.") {
        let parts: Vec<&str> = rest.split('.').collect();
        if parts.len() >= 2 {
            return Some(parts[0].to_string());
        }
    }
    None
}

/// 从未解码 path 提取 /api/v1/projects/{p}/... 的 {p} 段（N2：URL 编码/大写/特殊字符
/// 不通过 valid_name → None → 对 PA 默认拒绝）。
fn project_segment(path: &str) -> Option<String> {
    let rest = path.strip_prefix("/api/v1/projects/")?;
    let seg = rest.split('/').next()?;
    if !seg.is_empty()
        && seg
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        Some(seg.to_string())
    } else {
        None
    }
}

/// 从 /v1/projects/{p}/... 路径提取 {p}（数据面；字符集同 N2，非法 → None → 401）。
fn data_plane_project_segment(path: &str) -> Option<String> {
    let rest = path.strip_prefix("/v1/projects/")?;
    let seg = rest.split('/').next()?;
    if !seg.is_empty()
        && seg
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        Some(seg.to_string())
    } else {
        None
    }
}

/// 从请求提取数据面 token：Authorization: Bearer <t> 优先，其次 ?token=<t>（SSE EventSource 兼容）。
fn extract_data_token(req: &axum::extract::Request) -> Option<String> {
    if let Some(v) = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|a| a.strip_prefix("Bearer "))
    {
        return Some(v.to_string());
    }
    req.uri().query().and_then(|q| {
        q.split('&')
            .find_map(|kv| kv.strip_prefix("token=").map(|t| t.to_string()))
    })
}

/// 数据面鉴权：dev token 全局有效；否则按项目校验（单次 KV 读）。
fn data_plane_authorized(app: &ApiState, req: &axum::extract::Request) -> bool {
    let Some(raw) = extract_data_token(req) else {
        return false;
    };
    if let Some(dev) = &app.dev_token {
        if raw == dev.as_ref() {
            return true;
        }
    }
    let Some(pid) = data_plane_project_segment(req.uri().path()) else {
        return false;
    };
    let sm = match app.sm.read() {
        Ok(s) => s,
        Err(_) => return false,
    };
    match sm.get_data_token(&dsh_core::token_hash(&raw)) {
        Ok(Some(rec)) => !rec.revoked && rec.project.0 == pid,
        _ => false,
    }
}

/// 数据面 reveal 审计身份：dev-single 开发 token → "data-plane:dev-single"；
/// 项目访问令牌 → "data-plane:{token name}"；无法解析 → "data-plane"。
fn data_plane_operator(app: &ApiState, req: &axum::extract::Request) -> String {
    let Some(raw) = extract_data_token(req) else {
        return "data-plane".into();
    };
    if let Some(dev) = &app.dev_token {
        if raw == dev.as_ref() {
            return "data-plane:dev-single".into();
        }
    }
    let sm = match app.sm.read() {
        Ok(s) => s,
        Err(_) => return "data-plane".into(),
    };
    match sm.get_data_token(&dsh_core::token_hash(&raw)) {
        Ok(Some(rec)) => format!("data-plane:{}", rec.name),
        _ => "data-plane".into(),
    }
}

/// 项目管理员授权矩阵（§4：默认拒绝、显式放行）。
/// 返回 true=放行。全局管理员永远放行（调用方判断）。
fn pa_allowed(principal: &dsh_core::Principal, method: &str, path: &str) -> bool {
    let dsh_core::Principal::ProjectAdmin { project, .. } = principal else {
        return true; // 全局管理员
    };
    let pid = &project.0;

    // 自身会话（显式放行，防锁死 B5）
    if method == "POST" && (path == "/api/v1/logout" || path == "/api/v1/heartbeat") {
        return true;
    }
    // 自助改密（B1：校验当前密码；只能改自己，主体取自会话扩展不可伪造）
    if method == "POST" && path == "/api/v1/me/password" {
        return true;
    }
    // 读项目列表（handler 内过滤为自己项目）
    if method == "GET" && path == "/api/v1/projects" {
        return true;
    }
    // 审计（handler 内强制过滤 project）
    if method == "GET" && path == "/api/v1/audit" {
        return true;
    }
    // 集群成员只读（PA 需端点列表配置 SDK 连接；join/promote/remove 仍拒绝）
    if method == "GET" && path == "/api/v1/cluster/members" {
        return true;
    }
    // 共享库只读（草稿页「引用共享」下拉数据源；secret 值已掩码；写仍仅全局管理员）
    if method == "GET" && (path == "/api/v1/shared" || path == "/api/v1/shared-draft") {
        return true;
    }

    // 项目本地端点：/api/v1/projects/{p}/... 且 p == 自己项目
    if let Some(p) = project_segment(path) {
        if &p != pid {
            return false; // 跨项目
        }
        // PA 账号管理端点 + 访问令牌（全局面）拒绝
        if path
            .strip_prefix(&format!("/api/v1/projects/{pid}"))
            .is_some_and(|r| r.starts_with("/admins") || r.starts_with("/tokens"))
        {
            return false;
        }
        // 项目面操作拒绝：删除/创建项目本身（即使 {p}==自己项目也不许自毁/建新项目）
        if method == "DELETE" && path == format!("/api/v1/projects/{pid}") {
            return false;
        }
        // 其余项目本地端点全部放行（结构/值/分支/版本/对比/promote/diff/config/reveal）
        return true;
    }
    // 非项目路径：默认拒绝（共享写、集群、全局 admin、账号等）
    false
}

/// 管理面鉴权中间件（全局管理员 + 项目管理员双主体；除 login/healthz/readyz 外均需 Bearer）。
async fn auth_middleware(
    State(app): State<ApiState>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, (StatusCode, Json<ApiErrorBody>)> {
    let path = req.uri().path().to_string();
    let method = req.method().as_str().to_string();
    // cluster/join 是节点加入前的引导调用（尚无管理员会话），豁免会话鉴权；
    // 引导令牌（--join-token）校验在 handler 内完成（join_token_ok）
    if path == "/api/v1/login"
        || path == "/healthz"
        || path == "/readyz"
        || path == "/api/v1/cluster/join"
    {
        return Ok(next.run(req).await);
    }
    if path.starts_with("/api/v1/") {
        let auth = req
            .headers()
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(|t| t.to_string());
        let principal = match resolve_principal(&app, auth.as_deref()) {
            Ok(p) => p,
            Err(_) => {
                return Err((
                    StatusCode::UNAUTHORIZED,
                    Json(ApiErrorBody {
                        code: "ERR_SESSION_EXPIRED".into(),
                        message: "需要管理员会话".into(),
                        detail: None,
                    }),
                ));
            }
        };
        // 授权矩阵：项目管理员默认拒绝、显式放行（§4）
        if !pa_allowed(&principal, &method, &path) {
            return Err((
                StatusCode::FORBIDDEN,
                Json(ApiErrorBody {
                    code: "ERR_FORBIDDEN".into(),
                    message: "项目管理员无权访问该资源".into(),
                    detail: None,
                }),
            ));
        }
        req.extensions_mut().insert(principal);
    } else if path.starts_with("/v1/") {
        // 项目访问令牌鉴权：/v1/projects/{p}/... 需该项目有效 token（Bearer 或 ?token=，
        // 后者兼容 SSE EventSource）；dev-single 开发 token 全局有效。无有效 token → 401。
        // 例外①：渲染端点 + reveal=true → 走 handler 内鉴权（会话或数据面 token，B2：PA 仅能 reveal 自己项目）。
        // 例外②：渲染端点（掩码/数据面解密）→ 管理会话可查看（Admin 全项目 / PA 仅自己项目）——
        //         Admin UI 配置预览用会话访问（数据面 token 化后回归修复）；
        //         数据面 token 授权下 handler 默认解密 secret（构建脚本取真值，README「构建脚本取值」）。
        let is_render = path.ends_with("/config");
        let reveal_render = is_render
            && req
                .uri()
                .query()
                .map(|q| q.split('&').any(|kv| kv == "reveal=true"))
                .unwrap_or(false);
        let session_allowed = if is_render && !reveal_render {
            match resolve_principal(
                &app,
                req.headers()
                    .get(axum::http::header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok()),
            ) {
                Ok(dsh_core::Principal::Admin) => true,
                Ok(dsh_core::Principal::ProjectAdmin { project, .. }) => {
                    data_plane_project_segment(&path)
                        .map(|p| p == project.0)
                        .unwrap_or(false)
                }
                Err(_) => false,
            }
        } else {
            false
        };
        if !reveal_render && !session_allowed && !data_plane_authorized(&app, &req) {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(ApiErrorBody {
                    code: "ERR_UNAUTHORIZED".into(),
                    message: "data-plane token required".into(),
                    detail: None,
                }),
            ));
        }
    }
    Ok(next.run(req).await)
}

async fn create_project(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    Json(req): Json<CreateProjectReq>,
) -> ApiResult<serde_json::Value> {
    let pid = ProjectId(req.name.clone());
    app.write(
        &Command::ProjectCreate {
            name: req.name.clone(),
            operator: "admin".to_string(),
            ts: now_ms(),
        },
        now_ms(),
    )
    .await?;
    app.audit
        .append(
            "project_create",
            Some(pid.as_str().into()),
            None,
            None,
            None,
            serde_json::json!({}),
            &principal_op(&principal),
        )
        .await;
    Ok(Json(
        serde_json::json!({ "id": pid.as_str(), "branches": ["dev", "test", "prod"] }),
    ))
}

async fn list_projects(
    State(app): State<ApiState>,
    principal: axum::Extension<dsh_core::Principal>,
) -> ApiResult<serde_json::Value> {
    let sm = app.sm.read().map_err(lock_err)?;
    let mut projects = sm
        .list_projects()
        .map_err(ApiError::from)?
        .into_iter()
        .map(|p| serde_json::json!({ "id": p.id.as_str(), "name": p.name }))
        .collect::<Vec<_>>();
    // PA 只能看到自己项目（§4：handler 层过滤）
    if let dsh_core::Principal::ProjectAdmin { project, .. } = principal.0 {
        projects.retain(|p| p.get("id").and_then(|i| i.as_str()) == Some(project.0.as_str()));
    }
    Ok(Json(serde_json::json!(projects)))
}

async fn list_branches(
    State(app): State<ApiState>,
    AxumPath(pid): AxumPath<String>,
) -> ApiResult<serde_json::Value> {
    let sm = app.sm.read().map_err(lock_err)?;
    let id = ProjectId(pid);
    let branches = sm
        .list_branches(&id)
        .map_err(ApiError::from)?
        .into_iter()
        .map(|b| {
            let st = sm.get_branch_state(&id, &b).ok().flatten();
            serde_json::json!({
                "name": b.as_str(),
                "active_version": st.map(|s| s.active_version).unwrap_or(0)
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(serde_json::json!(branches)))
}

async fn create_branch(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    AxumPath(pid): AxumPath<String>,
    Json(req): Json<CreateBranchReq>,
) -> ApiResult<serde_json::Value> {
    let source = req.source.as_deref().map(BranchName::from);
    app.write(
        &Command::BranchCreate {
            project: ProjectId(pid.clone()),
            name: BranchName(req.name.clone()),
            source,

            operator: principal_op(&principal),
            ts: now_ms(),
        },
        now_ms(),
    )
    .await?;
    app.audit
        .append(
            "branch_create",
            Some(pid.clone()),
            Some(req.name.clone()),
            None,
            None,
            serde_json::json!({}),
            &principal_op(&principal),
        )
        .await;
    Ok(Json(
        serde_json::json!({ "name": req.name, "project": pid }),
    ))
}

async fn get_structure_draft(
    State(app): State<ApiState>,
    AxumPath(pid): AxumPath<String>,
) -> ApiResult<serde_json::Value> {
    let sm = app.sm.read().map_err(lock_err)?;
    let draft = sm
        .get_structure_draft(&ProjectId(pid))
        .map_err(ApiError::from)?;
    match draft {
        Some(d) => Ok(Json(serde_json::to_value(d).expect("serialize"))),
        None => Ok(Json(
            serde_json::json!({ "base_version": null, "groups": [] }),
        )),
    }
}

/// 已发布结构查询（级联选择器数据源）。
async fn get_published_structure(
    State(app): State<ApiState>,
    AxumPath(pid): AxumPath<String>,
) -> ApiResult<serde_json::Value> {
    let sm = app.sm.read().map_err(lock_err)?;
    if sm
        .get_project(&ProjectId(pid.clone()))
        .map_err(ApiError::from)?
        .is_none()
    {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ApiErrorBody {
                code: "ERR_NOT_FOUND".into(),
                message: "project not found".into(),
                detail: None,
            }),
        ));
    }
    match sm.get_structure(&ProjectId(pid)).map_err(ApiError::from)? {
        Some(st) => Ok(Json(serde_json::to_value(st).expect("serialize"))),
        None => Ok(Json(serde_json::json!({ "version": 0, "groups": [] }))),
    }
}

async fn set_structure_draft(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    AxumPath(pid): AxumPath<String>,
    Json(req): Json<StructureDraftReq>,
) -> ApiResult<serde_json::Value> {
    app.write(
        &Command::StructureDraftSet {
            project: ProjectId(pid.clone()),
            base_version: req.base_version,
            groups: req.groups,

            operator: principal_op(&principal),
        },
        now_ms(),
    )
    .await?;
    app.audit
        .append(
            "draft_update",
            Some(pid.clone()),
            None,
            None,
            None,
            serde_json::json!({ "kind": "structure-draft" }),
            &principal_op(&principal),
        )
        .await;
    Ok(Json(serde_json::json!({ "saved": true, "project": pid })))
}

async fn publish_structure(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    AxumPath(pid): AxumPath<String>,
    Json(req): Json<PublishReq>,
) -> ApiResult<serde_json::Value> {
    let rid = req.request_id.unwrap_or_else(new_request_id);
    let outcome = app
        .publish
        .publish_structure(
            &ProjectId(pid.clone()),
            &req.comment,
            &rid,
            &principal_op(&principal),
        )
        .await
        .map_err(ApiError::from)?;
    let affected = outcome
        .affected
        .iter()
        .map(|(b, v)| serde_json::json!({ "branch": b, "version": v }))
        .collect::<Vec<_>>();
    app.audit
        .append(
            "structure_publish",
            Some(pid.clone()),
            None,
            None,
            Some(rid.clone()),
            serde_json::json!({ "affected_branches": affected }),
            &principal_op(&principal),
        )
        .await;
    Ok(Json(
        serde_json::json!({ "affected_branches": affected, "request_id": rid }),
    ))
}

async fn update_draft(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    AxumPath((pid, branch)): AxumPath<(String, String)>,
    Json(req): Json<DraftUpdateReq>,
) -> ApiResult<serde_json::Value> {
    // D-DEL：deletes 条目须为 "group/key" 形式（此前无 '/' 的条目被 filter_map 静默丢弃）
    if let Some(bad) = req.deletes.iter().find(|s| !s.contains('/')) {
        return Err(ApiError(dsh_core::Error::validation(format!(
            "delete 条目须为 group/key 形式: {bad:?}"
        )))
        .into());
    }
    let deletes: Vec<(String, String)> = req
        .deletes
        .iter()
        .filter_map(|s| {
            s.split_once('/')
                .map(|(g, k)| (g.to_string(), k.to_string()))
        })
        .collect();
    let updates_len = req.updates.len();
    let deletes_len = deletes.len();
    let bindings_len = req.shared_bindings.len();
    let bindings: Vec<dsh_core::command::SharedBinding> = req
        .shared_bindings
        .iter()
        .map(|b| dsh_core::command::SharedBinding {
            group: b.group.clone(),
            key: b.key.clone(),
            shared_key: b.shared_key.clone(),
        })
        .collect();
    let pid_obj = ProjectId(pid.clone());
    let branch_obj = BranchName(branch.clone());
    app.publish
        .update_draft(
            &pid_obj,
            &branch_obj,
            req.updates,
            deletes,
            bindings,
            req.expected_draft_rev,
            &principal_op(&principal),
        )
        .await
        .map_err(ApiError::from)?;
    app.audit
        .append(
            "draft_update",
            Some(pid.clone()),
            Some(branch.clone()),
            None,
            None,
            serde_json::json!({
                "updates": updates_len,
                "deletes": deletes_len,
                "bindings": bindings_len,
            }),
            &principal_op(&principal),
        )
        .await;
    Ok(Json(
        serde_json::json!({ "saved": true, "project": pid, "branch": branch }),
    ))
}

async fn publish(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    AxumPath((pid, branch)): AxumPath<(String, String)>,
    Json(req): Json<PublishReq>,
) -> ApiResult<serde_json::Value> {
    let rid = req.request_id.unwrap_or_else(new_request_id);
    let outcome = app
        .publish
        .publish(
            &ProjectId(pid.clone()),
            &BranchName(branch.clone()),
            &req.comment,
            &rid,
            &principal_op(&principal),
        )
        .await
        .map_err(ApiError::from)?;
    app.audit
        .append(
            "publish",
            Some(pid.clone()),
            Some(branch.clone()),
            Some(outcome.version),
            Some(rid.clone()),
            // G1/D35：warn 模式下校验告警留痕
            serde_json::json!({
                "warnings": outcome.warnings,
            }),
            &principal_op(&principal),
        )
        .await;
    Ok(Json(serde_json::json!({
        "version": outcome.version,
        "changes": serde_json::to_value(&outcome.changes).expect("serialize"),
        // G1/D35：warn 发布时校验告警（block 恒空数组）
        "warnings": outcome.warnings,
        "request_id": rid,
    })))
}

async fn rollback(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    AxumPath((pid, branch)): AxumPath<(String, String)>,
    Json(req): Json<RollbackReq>,
) -> ApiResult<serde_json::Value> {
    let rid = req.request_id.unwrap_or_else(new_request_id);
    let new_version = app
        .publish
        .rollback(
            &ProjectId(pid.clone()),
            &BranchName(branch.clone()),
            req.to_version,
            &req.comment,
            &rid,
            &principal_op(&principal),
        )
        .await
        .map_err(ApiError::from)?;
    app.audit
        .append(
            "rollback",
            Some(pid.clone()),
            Some(branch.clone()),
            Some(new_version),
            Some(rid.clone()),
            serde_json::json!({ "to_version": req.to_version }),
            &principal_op(&principal),
        )
        .await;
    Ok(Json(
        serde_json::json!({ "new_version": new_version, "request_id": rid }),
    ))
}

// ---------------- 灰度管理面（G3 最小端点；G4 UI tab + openapi 补全） ----------------

/// 灰度发布（POST /api/v1/projects/{p}/branches/{b}/gray-publish）。
async fn gray_publish_branch(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    AxumPath((pid, branch)): AxumPath<(String, String)>,
    Json(req): Json<GrayPublishReq>,
) -> ApiResult<serde_json::Value> {
    let rid = req.request_id.unwrap_or_else(new_request_id);
    let events = app
        .publish
        .gray_publish(
            &ProjectId(pid.clone()),
            &BranchName(branch.clone()),
            req.rule,
            &req.comment,
            &rid,
            &principal_op(&principal),
        )
        .await
        .map_err(ApiError::from)?;
    let (gray_seq, gray_rule, active_version) = {
        let sm = app.sm.read().map_err(lock_err)?;
        let st = sm
            .get_branch_state(&ProjectId(pid.clone()), &BranchName(branch.clone()))
            .map_err(ApiError::from)?;
        (
            st.as_ref().map(|s| s.gray_seq).unwrap_or(0),
            st.as_ref().and_then(|s| s.gray_rule.clone()),
            st.as_ref().map(|s| s.active_version).unwrap_or(0),
        )
    };
    app.audit
        .append(
            "gray_publish",
            Some(pid.clone()),
            Some(branch.clone()),
            None,
            Some(rid.clone()),
            serde_json::json!({ "gray_seq": gray_seq }),
            &principal_op(&principal),
        )
        .await;
    Ok(Json(serde_json::json!({
        "gray_seq": gray_seq,
        "gray_rule": gray_rule,
        "active_version": active_version,
        "event_gray": events.first().map(|e| e.gray).unwrap_or(false),
        "request_id": rid,
    })))
}

/// 灰度转正（POST /api/v1/projects/{p}/branches/{b}/gray-promote）。
async fn gray_promote_branch(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    AxumPath((pid, branch)): AxumPath<(String, String)>,
    Json(req): Json<GrayReq>,
) -> ApiResult<serde_json::Value> {
    let rid = req.request_id.unwrap_or_else(new_request_id);
    let events = app
        .publish
        .gray_promote(
            &ProjectId(pid.clone()),
            &BranchName(branch.clone()),
            &req.comment,
            &rid,
            &principal_op(&principal),
        )
        .await
        .map_err(ApiError::from)?;
    let new_active = events.first().map(|e| e.version).unwrap_or_else(|| {
        // 幂等重放：事件为空 → 读当前 active
        app.sm
            .read()
            .ok()
            .and_then(|sm| {
                sm.get_branch_state(&ProjectId(pid.clone()), &BranchName(branch.clone()))
                    .ok()
                    .flatten()
            })
            .map(|s| s.active_version)
            .unwrap_or(0)
    });
    app.audit
        .append(
            "gray_promote",
            Some(pid.clone()),
            Some(branch.clone()),
            Some(new_active),
            Some(rid.clone()),
            serde_json::json!({}),
            &principal_op(&principal),
        )
        .await;
    Ok(Json(serde_json::json!({
        "active_version": new_active,
        "request_id": rid,
    })))
}

/// 灰度下量/回滚（POST /api/v1/projects/{p}/branches/{b}/gray-abort）。
async fn gray_abort_branch(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    AxumPath((pid, branch)): AxumPath<(String, String)>,
    Json(req): Json<GrayReq>,
) -> ApiResult<serde_json::Value> {
    let rid = req.request_id.unwrap_or_else(new_request_id);
    let events = app
        .publish
        .gray_abort(
            &ProjectId(pid.clone()),
            &BranchName(branch.clone()),
            &req.comment,
            &rid,
            &principal_op(&principal),
        )
        .await
        .map_err(ApiError::from)?;
    let fallback_version = events.first().map(|e| e.version).unwrap_or(0);
    app.audit
        .append(
            "gray_abort",
            Some(pid.clone()),
            Some(branch.clone()),
            None,
            Some(rid.clone()),
            serde_json::json!({}),
            &principal_op(&principal),
        )
        .await;
    Ok(Json(serde_json::json!({
        "fallback_version": fallback_version,
        "request_id": rid,
    })))
}

/// 灰度状态（GET /api/v1/projects/{p}/branches/{b}/gray-status）。
async fn gray_status(
    State(app): State<ApiState>,
    AxumPath((pid, branch)): AxumPath<(String, String)>,
) -> ApiResult<serde_json::Value> {
    let sm = app.sm.read().map_err(lock_err)?;
    let st = sm
        .get_branch_state(&ProjectId(pid.clone()), &BranchName(branch.clone()))
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError(dsh_core::Error::not_found(format!("branch {branch}"))))?;
    Ok(Json(serde_json::json!({
        "active_version": st.active_version,
        "structure_version": st.structure_version,
        "gray_active": st.gray_seq > 0,
        "gray_seq": st.gray_seq,
        "gray_rule": st.gray_rule,
    })))
}

// ---------------- 项目/分支详情与删除（openapi 契约补全） ----------------

async fn project_detail(
    State(app): State<ApiState>,
    AxumPath(pid): AxumPath<String>,
) -> ApiResult<serde_json::Value> {
    let sm = app.sm.read().map_err(lock_err)?;
    let p = sm
        .get_project(&ProjectId(pid.clone()))
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError(dsh_core::Error::not_found("project")))?;
    Ok(Json(serde_json::json!({
        "id": p.id.as_str(),
        "name": p.name,
        "created_at": p.created_at,
    })))
}

#[derive(Deserialize)]
struct ForceQuery {
    #[serde(default)]
    force: bool,
}

async fn delete_project(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    AxumPath(pid): AxumPath<String>,
    axum::extract::Query(q): axum::extract::Query<ForceQuery>,
) -> Result<StatusCode, (StatusCode, Json<ApiErrorBody>)> {
    if !q.force {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(ApiErrorBody {
                code: "ERR_VALIDATION".into(),
                message: "删除项目需要 force=true 确认".into(),
                detail: None,
            }),
        ));
    }
    let pid_obj = ProjectId(pid.clone());
    app.write(
        &Command::ProjectDelete {
            id: pid_obj,
            operator: "admin".to_string(),
        },
        now_ms(),
    )
    .await
    .map_err(Into::<(StatusCode, Json<ApiErrorBody>)>::into)?;
    app.audit
        .append(
            "project_delete",
            Some(pid.clone()),
            None,
            None,
            None,
            serde_json::json!({}),
            &principal_op(&principal),
        )
        .await;
    Ok(StatusCode::NO_CONTENT)
}

async fn branch_detail(
    State(app): State<ApiState>,
    AxumPath((pid, branch)): AxumPath<(String, String)>,
) -> ApiResult<serde_json::Value> {
    let sm = app.sm.read().map_err(lock_err)?;
    let id = ProjectId(pid.clone());
    let bname = BranchName(branch.clone());
    let st = sm
        .get_branch_state(&id, &bname)
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError(dsh_core::Error::not_found("branch")))?;
    let drafts: serde_json::Value = st
        .value_draft
        .iter()
        .map(|(g, items)| {
            let m: serde_json::Map<String, serde_json::Value> = items
                .iter()
                .map(|(k, dv)| {
                    (
                        k.clone(),
                        serde_json::json!({ "value": dv.value, "updated_at": dv.updated_at }),
                    )
                })
                .collect();
            (g.clone(), serde_json::Value::Object(m))
        })
        .collect();
    // 引用项展示：结构 shared 项 × 本分支绑定解析值（secret 掩码，与共享库列表一致）；
    // 未绑定项 shared_key 为空串、version/value 为 null（草稿页据此渲染「请选择」下拉状态）
    let mut shared_refs = Vec::new();
    if let Some(structure) = sm.get_structure(&id).map_err(ApiError::from)? {
        for g in &structure.groups {
            for item in &g.items {
                if !item.shared {
                    continue;
                }
                let rk = st
                    .shared_bindings
                    .get(&g.name)
                    .and_then(|m| m.get(&item.key));
                match rk {
                    Some(rk) => {
                        if let Some(shared) = sm.get_shared(rk).map_err(ApiError::from)? {
                            shared_refs.push(serde_json::json!({
                                "group": g.name,
                                "key": item.key,
                                "shared_key": rk,
                                "version": shared.version,
                                "value": masked_shared_value(&shared),
                            }));
                        }
                    }
                    None => shared_refs.push(serde_json::json!({
                        "group": g.name,
                        "key": item.key,
                        "shared_key": "",
                        "version": null,
                        "value": null,
                    })),
                }
            }
        }
    }
    // 活动版本的值（草稿基线：发布后草稿清空，UI 显示已发布值；secret 恒掩码，草稿页只显示「已加密」占位）
    let active: serde_json::Value = match sm
        .get_config_resolved(&id, &bname, 0, &dsh_core::ClientCtx::default())
        .map_err(ApiError::from)
    {
        Ok(snap) => {
            let mut gmap = serde_json::Map::new();
            for (g, items) in &snap.groups {
                let mut m = serde_json::Map::new();
                for (k, v) in items {
                    let vj = match v {
                        dsh_core::model::Value::Secret(_) => serde_json::json!({
                            "type": "string", "str_value": "***", "masked": true,
                        }),
                        other => serde_json::json!(other),
                    };
                    m.insert(k.clone(), serde_json::json!({ "value": vj, "updated_at": 0 }));
                }
                gmap.insert(g.clone(), serde_json::Value::Object(m));
            }
            serde_json::Value::Object(gmap)
        }
        Err(_) => serde_json::json!({}),
    };
    Ok(Json(serde_json::json!({
        "name": branch,
        "active_version": st.active_version,
        "structure_version": st.structure_version,
        "draft_rev": st.draft_rev,
        "draft": drafts,
        "shared_refs": shared_refs,
        "active": active,
    })))
}

async fn delete_branch(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    AxumPath((pid, branch)): AxumPath<(String, String)>,
) -> Result<StatusCode, (StatusCode, Json<ApiErrorBody>)> {
    app.write(
        &Command::BranchDelete {
            project: ProjectId(pid.clone()),
            name: BranchName(branch.clone()),

            operator: principal_op(&principal),
        },
        now_ms(),
    )
    .await
    .map_err(Into::<(StatusCode, Json<ApiErrorBody>)>::into)?;
    app.audit
        .append(
            "branch_delete",
            Some(pid.clone()),
            Some(branch.clone()),
            None,
            None,
            serde_json::json!({}),
            &principal_op(&principal),
        )
        .await;
    Ok(StatusCode::NO_CONTENT)
}

// ---------------- 分支对比 + 值提升（openapi 契约补全） ----------------

#[derive(Deserialize)]
struct DiffQuery {
    branch_a: String,
    branch_b: String,
}

async fn branch_diff(
    State(app): State<ApiState>,
    AxumPath(pid): AxumPath<String>,
    axum::extract::Query(q): axum::extract::Query<DiffQuery>,
) -> ApiResult<serde_json::Value> {
    // G1/D37：线性读门控
    app.linearized_read().await?;
    let sm = app.sm.read().map_err(lock_err)?;
    let id = ProjectId(pid);
    let a = sm
        .get_config(&id, &BranchName(q.branch_a), 0)
        .map_err(ApiError::from)?;
    let b = sm
        .get_config(&id, &BranchName(q.branch_b), 0)
        .map_err(ApiError::from)?;
    let mut keys: std::collections::BTreeSet<(String, String)> = std::collections::BTreeSet::new();
    for (g, items) in &a.groups {
        for k in items.keys() {
            keys.insert((g.clone(), k.clone()));
        }
    }
    for (g, items) in &b.groups {
        for k in items.keys() {
            keys.insert((g.clone(), k.clone()));
        }
    }
    let mut diffs = Vec::new();
    let mut missing = Vec::new();
    for (g, k) in keys {
        let va = a.groups.get(&g).and_then(|m| m.get(&k));
        let vb = b.groups.get(&g).and_then(|m| m.get(&k));
        match (va, vb) {
            (Some(x), Some(y)) if x != y => {
                // F2：secret 密文不得出网 —— 统一经 masked_value 掩码（其余值原样）
                diffs.push(serde_json::json!({
                    "group": g, "key": k, "branch_a": masked_value(x), "branch_b": masked_value(y),
                }));
            }
            (Some(_), None) | (None, Some(_)) => missing.push(format!("{g}/{k}")),
            _ => {}
        }
    }
    Ok(Json(
        serde_json::json!({ "diffs": diffs, "missing": missing }),
    ))
}

#[derive(Deserialize)]
struct PromoteReq {
    from: String,
    to: String,
    /// 限定 item（"group/key"）；缺省=全部
    items: Option<Vec<String>>,
    #[serde(default)]
    force: bool,
}

/// 值提升（design-v2 §4.8）：把源分支活动版本值写入目标分支草稿（不发布）。
/// 目标草稿已修改项默认跳过（force=true 覆盖）；items 中源分支没有的进入 missing_from。
async fn promote(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    AxumPath(pid): AxumPath<String>,
    Json(req): Json<PromoteReq>,
) -> ApiResult<serde_json::Value> {
    let pid_obj = ProjectId(pid.clone());
    let from_b = BranchName(req.from.clone());
    let to_b = BranchName(req.to.clone());
    let filter: Option<Vec<(String, String)>> = req.items.as_ref().map(|items| {
        items
            .iter()
            .filter_map(|s| {
                s.split_once('/')
                    .map(|(g, k)| (g.to_string(), k.to_string()))
            })
            .collect()
    });
    let (updates, applied, skipped, missing_from) = {
        let sm = app.sm.read().map_err(lock_err)?;
        let src = sm
            .get_config(&pid_obj, &from_b, 0)
            .map_err(ApiError::from)?;
        let dst = sm
            .get_branch_state(&pid_obj, &to_b)
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError(dsh_core::Error::not_found("target branch")))?;
        // 目标结构中的引用项只读：跳过（值由共享库物化，写入本地草稿会被拒）
        let ref_keys: std::collections::HashSet<(String, String)> = sm
            .get_structure(&pid_obj)
            .map_err(ApiError::from)?
            .map(|st| {
                st.groups
                    .iter()
                    .flat_map(|g| {
                        g.items
                            .iter()
                            .filter(|i| i.shared)
                            .map(move |i| (g.name.clone(), i.key.clone()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut updates = Vec::new();
        let mut applied = Vec::new();
        let mut skipped = Vec::new();
        let mut missing_from = Vec::new();
        for (g, items) in &src.groups {
            for (k, v) in items {
                if let Some(f) = &filter {
                    if !f.contains(&(g.clone(), k.clone())) {
                        continue;
                    }
                }
                let key = format!("{g}/{k}");
                if ref_keys.contains(&(g.clone(), k.clone())) {
                    skipped.push(key);
                    continue;
                }
                let draft_modified = dst.value_draft.get(g).is_some_and(|m| m.contains_key(k));
                if draft_modified && !req.force {
                    skipped.push(key);
                    continue;
                }
                updates.push(DraftUpdateItem {
                    group: g.clone(),
                    key: k.clone(),
                    value: v.clone(),
                });
                applied.push(key);
            }
        }
        if let Some(f) = &filter {
            for (g, k) in f {
                if !src.groups.get(g).is_some_and(|m| m.contains_key(k)) {
                    missing_from.push(format!("{g}/{k}"));
                }
            }
        }
        (updates, applied, skipped, missing_from)
    };
    if !updates.is_empty() {
        app.publish
            .update_draft(
                &pid_obj,
                &to_b,
                updates,
                vec![],
                vec![],
                None,
                &principal_op(&principal),
            )
            .await
            .map_err(ApiError::from)?;
    }
    app.audit
        .append(
            "promote",
            Some(pid.clone()),
            Some(req.to.clone()),
            None,
            None,
            serde_json::json!({
                "from": req.from,
                "applied": applied.len(),
                "skipped": skipped.len(),
                "missing_from": missing_from.len(),
            }),
            &principal_op(&principal),
        )
        .await;
    Ok(Json(serde_json::json!({
        "applied": applied,
        "skipped": skipped,
        "missing_from": missing_from,
    })))
}

// ---------------- 共享库（openapi 契约补全；core 已支持，补 HTTP 面） ----------------

/// 共享项请求（对齐 openapi SharedItem；version 由状态机分配）。
#[derive(Deserialize)]
struct SharedItemReq {
    key: String,
    r#type: ValueType,
    #[serde(default)]
    secret: bool,
    #[serde(default)]
    required: bool,
    #[serde(default)]
    description: Option<String>,
    value: Value,
}

fn masked_shared_value(item: &SharedItem) -> serde_json::Value {
    if item.secret {
        serde_json::json!({ "type": "string", "str_value": "***", "masked": true })
    } else {
        serde_json::json!(item.value)
    }
}

fn shared_item_json(
    item: &SharedItem,
    refs: Option<&[(ProjectId, BranchName, String, String)]>,
) -> serde_json::Value {
    let mut obj = serde_json::json!({
        "key": item.key,
        "type": item.ty,
        "secret": item.secret,
        "required": item.required,
        "value": masked_shared_value(item),
        "version": item.version,
    });
    if let Some(d) = &item.description {
        obj["description"] = serde_json::Value::String(d.clone());
    }
    if let Some(r) = refs {
        obj["refs"] = serde_json::json!(
            r.iter()
                .map(|(p, b, g, k)| serde_json::json!({
                    "project": p.as_str(),
                    "branch": b.as_str(),
                    "group": g,
                    "item_key": k,
                }))
                .collect::<Vec<_>>()
        );
    }
    obj
}

/// 写共享草稿（secret 项提交前加密，I8）。
async fn write_shared_draft(
    app: &ApiState,
    req: SharedItemReq,
    action: &str,
) -> Result<serde_json::Value, (StatusCode, Json<ApiErrorBody>)> {
    let mut value = req.value;
    let mut keep_cipher = false;
    if req.secret {
        // F9：secret 共享项只接受 secret 类型的字符串值——非字符串值无法加密，
        // 明文落库后会经 SharedPublish 级联进项目分支并在数据面明文暴露。
        if req.r#type != ValueType::Secret {
            return Err(ApiError(dsh_core::Error::validation(
                "secret 共享项 type 必须为 secret",
            ))
            .into());
        }
        match &value {
            // shared-edit-ui：value 空字符串 = 保留当前生效密文（get_shared_effective
            // 草稿优先回落已发布）——仅改描述/required 的编辑无需重输密钥。
            Value::String(s) if s.is_empty() => {
                let cur = app
                    .sm
                    .read()
                    .map_err(lock_err)?
                    .get_shared_effective(&req.key)
                    .map_err(ApiError::from)?;
                match cur {
                    Some(c) => match c.value {
                        Value::Secret(ct) => {
                            value = Value::Secret(ct); // 保留既有密文（草稿优先）
                            keep_cipher = true;
                        }
                        _ => {
                            return Err(ApiError(dsh_core::Error::validation(
                                "该共享项当前非 secret，请先输入明文值",
                            ))
                            .into())
                        }
                    },
                    None => {
                        return Err(ApiError(dsh_core::Error::validation(
                            "secret 共享项首次保存必须输入值",
                        ))
                        .into())
                    }
                }
            }
            Value::String(_) => { /* 有明文，走下方加密 */ }
            _ => {
                return Err(
                    ApiError(dsh_core::Error::validation("secret 共享项值必须为字符串")).into(),
                )
            }
        }
        if !keep_cipher {
            let cipher = app.cipher.as_ref().ok_or_else(|| {
                ApiError(dsh_core::Error::validation(
                    "secret 共享项需要主密钥（--master-key-file 或 DSH_MASTER_KEY）",
                ))
            })?;
            let plain = match &value {
                Value::String(s) => s.clone(),
                _ => unreachable!("secret 空值路径已分流"),
            };
            let ct = cipher
                .encrypt_secret(plain.as_bytes())
                .map_err(|e| ApiError(dsh_core::Error::internal(format!("encrypt: {e}"))))?;
            value = Value::Secret(ct);
        }
    } else if req.r#type == ValueType::Secret {
        return Err(ApiError(dsh_core::Error::validation(
            "type=secret 的共享项必须标记 secret=true",
        ))
        .into());
    }
    if let Some(d) = &req.description {
        if d.len() > dsh_core::limits::MAX_DESC_BYTES {
            return Err(ApiError(dsh_core::Error::validation(
                "描述超过上限（200 字节）",
            ))
            .into());
        }
    }
    let item = SharedItem {
        key: req.key.clone(),
        ty: req.r#type,
        secret: req.secret,
        required: req.required,
        value,
        version: 0,
        description: req.description.clone(),
    };
    app.write(
        &Command::SharedDraftUpdate {
            item,
            operator: "admin".to_string(),
        },
        now_ms(),
    )
    .await
    .map_err(Into::<(StatusCode, Json<ApiErrorBody>)>::into)?;
    app.audit
        .append(
            action,
            None,
            None,
            None,
            None,
            serde_json::json!({ "key": req.key }),
            "admin",
        )
        .await;
    Ok(serde_json::json!({
        "saved": true,
        "key": req.key,
    }))
}

async fn create_shared(
    State(app): State<ApiState>,
    Json(req): Json<SharedItemReq>,
) -> ApiResult<serde_json::Value> {
    write_shared_draft(&app, req, "shared_draft_update")
        .await
        .map(Json)
}

async fn update_shared_draft(
    State(app): State<ApiState>,
    Json(req): Json<SharedItemReq>,
) -> ApiResult<serde_json::Value> {
    write_shared_draft(&app, req, "shared_draft_update")
        .await
        .map(Json)
}

async fn list_shared(
    State(app): State<ApiState>,
    principal: axum::Extension<dsh_core::Principal>,
) -> ApiResult<serde_json::Value> {
    let sm = app.sm.read().map_err(lock_err)?;
    let items = sm
        .list_shared_published()
        .map_err(ApiError::from)?;
    let out = items
        .iter()
        .map(|item| {
            // N11：PA 只读共享列表时 refs 过滤为仅自己项目（防跨项目元数据读取）
            let refs = match &*principal {
                dsh_core::Principal::ProjectAdmin { project, .. } => sm
                    .shared_usage(&item.key)
                    .ok()
                    .map(|all| {
                        all.into_iter()
                            .filter(|(p, _, _, _)| p.as_str() == project.0)
                            .collect::<Vec<_>>()
                    }),
                _ => sm.shared_usage(&item.key).ok(),
            };
            shared_item_json(item, refs.as_deref())
        })
        .collect::<Vec<_>>();
    Ok(Json(serde_json::json!(out)))
}

async fn list_shared_drafts(State(app): State<ApiState>) -> ApiResult<serde_json::Value> {
    let sm = app.sm.read().map_err(lock_err)?;
    let items = sm
        .list_shared_drafts()
        .map_err(ApiError::from)?
        .iter()
        .map(|item| shared_item_json(item, None))
        .collect::<Vec<_>>();
    Ok(Json(serde_json::json!(items)))
}

async fn publish_shared(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    Json(req): Json<PublishReq>,
) -> ApiResult<serde_json::Value> {
    let rid = req.request_id.unwrap_or_else(new_request_id);
    let outcome = app
        .write(
            &Command::SharedPublish {
                comment: req.comment,
                request_id: rid.clone(),

                operator: principal_op(&principal),
                ts: now_ms(),
                // G1/D36/D35：从节点配置注入（CLI --shared-cascade / --publish-policy）
                cascade: app.publish.shared_cascade,
                policy: app.publish.publish_policy,
            },
            now_ms(),
        )
        .await?;
    let mut affected = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for e in &outcome.events {
        let k = (
            e.project.as_str().to_string(),
            e.branch.as_str().to_string(),
        );
        if seen.insert(k) {
            affected.push(serde_json::json!({
                "project": e.project.as_str(),
                "branch": e.branch.as_str(),
                "new_version": e.version,
            }));
        }
    }
    let max_version = {
        let sm = app.sm.read().map_err(lock_err)?;
        sm.list_shared_published()
            .map_err(ApiError::from)?
            .iter()
            .map(|i| i.version)
            .max()
            .unwrap_or(0)
    };
    app.audit
        .append(
            "shared_publish",
            None,
            None,
            None,
            Some(rid.clone()),
            serde_json::json!({ "affected": affected.len() }),
            &principal_op(&principal),
        )
        .await;
    Ok(Json(serde_json::json!({
        "version": max_version,
        "affected": affected,
        "request_id": rid,
    })))
}

// ---------------- 共享项删除（补全 CRUD 的 D） ----------------

async fn delete_shared(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    axum::extract::Path(key): axum::extract::Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiErrorBody>)> {
    app.write(
        &Command::SharedDelete {
            key: key.clone(),
            operator: principal_op(&principal),
        },
        now_ms(),
    )
    .await?;
    app.audit
        .append(
            "shared_delete",
            None,
            None,
            None,
            None,
            serde_json::json!({ "key": key }),
            &principal_op(&principal),
        )
        .await;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_shared_draft(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    axum::extract::Path(key): axum::extract::Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiErrorBody>)> {
    app.write(
        &Command::SharedDelete {
            key: key.clone(),
            operator: principal_op(&principal),
        },
        now_ms(),
    )
    .await?;
    app.audit
        .append(
            "shared_delete",
            None,
            None,
            None,
            None,
            serde_json::json!({ "key": key, "draft": true }),
            &principal_op(&principal),
        )
        .await;
    Ok(StatusCode::NO_CONTENT)
}

/// secret 值处理策略（§7.6）：reveal=false 一律掩码；reveal=true 解密（需会话 + 审计）。
/// 数据面（HTTP snapshot / gRPC）与渲染/导出默认掩码 —— 与 proto masked 语义及 gRPC 行为一致。
fn apply_secret_policy(
    groups: &mut std::collections::BTreeMap<String, std::collections::BTreeMap<String, Value>>,
    cipher: Option<&Cipher>,
    reveal: bool,
) {
    for items in groups.values_mut() {
        for v in items.values_mut() {
            if let Value::Secret(ct) = v {
                *v = if reveal {
                    match cipher.and_then(|c| c.decrypt_secret(ct).ok()) {
                        Some(p) => Value::String(String::from_utf8_lossy(&p).into_owned()),
                        None => Value::String("***".into()),
                    }
                } else {
                    Value::String("***".into())
                };
            }
        }
    }
}

#[derive(Deserialize)]
struct ConfigQuery {
    #[serde(default)]
    reveal: bool,
}

/// 管理面查看配置（GET /api/v1/projects/{p}/branches/{b}/config）：
/// secret 默认掩码；reveal=true 解密并审计（会话已由鉴权中间件保证）。
async fn admin_config(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    AxumPath((pid, branch)): AxumPath<(String, String)>,
    axum::extract::Query(q): axum::extract::Query<ConfigQuery>,
) -> ApiResult<ConfigResp> {
    // G1/D37：线性读门控
    app.linearized_read().await?;
    let (version, project, branch_name, structure_version, mut groups) = {
        let sm = app.sm.read().map_err(lock_err)?;
        let snap = sm
            .get_config(&ProjectId(pid.clone()), &BranchName(branch.clone()), 0)
            .map_err(ApiError::from)?;
        (
            snap.version,
            snap.project,
            snap.branch,
            snap.structure_version,
            snap.groups,
        )
    };
    apply_secret_policy(&mut groups, app.cipher.as_deref(), q.reveal);
    if q.reveal {
        app.audit
            .append(
                "config_reveal",
                Some(pid.clone()),
                Some(branch.clone()),
                Some(version),
                None,
                serde_json::json!({}),
                &principal_op(&principal),
            )
            .await;
    }
    Ok(Json(ConfigResp {
        project,
        branch: branch_name,
        version,
        resolved_version: version,
        structure_version,
        groups: plain_groups(&groups),
        gray: false, // 管理面 reveal：稳定路径（G3/D28 不 resolve）
    }))
}

// ---------------- 项目管理员账号管理（§5，全局管理员专用；中间件已对 PA 拒绝 /admins 路径）----------------

#[derive(Deserialize)]
struct ProjectAdminCreateReq {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct ProjectAdminResp {
    username: String,
    project: String,
    created_at: i64,
}

/// argon2id 哈希密码 → PHC 字符串（盐内嵌；新格式）。
fn hash_password(password: &str) -> Result<String, dsh_core::Error> {
    use argon2::password_hash::{rand_core::OsRng, PasswordHasher, SaltString};
    let salt = SaltString::generate(&mut OsRng);
    argon2::Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| dsh_core::Error::internal(format!("password hash: {e}")))
}

/// 校验密码：stored 以 "$argon2" 开头 → argon2 验证（盐在 PHC 内）；
/// 否则 legacy：sha256(legacy_salt || password) == stored（旧数据兼容，改密后自动升级）。
fn verify_password(password: &str, stored: &str, legacy_salt: &str) -> bool {
    if stored.starts_with("$argon2") {
        use argon2::password_hash::{PasswordHash, PasswordVerifier};
        match PasswordHash::new(stored) {
            Ok(phc) => argon2::Argon2::default()
                .verify_password(password.as_bytes(), &phc)
                .is_ok(),
            Err(_) => false,
        }
    } else {
        dsh_core::token_hash(&format!("{legacy_salt}{password}")) == stored
    }
}

/// 生成盐 + 密码哈希（§2）。
/// 新格式：PHC 字符串（盐内嵌，salt 字段置空）；旧数据仍为 sha256(salt||pw)，
/// 校验走 verify_password legacy 分支（改密后自动升级到 argon2）。
fn salted_password_hash(password: &str) -> Result<(String, String), dsh_core::Error> {
    let hash = hash_password(password)?;
    Ok((String::new(), hash))
}

async fn create_project_admin(
    State(app): State<ApiState>,
    AxumPath(pid): AxumPath<String>,
    Json(req): Json<ProjectAdminCreateReq>,
) -> Result<(StatusCode, Json<ProjectAdminResp>), (StatusCode, Json<ApiErrorBody>)> {
    let now = now_ms();
    let (salt, hash) = salted_password_hash(&req.password).map_err(ApiError::from)?;
    let cmd = Command::ProjectAdminCreate {
        project: ProjectId(pid.clone()),
        username: req.username.clone(),
        salt,
        password_hash: hash,
        ts: now_ms(),
    };
    match app.write(&cmd, now).await {
        Ok(_) => {
            app.audit
                .append(
                    "project_admin_create",
                    Some(pid.clone()),
                    None,
                    None,
                    None,
                    serde_json::json!({"username": req.username}),
                    "admin",
                )
                .await;
            // 响应用请求值构造（集群模式下 write 返回与本地 apply 读回存在竞态，禁 expect）
            Ok((
                StatusCode::CREATED,
                Json(ProjectAdminResp {
                    username: req.username,
                    project: pid.clone(),
                    created_at: now,
                }),
            ))
        }
        Err(e) if e.0.kind == ErrorKind::Conflict => Err((
            StatusCode::CONFLICT,
            Json(ApiErrorBody {
                code: "ERR_ACCOUNT_EXISTS".into(),
                message: e.0.message.clone(),
                detail: e.0.detail.clone(),
            }),
        )),
        Err(e) => Err(e.into()),
    }
}

async fn list_project_admins(
    State(app): State<ApiState>,
    AxumPath(pid): AxumPath<String>,
) -> ApiResult<serde_json::Value> {
    let sm = app.sm.read().map_err(lock_err)?;
    let accounts = sm
        .list_project_admins(&pid)
        .map_err(ApiError::from)?
        .into_iter()
        .map(|a| serde_json::json!({ "username": a.username, "created_at": a.created_at }))
        .collect::<Vec<_>>();
    Ok(Json(serde_json::json!(accounts)))
}

async fn delete_project_admin(
    State(app): State<ApiState>,
    AxumPath((pid, username)): AxumPath<(String, String)>,
) -> Result<StatusCode, (StatusCode, Json<ApiErrorBody>)> {
    match app
        .write(
            &Command::ProjectAdminDelete {
                username: username.clone(),
            },
            now_ms(),
        )
        .await
    {
        Ok(_) => {
            app.audit
                .append(
                    "project_admin_delete",
                    Some(pid),
                    None,
                    None,
                    None,
                    serde_json::json!({"username": username}),
                    "admin",
                )
                .await;
            Ok(StatusCode::NO_CONTENT)
        }
        Err(e) if e.0.kind == ErrorKind::NotFound => Err((
            StatusCode::NOT_FOUND,
            Json(ApiErrorBody {
                code: "ERR_ACCOUNT_NOT_FOUND".into(),
                message: e.0.message.clone(),
                detail: e.0.detail.clone(),
            }),
        )),
        Err(e) => Err(e.into()),
    }
}

#[derive(Deserialize)]
struct CreateProjectTokenReq {
    name: String,
}

/// 创建项目访问令牌（仅全局管理员；PA 由 pa_allowed 拒 403，此处防御性再校验）。
/// 明文 token 复用模块级 new_token()（16B → 32 hex，见路由区定义）。
async fn create_project_token(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    AxumPath(pid): AxumPath<String>,
    Json(req): Json<CreateProjectTokenReq>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ApiErrorBody>)> {
    if !matches!(&*principal, dsh_core::Principal::Admin) {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ApiErrorBody {
                code: "ERR_FORBIDDEN".into(),
                message: "仅全局管理员可管理访问令牌".into(),
                detail: None,
            }),
        ));
    }
    let now = now_ms();
    let raw = new_token();
    let token_hash = dsh_core::token_hash(&raw);
    let cmd = Command::ProjectTokenCreate {
        project: ProjectId(pid.clone()),
        name: req.name.clone(),
        token_hash: token_hash.clone(),
        operator: principal_op(&principal),
        ts: now,
    };
    match app.write(&cmd, now).await {
        Ok(_) => {
            app.audit
                .append(
                    "token_create",
                    Some(pid.clone()),
                    Some(req.name.clone()),
                    None,
                    None,
                    serde_json::json!({}),
                    &principal_op(&principal),
                )
                .await;
            Ok((
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "id": token_hash.chars().take(16).collect::<String>(),
                    "name": req.name,
                    "token": raw,            // 明文仅此一次
                    "created_at": now,
                })),
            ))
        }
        Err(e) if e.0.kind == ErrorKind::Conflict => Err((
            StatusCode::CONFLICT,
            Json(ApiErrorBody {
                code: "ERR_TOKEN_NAME_EXISTS".into(),
                message: e.0.message.clone(),
                detail: e.0.detail.clone(),
            }),
        )),
        Err(e) if e.0.kind == ErrorKind::NotFound => Err((
            StatusCode::NOT_FOUND,
            Json(ApiErrorBody {
                code: "ERR_NOT_FOUND".into(),
                message: e.0.message.clone(),
                detail: e.0.detail.clone(),
            }),
        )),
        Err(e) => Err(e.into()),
    }
}

/// 项目 token 列表（不含明文与 hash；含 revoked 标记）。
async fn list_project_tokens(
    State(app): State<ApiState>,
    AxumPath(pid): AxumPath<String>,
) -> ApiResult<serde_json::Value> {
    let sm = app.sm.read().map_err(lock_err)?;
    let tokens = sm
        .list_project_tokens(&ProjectId(pid.clone()))
        .map_err(ApiError::from)?;
    let out: Vec<serde_json::Value> = tokens
        .into_iter()
        .map(|t| {
            serde_json::json!({
                "id": t.id,
                "name": t.name,
                "created_at": t.created_at,
                "created_by": t.created_by,
                "revoked": t.revoked,
            })
        })
        .collect();
    Ok(Json(serde_json::json!(out)))
}

/// 吊销项目 token（幂等；不存在 → 404）。
async fn delete_project_token(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    AxumPath((pid, tid)): AxumPath<(String, String)>,
) -> Result<StatusCode, (StatusCode, Json<ApiErrorBody>)> {
    if !matches!(&*principal, dsh_core::Principal::Admin) {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ApiErrorBody {
                code: "ERR_FORBIDDEN".into(),
                message: "仅全局管理员可管理访问令牌".into(),
                detail: None,
            }),
        ));
    }
    let now = now_ms();
    match app
        .write(
            &Command::ProjectTokenRevoke {
                project: ProjectId(pid.clone()),
                token_id: tid.clone(),
            },
            now,
        )
        .await
    {
        Ok(_) => {
            app.audit
                .append(
                    "token_revoke",
                    Some(pid.clone()),
                    Some(tid.clone()),
                    None,
                    None,
                    serde_json::json!({}),
                    &principal_op(&principal),
                )
                .await;
            Ok(StatusCode::NO_CONTENT)
        }
        Err(e) if e.0.kind == ErrorKind::NotFound => Err((
            StatusCode::NOT_FOUND,
            Json(ApiErrorBody {
                code: "ERR_NOT_FOUND".into(),
                message: e.0.message.clone(),
                detail: e.0.detail.clone(),
            }),
        )),
        Err(e) => Err(e.into()),
    }
}

#[derive(Deserialize)]
struct ProjectAdminSetPasswordReq {
    password: String,
}

async fn set_project_admin_password(
    State(app): State<ApiState>,
    AxumPath((pid, username)): AxumPath<(String, String)>,
    Json(req): Json<ProjectAdminSetPasswordReq>,
) -> Result<StatusCode, (StatusCode, Json<ApiErrorBody>)> {
    let (salt, hash) = salted_password_hash(&req.password).map_err(ApiError::from)?;
    match app
        .write(
            &Command::ProjectAdminSetPassword {
                username: username.clone(),
                salt,
                password_hash: hash,
            },
            now_ms(),
        )
        .await
    {
        Ok(_) => {
            app.audit
                .append(
                    "project_admin_set_password",
                    Some(pid),
                    None,
                    None,
                    None,
                    serde_json::json!({"username": username}),
                    "admin",
                )
                .await;
            Ok(StatusCode::NO_CONTENT)
        }
        Err(e) if e.0.kind == ErrorKind::NotFound => Err((
            StatusCode::NOT_FOUND,
            Json(ApiErrorBody {
                code: "ERR_ACCOUNT_NOT_FOUND".into(),
                message: e.0.message.clone(),
                detail: e.0.detail.clone(),
            }),
        )),
        Err(e) => Err(e.into()),
    }
}
async fn snapshot(
    State(app): State<ApiState>,
    AxumPath((pid, branch)): AxumPath<(String, String)>,
    headers: axum::http::HeaderMap,
    PeerAddr(peer): PeerAddr,
) -> ApiResult<ConfigResp> {
    // G1/D37：线性读门控（Linear 模式 ReadIndex 后本地读）
    app.linearized_read().await?;
    // G3/D26：数据面身份解析——X-Dsh-Instance / X-Dsh-Labels（k=v,k2=v2 逗号分隔）+ 对端 IP 兜底。
    // 无身份（缺头/空值）→ 空 ClientCtx → resolve 走稳定版（Q2 向后兼容）。
    let ctx = client_ctx_from_headers(&headers, peer);
    let (version, resolved_version, project, branch_name, structure_version, gray, mut groups) = {
        let sm = app.sm.read().map_err(lock_err)?;
        let snap = sm
            .get_config_resolved(
                &ProjectId(pid.clone()),
                &BranchName(branch.clone()),
                0,
                &ctx,
            )
            .map_err(ApiError::from)?;
        (
            snap.version,
            snap.resolved_version,
            snap.project,
            snap.branch,
            snap.structure_version,
            snap.gray,
            snap.groups,
        )
    };
    apply_secret_policy(&mut groups, app.cipher.as_deref(), false);
    Ok(Json(ConfigResp {
        project,
        branch: branch_name,
        version,
        resolved_version,
        structure_version,
        groups: plain_groups(&groups),
        gray,
    }))
}

/// G3/D26：HTTP 身份头解析 → ClientCtx。
/// `X-Dsh-Instance`: 稳定身份键（如 Pod 名）；`X-Dsh-Labels`: `k=v,k2=v2`（逗号分隔；
/// 值内不得含 `,`；`=` 允许但按首个 `=` 切分——SDK 侧保证格式）；重复 key 后者覆盖；
/// 非法段（无 `=`）跳过。
fn client_ctx_from_headers(
    headers: &axum::http::HeaderMap,
    peer: Option<std::net::IpAddr>,
) -> dsh_core::ClientCtx {
    let instance = headers
        .get("x-dsh-instance")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .trim()
        .to_string();
    let mut labels = std::collections::BTreeMap::new();
    if let Some(raw) = headers.get("x-dsh-labels").and_then(|v| v.to_str().ok()) {
        for pair in raw.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            if let Some((k, v)) = pair.split_once('=') {
                let k = k.trim();
                let v = v.trim();
                if !k.is_empty() {
                    labels.insert(k.to_string(), v.to_string());
                }
            }
        }
    }
    dsh_core::ClientCtx {
        instance_id: instance,
        labels,
        ip: peer,
    }
}

async fn version_history(
    State(app): State<ApiState>,
    AxumPath((pid, branch)): AxumPath<(String, String)>,
) -> ApiResult<serde_json::Value> {
    let sm = app.sm.read().map_err(lock_err)?;
    let versions = sm
        .version_history(&ProjectId(pid), &BranchName(branch))
        .map_err(ApiError::from)?;
    Ok(Json(serde_json::to_value(versions).expect("serialize")))
}

/// G1/D37：从 ensure_linearizable 错误中提取 leader http_addr（ForwardToLeader 时）。
fn leader_http_hint(
    e: &dsh_raft::openraft::error::RaftError<
        u64,
        dsh_raft::openraft::error::CheckIsLeaderError<u64, dsh_raft::NodeInfo>,
    >,
) -> Option<String> {
    match e {
        dsh_raft::openraft::error::RaftError::APIError(check) => {
            if let dsh_raft::openraft::error::CheckIsLeaderError::ForwardToLeader(f) = check {
                return f.leader_node.as_ref().map(|n| n.http_addr.clone());
            }
            None
        }
        _ => None,
    }
}

// ---------------- 数据面快照（纯值输出） ----------------

/// Value → 纯 JSON（去掉 type 标签，供 SDK/应用消费）。
fn plain_value(v: &Value) -> serde_json::Value {
    match v {
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Json(s) => serde_json::from_str(s).unwrap_or(serde_json::Value::String(s.clone())),
        Value::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(|s| serde_json::Value::String(s.clone()))
                .collect(),
        ),
        Value::Secret(_) => serde_json::Value::String("***".into()),
    }
}

fn plain_groups(
    groups: &std::collections::BTreeMap<String, std::collections::BTreeMap<String, Value>>,
) -> serde_json::Value {
    let out: serde_json::Map<String, serde_json::Value> = groups
        .iter()
        .map(|(g, items)| {
            let m: serde_json::Map<String, serde_json::Value> = items
                .iter()
                .map(|(k, v)| (k.clone(), plain_value(v)))
                .collect();
            (g.clone(), serde_json::Value::Object(m))
        })
        .collect();
    serde_json::Value::Object(out)
}

// ---------------- 渲染（模块 08） ----------------

#[derive(Deserialize)]
struct RenderQuery {
    #[serde(default = "default_format")]
    format: String,
    /// 目标版本（0=活动版本）
    #[serde(default)]
    version: u64,
    /// 解密 secret 输出（需管理面会话 + 审计；默认掩码）
    #[serde(default)]
    reveal: bool,
}

fn default_format() -> String {
    "yaml".into()
}

/// 渲染配置文档（GET /v1/projects/{p}/branches/{b}/config?format=&version=&reveal=）。
/// secret 策略：管理会话 → 默认掩码、reveal=true 解密（审计）；
/// 数据面项目 token 授权（构建脚本取值，README「构建脚本取值（curl）」）→ **默认解密**（token 即机器凭据）。
/// SDK 快照（/snapshot、gRPC）保持恒掩码（proto masked 语义）。
async fn render_config(
    State(app): State<ApiState>,
    AxumPath((pid, branch)): AxumPath<(String, String)>,
    axum::extract::Query(q): axum::extract::Query<RenderQuery>,
    req: axum::extract::Request,
) -> Result<axum::response::Response, (StatusCode, Json<ApiErrorBody>)> {
    // G1/D37：线性读门控
    app.linearized_read().await.map_err(|e| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiErrorBody {
                code: "ERR_LINEAR_READ".into(),
                message: e.0.message,
                detail: None,
            }),
        )
    })?;
    // G3/D28（Q6）：管理面渲染——明确**不接身份**（不 resolve 灰度）。管理员看到的是
    // 稳定客户端所见（version=0 → active）；要看灰度明文须显式传版本号（G4 gray-status）。
    // reveal=true：校验管理面会话（本端点不在 /api/v1 鉴权中间件覆盖内，手动校验）；
    // 或数据面项目 token 授权（构建脚本取真值，B2：PA 只能 reveal 自己项目）。
    // 区分两种失败：无有效会话 → 401；已认证但越权 → 403。
    let principal = resolve_principal(
        &app,
        req.headers()
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
    );
    let session_ok = match &principal {
        Ok(dsh_core::Principal::Admin) => true,
        Ok(dsh_core::Principal::ProjectAdmin { project, .. }) => project.0 == pid,
        Err(_) => false,
    };
    // 数据面授权：dev token 全局有效；项目访问令牌须匹配本项目（单次 KV 读）
    let data_ok = data_plane_authorized(&app, &req);
    if q.reveal && !data_ok {
        if principal.is_err() {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(ApiErrorBody {
                    code: "ERR_SESSION_EXPIRED".into(),
                    message: "reveal=true 需要管理员会话或本项目数据面 token".into(),
                    detail: None,
                }),
            ));
        }
        if !session_ok {
            return Err((
                StatusCode::FORBIDDEN,
                Json(ApiErrorBody {
                    code: "ERR_FORBIDDEN".into(),
                    message: "项目管理员只能查看本项目的配置明文".into(),
                    detail: None,
                }),
            ));
        }
    }
    // 数据面 token 授权（构建脚本）：无论 reveal 标记，secret 解密返回
    let reveal = q.reveal || data_ok;
    let (version, mut groups) = {
        let sm = app.sm.read().map_err(lock_err)?;
        let snap = sm
            .get_config(
                &ProjectId(pid.clone()),
                &BranchName(branch.clone()),
                q.version,
            )
            .map_err(ApiError::from)?;
        (snap.version, snap.groups)
    };
    apply_secret_policy(&mut groups, app.cipher.as_deref(), reveal);
    if reveal {
        // 审计 operator：会话 principal；数据面 → 项目 token 名（dev-single 开发 token → data-plane:dev-single）
        let operator = match &principal {
            Ok(p) => principal_op(p),
            Err(_) => data_plane_operator(&app, &req),
        };
        app.audit
            .append(
                "config_reveal",
                Some(pid.clone()),
                Some(branch.clone()),
                Some(version),
                None,
                serde_json::json!({ "format": q.format, "via": if data_ok { "data-plane" } else { "session" } }),
                &operator,
            )
            .await;
    }
    let format = Format::parse(&q.format).map_err(ApiError)?;
    let body = Renderer.render(&groups, format).map_err(ApiError)?;
    let mime = match format {
        Format::Yaml => "application/yaml",
        Format::Toml => "application/toml",
        Format::Json => "application/json",
        Format::Env => "text/plain",
    };
    Ok(axum::response::Response::builder()
        .header("content-type", mime)
        .body(axum::body::Body::from(body))
        .expect("response"))
}

// ---------------- 会话（I7） ----------------

#[derive(Deserialize, Serialize)]
struct LoginReq {
    password: String,
    /// 项目管理员用户名（缺省/空 = 全局管理员，向后兼容）。
    #[serde(default)]
    username: Option<String>,
}

#[derive(Serialize)]
struct LoginResp {
    token: String,
    /// "admin" | "project_admin"（调用方感知身份）。
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    project: Option<String>,
}

/// 会话标识（multisession 改造）：随机 32B hex，作会话 key 的一部分 + token 内嵌。
fn new_session_id() -> String {
    new_token()
}

/// 生成管理员 token：adm.{session_id}.{secret}（multisession：sid 内嵌，O(1) 路由到 sess/admin/{sid}）。
fn new_admin_token(session_id: &str) -> String {
    format!("adm.{session_id}.{}", new_token())
}

/// 生成项目管理员 token：pa.{username}.{session_id}.{secret}（multisession）。
fn new_pa_token(username: &str, session_id: &str) -> String {
    format!("pa.{username}.{session_id}.{}", new_token())
}

async fn login(
    PeerAddr(peer): PeerAddr,
    State(app): State<ApiState>,
    headers: axum::http::HeaderMap,
    Json(req): Json<LoginReq>,
) -> Result<Json<LoginResp>, (StatusCode, Json<ApiErrorBody>)> {
    // 登录节流（S6/F4）：节流键 = 对端 socket 地址（不可伪造）；仅当对端命中可信代理
    // CIDR（--trusted-proxy）时才采用 X-Forwarded-For 首值（直连场景忽略 XFF，防伪造绕过/
    // 受害 IP 锁定 DoS）。窗口 600s、窗口内失败 ≥5 即 429；进程内、按节点独立计数。
    let ip = login_throttle_key(&app, &headers, peer);
    if app.login_throttle.blocked(&ip) {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(ApiErrorBody {
                code: "ERR_TOO_MANY_ATTEMPTS".into(),
                message: "登录尝试过多，请稍后再试".into(),
                detail: None,
            }),
        ));
    }
    // 项目管理员登录分支（§3）：username 非空 → 校验 adm/pa/{username}。
    let pa_username = req
        .username
        .as_deref()
        .map(str::trim)
        .filter(|u| !u.is_empty())
        .map(str::to_string);
    if let Some(username) = pa_username {
        return pa_login(app, req, username, &ip, &headers).await;
    }
    // 密码校验：set-password 落状态机后优先；未设置时回退节点配置（--admin-password）。
    let sm_pw_ok = {
        let sm = app.sm.read().map_err(lock_err)?;
        match sm.get_admin_password_hash().ok().flatten() {
            Some(hash) => verify_password(&req.password, &hash, ""),
            None => req.password == app.admin_password.as_ref(),
        }
    };
    if !sm_pw_ok {
        app.login_throttle.record_failure(&ip);
        app.audit
            .append(
                "login_failed",
                None,
                None,
                None,
                None,
                serde_json::json!({}),
                "admin",
            )
            .await;
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ApiErrorBody {
                // N4：登录失败统一码（与 403 ERR_FORBIDDEN 区分），文案不区分账号是否存在（防枚举）
                code: "ERR_BAD_CREDENTIALS".into(),
                message: "账号或密码错误".into(),
                detail: None,
            }),
        ));
    }
    // 会话落 Raft 状态机（I7）：token 明文只在响应中返回一次，状态机仅存 SHA-256 哈希。
    // multisession 改造：token 带 session_id（adm.{sid}.{secret}），MultiSessionLogin 多会话并存（不 409）。
    // 非 leader 时跟随 leader_hint 转发到 leader 的公开 login 端点（跨节点唯一）。
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let session_id = new_session_id();
        let token = new_admin_token(&session_id);
        let hash = dsh_core::token_hash(&token);
        let ttl = app.session_ttl;
        let now = now_ms();
        let res = app
            .write(
                &Command::MultiSessionLogin {
                    token_hash: hash,
                    issued_at: now,
                    expires_at: (ttl.as_secs() > 0).then(|| now + ttl.as_millis() as i64),
                    session_id,
                },
                now,
            )
            .await;
        match res {
            Ok(_) => {
                app.login_throttle.reset(&ip);
                app.audit
                    .append(
                        "login",
                        None,
                        None,
                        None,
                        None,
                        serde_json::json!({}),
                        "admin",
                    )
                    .await;
                return Ok(Json(LoginResp {
                    token,
                    role: Some("admin".into()),
                    project: None,
                }));
            }
            Err(ApiError(e)) if e.kind == ErrorKind::LeaderRedirect => {
                let hint = e.leader_hint.unwrap_or_default();
                if !hint.is_empty() {
                    // NodeInfo.http_addr 无 scheme（如 127.0.0.1:8601）→ 转发前补 http://
                    let base = if hint.starts_with("http://") || hint.starts_with("https://") {
                        hint
                    } else {
                        format!("http://{hint}")
                    };
                    let client = forward_request(&base, "/api/v1/login", &headers);
                    // N1：转发体透传 username（PA 登录不能在非 leader 节点被当 admin 路径）
                    let fwd = LoginReq {
                        password: req.password.clone(),
                        username: req.username.clone(),
                    };
                    match client.json(&fwd).send().await {
                        Ok(resp) => {
                            let status = resp.status();
                            let body: serde_json::Value =
                                resp.json().await.unwrap_or(serde_json::json!({}));
                            if status.is_success() {
                                app.login_throttle.reset(&ip);
                                let token = body
                                    .get("token")
                                    .and_then(|t| t.as_str())
                                    .unwrap_or_default()
                                    .to_string();
                                let role = body
                                    .get("role")
                                    .and_then(|r| r.as_str())
                                    .map(str::to_string);
                                let project = body
                                    .get("project")
                                    .and_then(|p| p.as_str())
                                    .map(str::to_string);
                                return Ok(Json(LoginResp {
                                    token,
                                    role,
                                    project,
                                }));
                            }
                            // 409 ERR_SESSION_IN_USE 等：原样转发 leader 的错误体
                            let code = body
                                .get("code")
                                .and_then(|c| c.as_str())
                                .unwrap_or("ERR_INTERNAL")
                                .to_string();
                            let message = body
                                .get("message")
                                .and_then(|m| m.as_str())
                                .unwrap_or("login failed")
                                .to_string();
                            let detail = body.get("detail").cloned();
                            return Err((
                                status,
                                Json(ApiErrorBody {
                                    code,
                                    message,
                                    detail,
                                }),
                            ));
                        }
                        Err(_) => { /* leader 转发失败 → 重试 */ }
                    }
                }
            }
            Err(e) => return Err(e.into()),
        }
        if tokio::time::Instant::now() >= deadline {
            return Err((
                StatusCode::GATEWAY_TIMEOUT,
                Json(ApiErrorBody {
                    code: "ERR_LEADER_REDIRECT".into(),
                    message: "login forwarding to leader timed out".into(),
                    detail: None,
                }),
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// 项目管理员登录（§3）：加盐哈希校验 → PaSessionLogin（apply 判 SessionInUse，不读时钟）；
/// SessionInUse 时复查会话已过期则先登出重试一轮（N13，过期重登在 API 层组合）。
async fn pa_login(
    app: ApiState,
    req: LoginReq,
    username: String,
    ip: &str,
    headers: &axum::http::HeaderMap,
) -> Result<Json<LoginResp>, (StatusCode, Json<ApiErrorBody>)> {
    // 校验账号与密码（统一 401，防枚举）
    let account = {
        let sm = app.sm.read().map_err(lock_err)?;
        sm.get_project_admin(&username).ok().flatten()
    };
    let ok = account
        .as_ref()
        .map(|acct| verify_password(&req.password, &acct.password_hash, &acct.salt))
        .unwrap_or(false);
    if !ok {
        app.login_throttle.record_failure(ip);
        app.audit
            .append(
                "login_failed",
                None,
                None,
                None,
                None,
                serde_json::json!({"username": username}),
                &format!("pa:{username}"),
            )
            .await;
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ApiErrorBody {
                code: "ERR_BAD_CREDENTIALS".into(),
                message: "账号或密码错误".into(),
                detail: None,
            }),
        ));
    }
    let project = account.map(|a| a.project.0).unwrap_or_default();
    let operator = format!("pa:{username}");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        // multisession 改造：token 带 sid（pa.{username}.{sid}.{secret}），MultiPaSessionLogin 多会话并存（不 409）
        let session_id = new_session_id();
        let token = new_pa_token(&username, &session_id);
        let hash = dsh_core::token_hash(&token);
        let ttl = app.session_ttl;
        let now = now_ms();
        let res = app
            .write(
                &Command::MultiPaSessionLogin {
                    username: username.clone(),
                    token_hash: hash,
                    issued_at: now,
                    expires_at: (ttl.as_secs() > 0).then(|| now + ttl.as_millis() as i64),
                    device_id: "cli".into(),
                    session_id,
                },
                now,
            )
            .await;
        match res {
            Ok(_) => {
                app.login_throttle.reset(ip);
                app.audit
                    .append(
                        "login",
                        Some(project.clone()),
                        None,
                        None,
                        None,
                        serde_json::json!({"username": username}),
                        &operator,
                    )
                    .await;
                return Ok(Json(LoginResp {
                    token,
                    role: Some("project_admin".into()),
                    project: Some(project),
                }));
            }
            Err(ApiError(e)) if e.kind == ErrorKind::LeaderRedirect => {
                // 非 leader：转发到 leader（透传 username）
                let hint = e.leader_hint.unwrap_or_default();
                if !hint.is_empty() {
                    let base = if hint.starts_with("http://") || hint.starts_with("https://") {
                        hint
                    } else {
                        format!("http://{hint}")
                    };
                    let client = forward_request(&base, "/api/v1/login", headers);
                    let fwd = LoginReq {
                        password: req.password.clone(),
                        username: Some(username.clone()),
                    };
                    if let Ok(resp) = client.json(&fwd).send().await {
                        let status = resp.status();
                        let body: serde_json::Value =
                            resp.json().await.unwrap_or(serde_json::json!({}));
                        if status.is_success() {
                            app.login_throttle.reset(ip);
                            return Ok(Json(LoginResp {
                                token: body
                                    .get("token")
                                    .and_then(|t| t.as_str())
                                    .unwrap_or_default()
                                    .to_string(),
                                role: body
                                    .get("role")
                                    .and_then(|r| r.as_str())
                                    .map(str::to_string),
                                project: body
                                    .get("project")
                                    .and_then(|p| p.as_str())
                                    .map(str::to_string),
                            }));
                        }
                        let code = body
                            .get("code")
                            .and_then(|c| c.as_str())
                            .unwrap_or("ERR_INTERNAL")
                            .to_string();
                        let message = body
                            .get("message")
                            .and_then(|m| m.as_str())
                            .unwrap_or("login failed")
                            .to_string();
                        let detail = body.get("detail").cloned();
                        return Err((
                            status,
                            Json(ApiErrorBody {
                                code,
                                message,
                                detail,
                            }),
                        ));
                    }
                }
            }
            Err(e) => return Err(e.into()),
        }
        if tokio::time::Instant::now() >= deadline {
            return Err((
                StatusCode::GATEWAY_TIMEOUT,
                Json(ApiErrorBody {
                    code: "ERR_LEADER_REDIRECT".into(),
                    message: "login forwarding to leader timed out".into(),
                    detail: None,
                }),
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

async fn logout(
    State(app): State<ApiState>,
    headers: axum::http::HeaderMap,
    principal: axum::Extension<dsh_core::Principal>,
) -> Result<StatusCode, (StatusCode, Json<ApiErrorBody>)> {
    let operator = principal_op(&principal);
    // multisession：token 带 sid → 只删自己的会话；旧格式（无 sid）→ 旧单会话命令
    let sid = token_session_id(
        headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
    );
    match principal.0 {
        dsh_core::Principal::Admin => match sid {
            Some(sid) => {
                app.write(&Command::MultiSessionLogout { session_id: sid }, now_ms())
                    .await?;
            }
            None => {
                app.write(&Command::SessionLogout, now_ms()).await?;
            }
        },
        dsh_core::Principal::ProjectAdmin { ref username, .. } => match sid {
            Some(sid) => {
                app.write(
                    &Command::MultiPaSessionLogout {
                        username: username.clone(),
                        session_id: sid,
                    },
                    now_ms(),
                )
                .await?;
            }
            None => {
                app.write(
                    &Command::PaSessionLogout {
                        username: username.clone(),
                    },
                    now_ms(),
                )
                .await?;
            }
        },
    }
    app.audit
        .append(
            "logout",
            None,
            None,
            None,
            None,
            serde_json::json!({}),
            &operator,
        )
        .await;
    Ok(StatusCode::NO_CONTENT)
}

/// Principal → 审计 operator 值（"admin" / "pa:{username}"）。
fn principal_op(p: &dsh_core::Principal) -> String {
    match p {
        dsh_core::Principal::Admin => "admin".into(),
        dsh_core::Principal::ProjectAdmin { username, .. } => format!("pa:{username}"),
    }
}

async fn heartbeat(
    State(app): State<ApiState>,
    headers: axum::http::HeaderMap,
    principal: axum::Extension<dsh_core::Principal>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiErrorBody>)> {
    let ttl = app.session_ttl;
    let now = now_ms();
    let expires = (ttl.as_secs() > 0).then(|| now + ttl.as_millis() as i64);
    // multisession：token 带 sid → 续期自己的会话；旧格式 → 旧单会话命令
    let sid = token_session_id(
        headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
    );
    match principal.0 {
        dsh_core::Principal::Admin => match sid {
            Some(sid) => {
                app.write(
                    &Command::MultiSessionHeartbeat {
                        session_id: sid,
                        expires_at: expires,
                    },
                    now,
                )
                .await?;
            }
            None => {
                app.write(
                    &Command::SessionHeartbeat {
                        expires_at: expires,
                    },
                    now,
                )
                .await?;
            }
        },
        dsh_core::Principal::ProjectAdmin { ref username, .. } => match sid {
            Some(sid) => {
                app.write(
                    &Command::MultiPaSessionHeartbeat {
                        username: username.clone(),
                        session_id: sid,
                        expires_at: expires,
                    },
                    now,
                )
                .await?;
            }
            None => {
                app.write(
                    &Command::PaSessionHeartbeat {
                        username: username.clone(),
                        expires_at: expires,
                    },
                    now,
                )
                .await?;
            }
        },
    }
    Ok(Json(serde_json::json!({ "expires_at": expires })))
}

// ---------------- 可观测性（模块 10） ----------------

async fn metrics(State(app): State<ApiState>) -> String {
    metrics_text(&app.sm, app.raft.as_ref(), app.cipher.is_some())
}

/// G5/D32：HTTP 请求计数中间件（进程内 AtomicU64；5xx 为自动回滚钩子信号源）。
/// 置于最外层（最后一个 layer）——healthz/metrics/鉴权失败等全部计入。
async fn count_http(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    dsh_observability::HTTP_REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let resp = next.run(req).await;
    if resp.status().is_server_error() {
        dsh_observability::HTTP_5XX.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    resp
}

async fn readyz(State(app): State<ApiState>) -> Result<Json<serde_json::Value>, StatusCode> {
    if !is_ready(app.raft.as_ref()) {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    let state = app
        .raft
        .as_ref()
        .map(|r| format!("{:?}", r.metrics().borrow().state))
        .unwrap_or_else(|| "dev-single".into());
    Ok(Json(serde_json::json!({
        "status": "ok",
        "state": state,
        "build": { "commit": BUILD_COMMIT, "time": BUILD_TIME.parse::<u64>().unwrap_or(0) },
    })))
}

#[derive(Deserialize)]
struct AuditQuery {
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    since: Option<i64>,
    #[serde(default = "default_audit_limit")]
    limit: usize,
}

fn default_audit_limit() -> usize {
    100
}

/// 审计查询（读状态机落库）。
async fn audit_list(
    State(app): State<ApiState>,
    principal: axum::Extension<dsh_core::Principal>,
    axum::extract::Query(q): axum::extract::Query<AuditQuery>,
) -> ApiResult<serde_json::Value> {
    let sm = app.sm.read().map_err(lock_err)?;
    // PA 强制下推 project 过滤到状态机（§4 + R2：先截断后过滤会让 PA 视图被全局条目冲空）
    let project_filter = match principal.0 {
        dsh_core::Principal::Admin => None,
        dsh_core::Principal::ProjectAdmin { project, .. } => Some(project.0),
    };
    let entries = sm
        .get_audit(
            q.action.as_deref(),
            project_filter.as_deref(),
            q.since,
            q.limit,
        )
        .map_err(ApiError::from)?;
    Ok(Json(serde_json::to_value(entries).expect("serialize")))
}

// ---------------- 集群管理 ----------------

async fn cluster_members(State(app): State<ApiState>) -> ApiResult<serde_json::Value> {
    if app.raft.is_none() {
        return Err(ApiError(dsh_core::Error::not_found("cluster mode")).into());
    }
    Ok(Json(cluster_members_json(app.raft.as_ref(), app.node_id)))
}

/// join 引导调用鉴权：未配置 token 放行；配置后要求 Authorization: Bearer <token> 完全相等。
fn join_token_ok(app: &ApiState, headers: &axum::http::HeaderMap) -> bool {
    match &app.join_token {
        None => true,
        Some(expected) => headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|a| a.strip_prefix("Bearer "))
            .map(|t| t == expected.as_ref())
            .unwrap_or(false),
    }
}

async fn cluster_join(
    State(app): State<ApiState>,
    headers: axum::http::HeaderMap,
    Json(req): Json<JoinReq>,
) -> ApiResult<serde_json::Value> {
    if !join_token_ok(&app, &headers) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ApiErrorBody {
                code: "ERR_UNAUTHORIZED".into(),
                message: "join token required".into(),
                detail: None,
            }),
        ));
    }
    let raft = app
        .raft
        .as_ref()
        .ok_or_else(|| ApiError(dsh_core::Error::not_found("cluster mode")))?;
    // F14：node_id 未占用 + 地址可解析（防重复 node_id 扰乱成员表 / 恶意 raft_addr 触发出站连接）。
    // 幂等 join（崩溃恢复）：
    //   - 已存在且是 voter → 拒绝（重启恢复走本地 resume，无需 join；确认重新注册请先
    //     remove-node 并清空该节点数据目录，否则重启必然失败循环）。
    //   - 已存在但是 learner → 视为幂等成功：说明此前 join 已注册但响应丢失 / 节点崩溃于
    //     追赶中；openraft add_learner 对已存在节点是幂等 re-add（可刷新 NodeInfo），
    //     客户端 resume 后经 Raft 复制追赶即可。
    let (existing, is_voter) = {
        let metrics = raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership();
        (
            membership.nodes().map(|(id, _)| *id).collect::<Vec<u64>>(),
            membership.voter_ids().any(|id| id == req.node_id),
        )
    };
    let already_member = existing.contains(&req.node_id);
    if already_member && is_voter {
        return Err(ApiError(dsh_core::Error::conflict(format!(
            "node_id {} 已是集群 voter（重启恢复会自动 resume，无需 join；确认重新注册请先 remove-node 并清空该节点数据目录）",
            req.node_id
        )))
        .into());
    }
    for (label, addr) in [("http_addr", &req.http_addr), ("raft_addr", &req.raft_addr)] {
        let a = addr.trim();
        if a.is_empty() {
            return Err(ApiError(dsh_core::Error::validation(format!("{label} 不能为空"))).into());
        }
        if a.split(':').count() != 2 {
            return Err(ApiError(dsh_core::Error::validation(format!(
                "{label} 须为 host:port 形式"
            )))
            .into());
        }
    }
    let node = RaftNodeInfo {
        grpc_addr: String::new(),
        http_addr: req.http_addr,
        raft_addr: req.raft_addr,
    };
    raft.add_learner(req.node_id, node, false)
        .await
        .map_err(|e| {
            // 本节点非 leader：返回 428 + leader_hint（与写路径同约定），让 join 客户端
            // 跟随真实 leader 重试——--join 指向 follower 时不必 30s 空转（F14 场景扩展）。
            if let Some(fwd) = e.forward_to_leader::<RaftNodeInfo>() {
                let hint = fwd
                    .leader_node
                    .as_ref()
                    .map(|n| n.http_addr.clone())
                    .or_else(|| dsh_raft::leader_http_addr(raft))
                    .unwrap_or_default();
                return ApiError(
                    dsh_core::Error::new(
                        dsh_core::ErrorKind::LeaderRedirect,
                        "not leader, follow leader_hint",
                    )
                    .with_leader_hint(hint),
                );
            }
            ApiError(dsh_core::Error::internal(e.to_string()))
        })?;
    app.audit
        .append(
            "cluster_join",
            None,
            None,
            None,
            None,
            serde_json::json!({ "node_id": req.node_id, "rejoined": already_member }),
            "admin",
        )
        .await;
    Ok(Json(serde_json::json!({
        "added_learner": req.node_id,
        "rejoined": already_member,
    })))
}

async fn cluster_promote(
    State(app): State<ApiState>,
    Json(req): Json<JoinReq>,
) -> ApiResult<serde_json::Value> {
    let raft = app
        .raft
        .as_ref()
        .ok_or_else(|| ApiError(dsh_core::Error::not_found("cluster mode")))?;
    let metrics = raft.metrics().borrow().clone();
    let mut voters: Vec<u64> = metrics.membership_config.membership().voter_ids().collect();
    if !voters.contains(&req.node_id) {
        voters.push(req.node_id);
    }
    raft.change_membership(voters.clone(), false)
        .await
        .map_err(|e| ApiError(dsh_core::Error::internal(e.to_string())))?;
    app.audit
        .append(
            "cluster_promote",
            None,
            None,
            None,
            None,
            serde_json::json!({ "voters": voters }),
            "admin",
        )
        .await;
    Ok(Json(serde_json::json!({ "voters": voters })))
}

/// 移除节点（openapi /api/v1/cluster/remove）：voter 从投票集剔除（retain=false 一并移出成员表），
/// learner 用 RemoveNodes 直接移除；经 change_membership 提交。
async fn cluster_remove(
    State(app): State<ApiState>,
    Json(req): Json<JoinReq>,
) -> ApiResult<serde_json::Value> {
    use dsh_raft::openraft::ChangeMembers;
    use std::collections::BTreeSet;

    let raft = app
        .raft
        .as_ref()
        .ok_or_else(|| ApiError(dsh_core::Error::not_found("cluster mode")))?;
    let metrics = raft.metrics().borrow().clone();
    let m = metrics.membership_config.membership();
    let mut set = BTreeSet::new();
    set.insert(req.node_id);
    let voter_ids: Vec<u64> = m.voter_ids().collect();
    let node_ids: Vec<u64> = m.nodes().map(|(id, _)| *id).collect();
    let is_voter = voter_ids.contains(&req.node_id);
    let is_learner = node_ids.contains(&req.node_id);
    if !is_voter && !is_learner {
        return Err(ApiError(dsh_core::Error::validation("node not in membership")).into());
    }
    let changes = if is_voter {
        ChangeMembers::RemoveVoters(set)
    } else {
        ChangeMembers::RemoveNodes(set)
    };
    raft.change_membership(changes, false)
        .await
        .map_err(|e| ApiError(dsh_core::Error::internal(e.to_string())))?;
    let voters: Vec<u64> = raft
        .metrics()
        .borrow()
        .membership_config
        .membership()
        .voter_ids()
        .collect();
    app.audit
        .append(
            "cluster_remove",
            None,
            None,
            None,
            None,
            serde_json::json!({ "node_id": req.node_id, "voters": voters }),
            "admin",
        )
        .await;
    Ok(Json(serde_json::json!({ "voters": voters })))
}

// ---------------- 密钥管理（B6） ----------------

#[derive(Deserialize)]
struct RotateKeyReq {
    /// base64 32B 新主密钥
    new_key: String,
}

/// 轮换主密钥：新 KEK 成为当前（旧 KEK 保留，可解旧数据 CRY-002）；触发审计。
/// - 集群模式：命令经 Raft 复制到全部节点（各节点 apply 时经 dsh-raft 的 rotation hook
///   更新本地 keyring 并持久化 ring 文件，最终一致）；非 leader 节点按 login 模式转发到 leader。
/// - dev-single：本地轮换，先持久化 ring 文件、成功后才切换内存（避免文件写失败时内存已切换，
///   导致重启后新密文不可解）。
/// - N4：两种模式都要求 --master-key-file（ring 文件持久化）——仅用 DSH_MASTER_KEY 环境变量
///   时拒绝轮换，避免"内存轮换、重启丢失新 KEK、新密文永久不可解"。
async fn rotate_master_key(
    State(app): State<ApiState>,
    headers: axum::http::HeaderMap,
    Json(req): Json<RotateKeyReq>,
) -> ApiResult<serde_json::Value> {
    let cipher = app
        .cipher
        .as_ref()
        .ok_or_else(|| ApiError(dsh_core::Error::validation("master key not configured")))?;
    // N4：轮换必须能持久化 ring 文件（--master-key-file）；仅环境变量密钥时拒绝，
    // 避免"内存轮换、重启丢失新 KEK、新密文不可解"。
    if app.ring_path.is_none() {
        return Err(ApiError(dsh_core::Error::validation(
            "主密钥轮换需要 --master-key-file（ring 文件持久化）",
        ))
        .into());
    }
    let raw = base64::engine::general_purpose::STANDARD
        .decode(&req.new_key)
        .map_err(|e| ApiError(dsh_core::Error::validation(format!("new_key base64: {e}"))))?;
    if raw.len() != 32 {
        return Err(ApiError(dsh_core::Error::validation("new_key must be 32 bytes")).into());
    }
    let mut kek = [0u8; 32];
    kek.copy_from_slice(&raw);

    // ---------------- 集群模式：经 Raft 复制（各节点 apply 时更新本地 keyring + ring 文件） ----------------
    if app.raft.is_some() {
        // F7b：新 KEK 用当前 KEK 自加密后进命令载荷（Raft 日志不含明文主密钥）
        let kek_enc = match app.cipher.as_ref() {
            Some(c) => {
                dsh_crypto::Cipher::wrap_master_key(c.keyring().current(), &kek).map_err(|e| {
                    ApiError(dsh_core::Error::internal(format!(
                        "wrap new master key: {e}"
                    )))
                })?
            }
            None => return Err(ApiError(dsh_core::Error::validation("集群轮换需要主密钥")).into()),
        };
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let res = app
                .write(
                    &Command::RotateMasterKey {
                        kek: Vec::new(), // 新命令不留明文（旧日志路径）
                        kek_enc: kek_enc.clone(),
                    },
                    now_ms(),
                )
                .await;
            match res {
                Ok(_) => break,
                Err(ApiError(e)) if e.kind == ErrorKind::LeaderRedirect => {
                    let hint = e.leader_hint.unwrap_or_default();
                    if !hint.is_empty() {
                        // NodeInfo.http_addr 无 scheme（如 127.0.0.1:8601）→ 转发前补 http://
                        let base = if hint.starts_with("http://") || hint.starts_with("https://") {
                            hint
                        } else {
                            format!("http://{hint}")
                        };
                        let client =
                            forward_request(&base, "/api/v1/admin/rotate-master-key", &headers);
                        // 转发体原样：{"new_key": ...}（leader 侧完成校验/轮换/审计）
                        match client
                            .json(&serde_json::json!({ "new_key": req.new_key.clone() }))
                            .send()
                            .await
                        {
                            Ok(resp) => {
                                let status = resp.status();
                                let body: serde_json::Value =
                                    resp.json().await.unwrap_or(serde_json::json!({}));
                                if status.is_success() {
                                    return Ok(Json(body));
                                }
                                // 原样转发 leader 的错误体
                                let code = body
                                    .get("code")
                                    .and_then(|c| c.as_str())
                                    .unwrap_or("ERR_INTERNAL")
                                    .to_string();
                                let message = body
                                    .get("message")
                                    .and_then(|m| m.as_str())
                                    .unwrap_or("rotate failed")
                                    .to_string();
                                let detail = body.get("detail").cloned();
                                return Err((
                                    status,
                                    Json(ApiErrorBody {
                                        code,
                                        message,
                                        detail,
                                    }),
                                ));
                            }
                            Err(_) => { /* leader 转发失败 → 重试 */ }
                        }
                    }
                }
                Err(e) => return Err(e.into()),
            }
            if tokio::time::Instant::now() >= deadline {
                return Err((
                    StatusCode::GATEWAY_TIMEOUT,
                    Json(ApiErrorBody {
                        code: "ERR_LEADER_REDIRECT".into(),
                        message: "rotate-master-key forwarding to leader timed out".into(),
                        detail: None,
                    }),
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        // 提交成功：等待本地钩子生效（各节点最终一致；超时也返回 ok + 当前 generation）
        let hook_deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(3000);
        loop {
            let applied = app
                .cipher
                .as_ref()
                .map(|c| c.keyring().entries().contains(&kek))
                .unwrap_or(false);
            if applied || tokio::time::Instant::now() >= hook_deadline {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let generation = app
            .cipher
            .as_ref()
            .map(|c| c.keyring().generation())
            .unwrap_or(0);
        app.audit
            .append(
                "rotate_master_key",
                None,
                None,
                None,
                None,
                serde_json::json!({ "generation": generation }),
                "admin",
            )
            .await;
        return Ok(Json(
            serde_json::json!({ "ok": true, "generation": generation }),
        ));
    }

    // ---------------- dev-single：本地轮换（先持久化后切换内存） ----------------
    let ring = cipher.keyring();
    if !ring.entries().iter().any(|k| k == &kek) {
        let mut new_ring = ring;
        new_ring.push(kek);
        // 先持久化（成功才切换内存，避免重启后新密文不可解）
        if let Some(path) = &app.ring_path {
            dsh_crypto::save_ring(path, &new_ring)
                .map_err(|e| ApiError(dsh_core::Error::internal(e.to_string())))?;
        }
        cipher.rotate_master_key(kek);
    }
    let generation = cipher.keyring().generation();
    app.audit
        .append(
            "rotate_master_key",
            None,
            None,
            None,
            None,
            serde_json::json!({ "generation": generation }),
            "admin",
        )
        .await;
    Ok(Json(
        serde_json::json!({ "ok": true, "generation": generation }),
    ))
}

// ---------------- 管理员运维（P2：force-logout / set-password / snapshot / retention-status） ----------------

#[derive(Deserialize, Default)]
struct ForceLogoutReq {
    /// 指定 username = 踢对应项目管理员账号的全部会话（N16）。
    #[serde(default)]
    username: Option<String>,
    /// 精确踢单个会话（multisession）：管理员会话用管理员 token 的 sid；
    /// 与 username 同时给定时优先按 username+session_id 踢 PA 单会话。
    #[serde(default)]
    session_id: Option<String>,
}

/// 强制下线会话（CLI `dsh admin force-logout` 兜底，design §9.3/I7）。
/// multisession 改造：缺省 = 踢全部管理员会话；username = 踢该 PA 全部会话；
/// session_id = 踢单个管理员会话；username+session_id = 踢该 PA 单个会话。
async fn admin_force_logout(
    State(app): State<ApiState>,
    axum::extract::Json(req): axum::extract::Json<ForceLogoutReq>,
) -> ApiResult<serde_json::Value> {
    let username = req
        .username
        .as_deref()
        .map(str::trim)
        .filter(|u| !u.is_empty());
    let sid = req
        .session_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    match (username, sid) {
        // 精确踢单个管理员会话
        (None, Some(sid)) => {
            app.write(
                &Command::MultiSessionLogout {
                    session_id: sid.to_string(),
                },
                now_ms(),
            )
            .await?;
        }
        // 精确踢单个 PA 会话
        (Some(username), Some(sid)) => {
            app.write(
                &Command::MultiPaSessionLogout {
                    username: username.to_string(),
                    session_id: sid.to_string(),
                },
                now_ms(),
            )
            .await?;
        }
        // 踢该 PA 账号全部会话
        (Some(username), None) => {
            app.write(
                &Command::MultiPaSessionLogoutAll {
                    username: username.to_string(),
                },
                now_ms(),
            )
            .await?;
        }
        // 缺省：踢全部管理员会话
        (None, None) => {
            app.write(&Command::MultiSessionLogoutAll, now_ms()).await?;
        }
    }
    app.audit
        .append(
            "force_logout",
            None,
            None,
            None,
            None,
            serde_json::json!({ "username": req.username, "session_id": req.session_id }),
            "admin",
        )
        .await;
    Ok(Json(serde_json::json!({ "logged_out": true })))
}

#[derive(Deserialize)]
struct SetPasswordReq {
    password: String,
}

#[derive(Deserialize)]
struct ChangeMyPasswordReq {
    /// 当前密码（验证本人；防无验证会话改密）。
    current_password: String,
    /// 新密码（≥6 位）。
    new_password: String,
}

/// 当前密码错误统一响应（401 + ERR_BAD_CREDENTIALS，与登录同码位，防枚举）。
fn bad_current_password() -> (StatusCode, Json<ApiErrorBody>) {
    (
        StatusCode::UNAUTHORIZED,
        Json(ApiErrorBody {
            code: "ERR_BAD_CREDENTIALS".into(),
            message: "当前密码错误".into(),
            detail: None,
        }),
    )
}

/// 自助修改密码（全局管理员 + 项目管理员通用，侧边栏「修改密码」入口；B1：校验当前密码）。
/// - admin：校验全局管理员密码（状态机哈希优先，回退节点配置 --admin-password，同 login）
///   → AdminSetPassword + 踢全部管理员会话（改密即重登）；
/// - PA：校验账号加盐哈希 → ProjectAdminSetPassword（apply 级联收回该账号全部会话）。
/// 只能改自己的密码：主体取自会话扩展（中间件注入），不可由请求体伪造。
async fn change_my_password(
    State(app): State<ApiState>,
    principal: axum::Extension<dsh_core::Principal>,
    Json(req): Json<ChangeMyPasswordReq>,
) -> ApiResult<serde_json::Value> {
    if req.new_password.len() < 6 {
        return Err(ApiError(dsh_core::Error::validation("密码至少 6 位")).into());
    }
    let now = now_ms();
    let operator = principal_op(&principal.0);
    match &principal.0 {
        dsh_core::Principal::Admin => {
            let sm_pw_ok = {
                let sm = app.sm.read().map_err(lock_err)?;
                match sm.get_admin_password_hash().ok().flatten() {
                    Some(hash) => verify_password(&req.current_password, &hash, ""),
                    None => req.current_password == app.admin_password.as_ref(),
                }
            };
            if !sm_pw_ok {
                return Err(bad_current_password());
            }
            let hash = hash_password(&req.new_password).map_err(ApiError::from)?;
            app.write(&Command::AdminSetPassword { password_hash: hash }, now)
                .await?;
            // 改密后强制下线全部管理员会话（旧+新格式全清），与 admin_set_password 一致
            app.write(&Command::MultiSessionLogoutAll, now).await?;
            app.audit
                .append(
                    "set_password_self",
                    None,
                    None,
                    None,
                    None,
                    serde_json::json!({}),
                    &operator,
                )
                .await;
        }
        dsh_core::Principal::ProjectAdmin { username, .. } => {
            let account = {
                let sm = app.sm.read().map_err(lock_err)?;
                sm.get_project_admin(username).ok().flatten()
            };
            let ok = account
                .as_ref()
                .map(|acct| verify_password(&req.current_password, &acct.password_hash, &acct.salt))
                .unwrap_or(false);
            if !ok {
                return Err(bad_current_password());
            }
            let (salt, hash) = salted_password_hash(&req.new_password).map_err(ApiError::from)?;
            app.write(
                &Command::ProjectAdminSetPassword {
                    username: username.clone(),
                    salt,
                    password_hash: hash,
                },
                now,
            )
            .await?;
            // apply 已级联收回该账号全部会话（改密即重登）
            app.audit
                .append(
                    "set_password_self",
                    None,
                    None,
                    None,
                    None,
                    serde_json::json!({ "username": username }),
                    &operator,
                )
                .await;
        }
    }
    Ok(Json(serde_json::json!({ "changed": true })))
}

/// 修改管理员密码（哈希落状态机，集群一致；旧会话失效需重新登录）。
async fn admin_set_password(
    State(app): State<ApiState>,
    Json(req): Json<SetPasswordReq>,
) -> ApiResult<serde_json::Value> {
    if req.password.len() < 6 {
        return Err(ApiError(dsh_core::Error::validation("密码至少 6 位")).into());
    }
    let hash = hash_password(&req.password).map_err(ApiError::from)?;
    app.write(
        &Command::AdminSetPassword {
            password_hash: hash,
        },
        now_ms(),
    )
    .await?;
    // 改密后强制下线全部管理员会话（multisession：旧+新格式全清）
    app.write(&Command::MultiSessionLogoutAll, now_ms()).await?;
    app.audit
        .append(
            "set_password",
            None,
            None,
            None,
            None,
            serde_json::json!({}),
            "admin",
        )
        .await;
    Ok(Json(serde_json::json!({ "changed": true })))
}

/// 触发备份快照：返回状态机全量 KV dump（`dsh admin snapshot` 备份用；恢复走 dump/restore）。
async fn admin_snapshot(State(app): State<ApiState>) -> ApiResult<serde_json::Value> {
    let pairs = {
        let sm = app.sm.read().map_err(lock_err)?;
        sm.dump_all().map_err(ApiError::from)?
    };
    let entries: Vec<serde_json::Value> = pairs
        .iter()
        .map(|(k, v)| {
            serde_json::json!({
                "key": String::from_utf8_lossy(k),
                "value": String::from_utf8_lossy(v),
            })
        })
        .collect();
    app.audit
        .append(
            "snapshot",
            None,
            None,
            None,
            None,
            serde_json::json!({ "entries": entries.len() }),
            "admin",
        )
        .await;
    Ok(Json(serde_json::json!({
        "version": 1,
        "kind": "dsh-state-dump",
        "entries": entries,
    })))
}

/// 保留策略状态（`dsh admin version-retention-status`）：配置值 + 当前版本/审计计数。
async fn admin_retention_status(State(app): State<ApiState>) -> ApiResult<serde_json::Value> {
    let (projects, versions, audits) = {
        let sm = app.sm.read().map_err(lock_err)?;
        let projects = sm.list_projects().map(|p| p.len()).unwrap_or(0);
        let mut versions = 0u64;
        if let Ok(plist) = sm.list_projects() {
            for p in plist {
                if let Ok(bs) = sm.list_branches(&p.id) {
                    for b in bs {
                        if let Ok(Some(st)) = sm.get_branch_state(&p.id, &b) {
                            versions += st.active_version;
                        }
                    }
                }
            }
        }
        let audits = sm
            .get_audit(None, None, None, 1)
            .map(|v| v.first().map(|e| e.seq).unwrap_or(0))
            .unwrap_or(0);
        (projects, versions, audits)
    };
    Ok(Json(serde_json::json!({
        "version_retention": app.version_retention,
        "audit_retention": app.audit_retention,
        "projects": projects,
        "active_versions": versions,
        "audit_entries": audits,
        "hint": "version_retention=0 表示全量保留；audit_retention=0 表示不裁剪",
    })))
}

// ---------------- watch（模块 06） ----------------

#[derive(Deserialize)]
struct WatchQuery {
    /// 断线续传起点：重放该版本之后的历史事件再转实时（0=仅实时）
    #[serde(default)]
    after_version: u64,
}

async fn watch_branch(
    State(app): State<ApiState>,
    AxumPath((pid, branch)): AxumPath<(String, String)>,
    axum::extract::Query(q): axum::extract::Query<WatchQuery>,
) -> Sse<impl Stream<Item = Result<SseEvent, Infallible>>> {
    // after_version > 0：按版本链合成历史事件（相邻快照 diff；与 gRPC Watch 重放一致）
    // D-PRUNED：起点已被版本保留策略裁剪 → force_snapshot（SSE 发 snapshot_required 事件并结束）
    let (replay, force_snapshot) = {
        let mut out: Vec<dsh_core::model::PublishEvent> = Vec::new();
        let mut force = false;
        if q.after_version > 0 {
            // F13：watch 返回 Sse（不可 ?），锁中毒时取内部值继续（只读重放）
            let sm = app.sm.read().unwrap_or_else(|e| e.into_inner());
            let pid = ProjectId(pid.clone());
            let bname = BranchName(branch.clone());
            if let Ok(hist) = sm.version_history(&pid, &bname) {
                if let (Some(min), Some(active)) =
                    (hist.first().map(|r| r.no), hist.last().map(|r| r.no))
                {
                    if q.after_version < min && q.after_version < active {
                        force = true;
                    }
                }
                let mut prev: dsh_core::model::SnapshotMap = Default::default();
                for rec in hist {
                    if rec.no <= q.after_version {
                        continue;
                    }
                    if let Ok(cur) = sm.snapshot_of(&pid, &bname, rec.no) {
                        let diff = dsh_core::diff::compute_diff(&prev, &cur);
                        prev = cur;
                        // D-TYPE：事件类型保真（结构发布/级联不再被标为 value_publish）
                        let ty = rec.event_ty.unwrap_or(if rec.rollback_of.is_some() {
                            dsh_core::model::EventType::Rollback
                        } else {
                            dsh_core::model::EventType::ValuePublish
                        });
                        out.push(dsh_core::model::PublishEvent {
                            project: pid.clone(),
                            branch: bname.clone(),
                            version: rec.no,
                            ty,
                            structure_version: rec.structure_version,
                            comment: rec.comment,
                            request_id: String::new(),
                            changes: diff,
                            // G2/Q3：重放保真——转正（GrayPromote）产生的版本记录 gray=true，
                            // 重放时同样标记，SDK 对 gray 事件不按版本过滤（Q4 方案 b 兜底）。
                            gray: rec.gray,
                        });
                    }
                }
            }
        }
        (out, force)
    };
    watch_sse(app.hub.subscribe(), &pid, &branch, replay, force_snapshot)
}

// ---------------- 工具 ----------------

/// 对端地址提取器（F4）：生产环境经 `into_make_service_with_connect_info` 注入
/// `ConnectInfo<SocketAddr>` 扩展，此处取其对端 IP；单测 oneshot 无该扩展时返回 None
/// （登录节流键回落 "direct"）。避免依赖 axum 对 `Option<Extractor>` 的 FromRequestParts 支持。
#[derive(Debug, Clone, Copy)]
pub struct PeerAddr(pub Option<std::net::IpAddr>);

impl<S: std::marker::Sync> axum::extract::FromRequestParts<S> for PeerAddr {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        let ip = parts
            .extensions
            .get::<ConnectInfo<std::net::SocketAddr>>()
            .map(|c| c.0.ip());
        Ok(PeerAddr(ip))
    }
}

/// 构建到 leader 的转发请求（F8/F4 修复）：客户端带 connect 3s + total 10s 超时
/// （黑洞 leader 不再挂起至 OS TCP 超时）；透传 `X-Forwarded-For` 供 leader 侧按
/// 可信代理策略继续对真实客户端限流。
fn forward_request(
    base: &str,
    path: &str,
    headers: &axum::http::HeaderMap,
) -> reqwest::RequestBuilder {
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(3))
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("reqwest client 构建失败");
    let mut req = client.post(format!("{base}{path}"));
    if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        req = req.header("x-forwarded-for", xff);
    }
    req
}

/// 登录节流键（F4）：对端命中可信代理 CIDR → 用 X-Forwarded-For 首值（经代理转发场景）；
/// 否则用对端 socket IP（直连/不可信代理——伪造 XFF 无效）；对端不可得（如单测 oneshot）→ "direct"。
fn login_throttle_key(
    app: &ApiState,
    headers: &axum::http::HeaderMap,
    peer_ip: Option<std::net::IpAddr>,
) -> String {
    if let Some(ip) = peer_ip {
        if !app.trusted_proxies.is_empty() && app.trusted_proxies.contains(&ip) {
            if let Some(xff) = headers
                .get("x-forwarded-for")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.split(',').next())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                return xff.to_string();
            }
        }
        return ip.to_string();
    }
    "direct".to_string()
}

/// 可信代理 CIDR 集（F4）：仅当请求对端 IP 命中这些网段时，才信任 `X-Forwarded-For` 首值
/// 作为登录节流键；未配置（空集）时一律忽略 XFF，直接用对端 socket 地址（不可伪造）。
#[derive(Debug, Clone, Default)]
pub struct TrustedProxies {
    nets: Vec<(std::net::IpAddr, u8)>,
}

impl TrustedProxies {
    pub fn empty() -> Self {
        Self::default()
    }

    /// 解析逗号分隔的 CIDR 列表（如 "10.0.0.0/8,192.168.1.0/24"；无前缀 = /32 或 /128）。
    pub fn parse(s: &str) -> Result<Self, String> {
        let mut nets = Vec::new();
        for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let (ip_str, prefix) = match part.split_once('/') {
                Some((ip, p)) => {
                    let p: u8 = p.parse().map_err(|e| format!("{part}: 前缀 {e}"))?;
                    (ip, Some(p))
                }
                None => (part, None),
            };
            let ip: std::net::IpAddr =
                ip_str.parse().map_err(|e| format!("{part}: 非法 IP {e}"))?;
            // 按地址族校验前缀范围；缺省前缀 = 单地址
            let prefix = match prefix {
                Some(p) => {
                    let max = match ip {
                        std::net::IpAddr::V4(_) => 32,
                        std::net::IpAddr::V6(_) => 128,
                    };
                    if p > max {
                        return Err(format!("{part}: 前缀须 ≤{max}"));
                    }
                    p
                }
                None => match ip {
                    std::net::IpAddr::V4(_) => 32,
                    std::net::IpAddr::V6(_) => 128,
                },
            };
            nets.push((ip, prefix));
        }
        Ok(Self { nets })
    }

    pub fn is_empty(&self) -> bool {
        self.nets.is_empty()
    }

    /// 对端 IP 是否命中任一可信代理网段（按地址族计算掩码）。
    pub fn contains(&self, ip: &std::net::IpAddr) -> bool {
        self.nets.iter().any(|(net, prefix)| match (ip, net) {
            (std::net::IpAddr::V4(a), std::net::IpAddr::V4(b)) => {
                let mask: u32 = if *prefix == 0 {
                    0
                } else {
                    u32::MAX << (32 - *prefix as u32)
                };
                (u32::from(*a) ^ u32::from(*b)) & mask == 0
            }
            (std::net::IpAddr::V6(a), std::net::IpAddr::V6(b)) => {
                let mask: u128 = if *prefix == 0 {
                    0
                } else {
                    u128::MAX << (128 - *prefix as u32)
                };
                (u128::from(*a) ^ u128::from(*b)) & mask == 0
            }
            // v4-mapped v6 简化为不匹配（配置时用同族地址即可）
            _ => false,
        })
    }
}

/// 登录失败节流（进程内；集群各节点独立计数，MVP 足够）。固定窗口：窗口内失败 ≥ max 即 429。
/// 窗口 600s、上限 5 次；成功登录 reset。集群多节点需前置 LB 层限流（各节点计数独立）。
struct LoginThrottle {
    inner: std::sync::Mutex<std::collections::HashMap<String, (u32, std::time::Instant)>>,
    window: std::time::Duration,
    max_failures: u32,
}

impl LoginThrottle {
    fn new() -> Self {
        Self {
            inner: Default::default(),
            window: std::time::Duration::from_secs(600),
            max_failures: 5,
        }
    }

    /// 已锁定时返回 true（读时顺带清理过期条目：now - last >= window 视为过期清除）。
    fn blocked(&self, key: &str) -> bool {
        let mut map = self.inner.lock().expect("login throttle lock");
        let now = std::time::Instant::now();
        match map.get(key) {
            Some((count, last)) => {
                if now.duration_since(*last) >= self.window {
                    map.remove(key);
                    false
                } else {
                    *count >= self.max_failures
                }
            }
            None => false,
        }
    }

    /// 记录一次失败（写时同样按窗口过期判定：过期则重置计数）。
    fn record_failure(&self, key: &str) {
        let mut map = self.inner.lock().expect("login throttle lock");
        let now = std::time::Instant::now();
        match map.get_mut(key) {
            Some((count, last)) => {
                if now.duration_since(*last) >= self.window {
                    *count = 1;
                    *last = now;
                } else {
                    *count += 1;
                }
            }
            None => {
                map.insert(key.to_string(), (1, now));
            }
        }
    }

    /// 成功登录后清空。
    fn reset(&self, key: &str) {
        self.inner.lock().expect("login throttle lock").remove(key);
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn new_request_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!(
        "auto-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// 会话令牌（128-bit 随机 hex；rand 提供 CSPRNG 熵）。
fn new_token() -> String {
    let b: [u8; 16] = rand::random();
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// 通用写转发（K3s/Service 负载均衡场景）：写请求落到 follower 时，服务端把
/// 428 ERR_LEADER_REDIRECT 透明转发到 leader 的同一路径，客户端无感。
///
/// 设计要点：
/// - 仅 POST/PUT/PATCH/DELETE 且路径以 /api/v1/ 开头才可能转发；
/// - 豁免：/api/v1/login、/api/v1/admin/rotate-master-key（已有内联服务端转发）、
///   /api/v1/cluster/join（428 是「客户端跟随 leader_hint」的设计契约，join 客户端自行跟随）；
/// - 仅当响应为 428 且 body.detail.leader_hint 非空时转发；单次尝试，
///   leader 不可达则回落原 428（客户端行为不变）；
/// - 转发保留原请求头（Authorization 等；会话令牌经 Raft 复制，全集群有效）。
async fn forward_leader_writes(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::http::{header, Method, StatusCode};

    let method = req.method().clone();
    let uri = req.uri().clone();
    let path = uri.path().to_string();
    let is_write = matches!(
        method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    );
    let eligible = is_write
        && path.starts_with("/api/v1/")
        && path != "/api/v1/login"
        && path != "/api/v1/admin/rotate-master-key"
        && path != "/api/v1/cluster/join";
    if !eligible {
        return next.run(req).await;
    }

    // 缓冲请求体（重建交给 handler；转发时复用同一份 bytes）
    let (parts, body) = req.into_parts();
    let req_headers = parts.headers.clone();
    let body_bytes = match axum::body::to_bytes(body, 2 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => {
            return next
                .run(axum::extract::Request::from_parts(
                    parts,
                    axum::body::Body::empty(),
                ))
                .await
        }
    };
    let req = axum::extract::Request::from_parts(
        parts,
        axum::body::Body::from(body_bytes.clone()),
    );
    let resp = next.run(req).await;
    if resp.status() != StatusCode::PRECONDITION_REQUIRED {
        return resp;
    }

    // 解析 428 体 → leader_hint
    let (rparts, rbody) = resp.into_parts();
    let rbytes = match axum::body::to_bytes(rbody, 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => {
            return axum::response::Response::from_parts(rparts, axum::body::Body::empty())
        }
    };
    let hint: Option<String> = serde_json::from_slice::<serde_json::Value>(&rbytes)
        .ok()
        .and_then(|v| {
            v.get("detail")
                .and_then(|d| d.get("leader_hint"))
                .and_then(|h| h.as_str())
                .map(String::from)
        })
        .filter(|h| !h.is_empty());
    let Some(hint) = hint else {
        // 非 LeaderRedirect 的 428（如 join 契约）：原样返回
        return axum::response::Response::from_parts(rparts, axum::body::Body::from(rbytes));
    };

    // 转发到 leader（http_addr 无 scheme → 补 http://）
    let base = if hint.starts_with("http://") || hint.starts_with("https://") {
        hint
    } else {
        format!("http://{hint}")
    };
    let query = uri
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    let target = format!("{base}{path}{query}");

    let client = reqwest::Client::new();
    let fwd = client
        .request(method, &target)
        .headers(req_headers)
        .body(body_bytes)
        .timeout(std::time::Duration::from_secs(10));
    match fwd.send().await {
        Ok(leader_resp) => {
            let status = leader_resp.status();
            let lheaders = leader_resp.headers().clone();
            let lbytes = leader_resp.bytes().await.unwrap_or_default();
            let mut builder = axum::response::Response::builder().status(status);
            for (k, v) in lheaders.iter() {
                if k != header::CONTENT_LENGTH && k != header::TRANSFER_ENCODING {
                    builder = builder.header(k, v);
                }
            }
            builder
                .header("X-Defing-Forwarded-To", &target)
                .body(axum::body::Body::from(lbytes))
                .unwrap_or_else(|_| axum::response::Response::new(axum::body::Body::empty()))
        }
        Err(_) => {
            // leader 不可达：回落原 428（客户端行为不变，可自行重试）
            axum::response::Response::from_parts(rparts, axum::body::Body::from(rbytes))
        }
    }
}

// ---------------- 路由 ----------------

pub fn build_router(app: ApiState) -> Router {
    let mut router = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route("/admin", get(admin_index))
        .route("/admin/{*path}", get(admin_static))
        .route("/api/v1/login", post(login))
        .route("/api/v1/logout", post(logout))
        .route("/api/v1/heartbeat", post(heartbeat))
        .route("/api/v1/me/password", post(change_my_password))
        .route("/api/v1/audit", get(audit_list))
        .route("/api/v1/admin/rotate-master-key", post(rotate_master_key))
        .route("/api/v1/admin/force-logout", post(admin_force_logout))
        .route("/api/v1/admin/set-password", post(admin_set_password))
        .route("/api/v1/admin/snapshot", get(admin_snapshot))
        .route(
            "/api/v1/admin/retention-status",
            get(admin_retention_status),
        )
        .route("/api/v1/projects", get(list_projects).post(create_project))
        .route(
            "/api/v1/projects/{p}/admins",
            get(list_project_admins).post(create_project_admin),
        )
        .route(
            "/api/v1/projects/{p}/tokens",
            get(list_project_tokens).post(create_project_token),
        )
        .route(
            "/api/v1/projects/{p}/tokens/{id}",
            delete(delete_project_token),
        )
        .route(
            "/api/v1/projects/{p}/admins/{u}",
            delete(delete_project_admin).put(set_project_admin_password),
        )
        .route(
            "/api/v1/projects/{p}",
            get(project_detail).delete(delete_project),
        )
        .route(
            "/api/v1/projects/{p}/branches",
            get(list_branches).post(create_branch),
        )
        .route(
            "/api/v1/projects/{p}/branches/{b}",
            get(branch_detail).delete(delete_branch),
        )
        .route("/api/v1/projects/{p}/diff", get(branch_diff))
        .route("/api/v1/projects/{p}/promote", post(promote))
        .route(
            "/api/v1/projects/{p}/branches/{b}/gray-publish",
            post(gray_publish_branch),
        )
        .route(
            "/api/v1/projects/{p}/branches/{b}/gray-promote",
            post(gray_promote_branch),
        )
        .route(
            "/api/v1/projects/{p}/branches/{b}/gray-abort",
            post(gray_abort_branch),
        )
        .route(
            "/api/v1/projects/{p}/branches/{b}/gray-status",
            get(gray_status),
        )
        .route("/api/v1/shared", get(list_shared).post(create_shared))
        .route(
            "/api/v1/shared-draft",
            get(list_shared_drafts).put(update_shared_draft),
        )
        .route("/api/v1/shared/publish", post(publish_shared))
        .route("/api/v1/shared/{key}", delete(delete_shared))
        .route("/api/v1/shared-draft/{key}", delete(delete_shared_draft))
        .route(
            "/api/v1/projects/{p}/structure-draft",
            get(get_structure_draft).put(set_structure_draft),
        )
        .route(
            "/api/v1/projects/{p}/structure-draft/publish",
            post(publish_structure),
        )
        .route(
            "/api/v1/projects/{p}/structure",
            get(get_published_structure),
        )
        .route("/api/v1/projects/{p}/branches/{b}/draft", put(update_draft))
        .route("/api/v1/projects/{p}/branches/{b}/publish", post(publish))
        .route("/api/v1/projects/{p}/branches/{b}/rollback", post(rollback))
        .route(
            "/api/v1/projects/{p}/branches/{b}/config",
            get(admin_config),
        )
        .route(
            "/api/v1/projects/{p}/branches/{b}/versions",
            get(version_history),
        )
        // 数据面快照（SDK get 用；无鉴权，面向应用拉取；secret 脱敏输出）
        .route("/v1/projects/{p}/branches/{b}/snapshot", get(snapshot))
        .route("/v1/projects/{p}/branches/{b}/config", get(render_config))
        .route("/v1/projects/{p}/branches/{b}/watch", get(watch_branch));
    if app.raft.is_some() {
        router = router
            .route("/api/v1/cluster/members", get(cluster_members))
            .route("/api/v1/cluster/join", post(cluster_join))
            .route("/api/v1/cluster/promote", post(cluster_promote))
            .route("/api/v1/cluster/remove", post(cluster_remove));
    }
    router = router.layer(axum::middleware::from_fn_with_state(
        app.clone(),
        auth_middleware,
    ));
    router = router.layer(axum::middleware::from_fn(security_headers));
    // 写转发：包住鉴权+handler，仅对鉴权通过的写响应处理 428（K3s/LB 场景管理面写）
    router = router.layer(axum::middleware::from_fn(forward_leader_writes));
    // G5/D32：HTTP 计数最外层（统计全部请求含 healthz/metrics）
    router = router.layer(axum::middleware::from_fn(count_http));
    router.with_state(app)
}

// ---------------- join token 单元测试（S2） ----------------

#[cfg(test)]
mod join_token_tests {
    use std::sync::{Arc, RwLock};

    use axum::http::{HeaderMap, HeaderValue};

    use dsh_core::{InMemoryStore, StateMachine};
    use dsh_watch::WatchHub;

    use super::{join_token_ok, ApiState};

    /// 用内存态构造 ApiState（仿照 ApiState::new 传参，经 with_retention 注入 join_token）。
    fn state_with_join_token(token: Option<Arc<str>>) -> ApiState {
        ApiState::with_retention(
            Arc::new(RwLock::new(StateMachine::new(Box::new(
                InMemoryStore::new(),
            )))),
            WatchHub::new(),
            None,
            None,
            None,
            std::time::Duration::from_secs(86400),
            "admin-pw".into(),
            None,
            0,
            0,
            token,
            std::sync::Arc::new(super::TrustedProxies::empty()),
            None,
        )
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (_k, v) in pairs {
            h.insert(
                axum::http::header::AUTHORIZATION,
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    #[test]
    fn configured_token_accepts_exact_bearer() {
        let app = state_with_join_token(Some(Arc::from("s3cret")));
        assert!(join_token_ok(
            &app,
            &headers(&[("authorization", "Bearer s3cret")])
        ));
    }

    #[test]
    fn configured_token_rejects_wrong_missing_or_non_bearer() {
        let app = state_with_join_token(Some(Arc::from("s3cret")));
        // 错误 token
        assert!(!join_token_ok(
            &app,
            &headers(&[("authorization", "Bearer wrong")])
        ));
        // 缺头
        assert!(!join_token_ok(&app, &headers(&[])));
        // 非 Bearer 前缀（Basic / 无前缀）
        assert!(!join_token_ok(
            &app,
            &headers(&[("authorization", "Basic s3cret")])
        ));
        assert!(!join_token_ok(
            &app,
            &headers(&[("authorization", "s3cret")])
        ));
    }

    #[test]
    fn no_token_allows_any_header() {
        let app = state_with_join_token(None);
        assert!(join_token_ok(&app, &headers(&[])));
        assert!(join_token_ok(
            &app,
            &headers(&[("authorization", "Bearer whatever")])
        ));
    }
}


// ---------------- PA 授权矩阵单元测试（N11：默认拒绝、显式放行） ----------------

#[cfg(test)]
mod pa_matrix_tests {
    use super::pa_allowed;

    fn pa(project: &str) -> dsh_core::Principal {
        dsh_core::Principal::ProjectAdmin {
            username: "u".into(),
            project: dsh_core::ProjectId(project.into()),
        }
    }

    #[test]
    fn pa_can_read_cluster_members_but_not_manage() {
        let p = pa("order-service");
        // 只读：集群成员端点列表（SDK 连接配置）
        assert!(pa_allowed(&p, "GET", "/api/v1/cluster/members"));
        // 管理操作一律拒绝
        assert!(!pa_allowed(&p, "POST", "/api/v1/cluster/join"));
        assert!(!pa_allowed(&p, "POST", "/api/v1/cluster/promote"));
        assert!(!pa_allowed(&p, "POST", "/api/v1/cluster/remove"));
    }

    #[test]
    fn pa_shared_read_allowed_writes_denied() {
        let p = pa("order-service");
        // 共享库只读放行（草稿页「引用共享」下拉数据源；secret 值已掩码）
        assert!(pa_allowed(&p, "GET", "/api/v1/shared"));
        assert!(pa_allowed(&p, "GET", "/api/v1/shared-draft"));
        // 写仍仅全局管理员
        assert!(!pa_allowed(&p, "POST", "/api/v1/shared"));
        assert!(!pa_allowed(&p, "PUT", "/api/v1/shared-draft"));
        assert!(!pa_allowed(&p, "POST", "/api/v1/shared/publish"));
        assert!(!pa_allowed(&p, "DELETE", "/api/v1/shared/timeout"));
        assert!(!pa_allowed(&p, "DELETE", "/api/v1/shared-draft/timeout"));
        // 全局管理员面
        assert!(!pa_allowed(&p, "POST", "/api/v1/admin/set-password"));
        assert!(!pa_allowed(&p, "GET", "/api/v1/admin/snapshot"));
        // 本项目内 admins 也拒绝
        assert!(!pa_allowed(&p, "GET", "/api/v1/projects/order-service/admins"));
    }

    #[test]
    fn pa_own_project_allowed_cross_project_denied() {
        let p = pa("order-service");
        assert!(pa_allowed(&p, "GET", "/api/v1/projects/order-service/branches"));
        assert!(pa_allowed(&p, "PUT", "/api/v1/projects/order-service/branches/dev/draft"));
        assert!(!pa_allowed(&p, "GET", "/api/v1/projects/other-svc/branches"));
        assert!(!pa_allowed(&p, "DELETE", "/api/v1/projects/order-service"));
    }

    #[test]
    fn pa_self_service_password_change_allowed_only_post() {
        let p = pa("order-service");
        // 自助改密放行（主体取自会话扩展，只能改自己）
        assert!(pa_allowed(&p, "POST", "/api/v1/me/password"));
        // 其他方法/其他面不放行
        assert!(!pa_allowed(&p, "GET", "/api/v1/me/password"));
        assert!(!pa_allowed(&p, "PUT", "/api/v1/me/password"));
        // 账号管理面仍拒绝（改别人密码仍仅全局管理员）
        assert!(!pa_allowed(&p, "PUT", "/api/v1/projects/order-service/admins/u"));
    }
}
// ---------------- 安全加固单元测试（S6 节流 / argon2 密码哈希） ----------------

#[cfg(test)]
mod security_tests {
    use std::time::Duration;

    use super::{hash_password, verify_password, LoginThrottle};

    #[test]
    fn throttle_blocks_after_max_failures() {
        let t = LoginThrottle::new();
        // 默认：窗口 600s、上限 5 次
        for _ in 0..5 {
            assert!(!t.blocked("1.2.3.4"), "前 5 次不应被锁");
            t.record_failure("1.2.3.4");
        }
        assert!(t.blocked("1.2.3.4"), "第 6 次（失败已达上限）应被锁");
        // 其他 IP 不受影响
        assert!(!t.blocked("5.6.7.8"));
    }

    #[test]
    fn throttle_reset_unblocks() {
        let t = LoginThrottle::new();
        for _ in 0..5 {
            t.record_failure("1.2.3.4");
        }
        assert!(t.blocked("1.2.3.4"));
        t.reset("1.2.3.4");
        assert!(!t.blocked("1.2.3.4"), "reset 后不应被锁");
        t.record_failure("1.2.3.4");
        assert!(!t.blocked("1.2.3.4"), "reset 后计数应从零开始");
    }

    #[test]
    fn throttle_window_expiry_clears() {
        // 构造小窗口实例：50ms 窗口、上限 2 次
        let t = LoginThrottle {
            inner: Default::default(),
            window: Duration::from_millis(50),
            max_failures: 2,
        };
        t.record_failure("1.2.3.4");
        t.record_failure("1.2.3.4");
        assert!(t.blocked("1.2.3.4"));
        std::thread::sleep(Duration::from_millis(80));
        assert!(!t.blocked("1.2.3.4"), "窗口过期后应解锁（读时清理）");
        // 窗口过期后写时重置计数：再失败 1 次不立即锁
        t.record_failure("1.2.3.4");
        assert!(!t.blocked("1.2.3.4"));
    }

    #[test]
    fn argon2_phc_roundtrip() {
        let stored = hash_password("s3cret-pw").expect("argon2 hash");
        assert!(
            stored.starts_with("$argon2"),
            "新格式应为 PHC 字符串: {stored}"
        );
        assert!(verify_password("s3cret-pw", &stored, ""));
        assert!(!verify_password("wrong-pw", &stored, ""));
    }

    #[test]
    fn legacy_sha256_compat() {
        // 旧数据：sha256(salt || pw)，无 "$argon2" 前缀 → 走 legacy 分支
        let salt = "deadbeef00cafe00";
        let stored = dsh_core::token_hash(&format!("{salt}old-pw"));
        assert!(!stored.starts_with("$argon2"));
        assert!(verify_password("old-pw", &stored, salt));
        assert!(!verify_password("wrong", &stored, salt));
        // 盐不匹配（如空盐）→ legacy 分支同样不通过
        assert!(!verify_password("old-pw", &stored, ""));
    }

    // ---------------- F4：可信代理 CIDR 与节流键 ----------------

    #[test]
    fn trusted_proxies_match_cidr() {
        use std::net::IpAddr;
        let tp = super::TrustedProxies::parse("10.0.0.0/8,192.168.1.0/24").unwrap();
        assert!(tp.contains(&"10.1.2.3".parse::<IpAddr>().unwrap()));
        assert!(tp.contains(&"192.168.1.9".parse::<IpAddr>().unwrap()));
        assert!(!tp.contains(&"192.168.2.9".parse::<IpAddr>().unwrap()));
        assert!(!tp.contains(&"8.8.8.8".parse::<IpAddr>().unwrap()));
        assert!(super::TrustedProxies::empty().is_empty());
        assert!(super::TrustedProxies::parse("bad-ip").is_err());
        assert!(super::TrustedProxies::parse("10.0.0.0/33").is_err());
        // 单 IP（无前缀 = /32）
        let single = super::TrustedProxies::parse("203.0.113.7").unwrap();
        assert!(single.contains(&"203.0.113.7".parse::<IpAddr>().unwrap()));
        assert!(!single.contains(&"203.0.113.8".parse::<IpAddr>().unwrap()));
    }

    #[test]
    fn throttle_key_ignores_xff_without_trusted_proxy() {
        use axum::http::HeaderMap;
        // 未配置可信代理：即使伪造 XFF，节流键也是对端 IP（不可伪造）
        let app = super::ApiState::with_retention(
            std::sync::Arc::new(std::sync::RwLock::new(dsh_core::StateMachine::new(
                Box::new(dsh_core::InMemoryStore::new()),
            ))),
            dsh_watch::WatchHub::new(),
            None,
            None,
            None,
            std::time::Duration::from_secs(86400),
            "admin-pw".into(),
            None,
            0,
            0,
            None,
            std::sync::Arc::new(super::TrustedProxies::empty()),
            None,
        );
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", "1.2.3.4".parse().unwrap());
        let key = super::login_throttle_key(&app, &h, Some("203.0.113.9".parse().unwrap()));
        assert_eq!(key, "203.0.113.9", "无可信代理时不得信任 XFF");
        // 对端不可得（单测 oneshot）→ "direct"
        let key2 = super::login_throttle_key(&app, &h, None);
        assert_eq!(key2, "direct");
    }

    #[test]
    fn throttle_key_trusts_xff_from_trusted_proxy() {
        use axum::http::HeaderMap;
        let app = super::ApiState::with_retention(
            std::sync::Arc::new(std::sync::RwLock::new(dsh_core::StateMachine::new(
                Box::new(dsh_core::InMemoryStore::new()),
            ))),
            dsh_watch::WatchHub::new(),
            None,
            None,
            None,
            std::time::Duration::from_secs(86400),
            "admin-pw".into(),
            None,
            0,
            0,
            None,
            std::sync::Arc::new(super::TrustedProxies::parse("10.0.0.0/8").unwrap()),
            None,
        );
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", "198.51.100.7".parse().unwrap());
        // 对端是可信代理 → 采用 XFF 首值
        let key = super::login_throttle_key(&app, &h, Some("10.1.1.1".parse().unwrap()));
        assert_eq!(key, "198.51.100.7");
        // 对端不是可信代理 → 忽略 XFF
        let key2 = super::login_throttle_key(&app, &h, Some("203.0.113.9".parse().unwrap()));
        assert_eq!(key2, "203.0.113.9");
    }
}

// ---------------- 共享库编辑（shared-edit-ui）：secret 空值 = 保留密文 ----------------

#[cfg(test)]
mod shared_secret_keep_tests {
    use std::sync::{Arc, RwLock};

    use dsh_core::command::Command;
    use dsh_core::{Ciphertext, InMemoryStore, SharedItem, StateMachine, Value, ValueType};
    use dsh_watch::WatchHub;

    use super::{write_shared_draft, ApiState, SharedItemReq};

    /// 测试用假密文（keep 路径不触碰内容，无需真实加密）。
    fn fake_ct() -> Ciphertext {
        Ciphertext {
            enc: "aes-256-gcm".into(),
            v: 1,
            dek_v: 1,
            nonce: "bm9uY2U=".into(),
            ct: "ZHVtbXk=".into(),
            edek: "ZWRlaw==".into(),
            edek_nonce: "bm9uY2Uy".into(),
        }
    }

    /// 内存态 ApiState（与 join_token_tests 同构造模式；无主密钥——keep 路径不需要）。
    fn state() -> ApiState {
        ApiState::with_retention(
            Arc::new(RwLock::new(StateMachine::new(Box::new(
                InMemoryStore::new(),
            )))),
            WatchHub::new(),
            None,
            None,
            None,
            std::time::Duration::from_secs(86400),
            "admin-pw".into(),
            None,
            0,
            0,
            None,
            std::sync::Arc::new(super::TrustedProxies::empty()),
            None,
        )
    }

    /// 直接经状态机写入种子密文（keep 路径不触碰密文，无需真实主密钥/密文格式）。
    fn seed_secret_draft(app: &ApiState, key: &str, ct: Ciphertext, description: Option<String>) {
        app.sm
            .write()
            .unwrap()
            .apply(
                &Command::SharedDraftUpdate {
                    item: SharedItem {
                        key: key.into(),
                        ty: ValueType::Secret,
                        secret: true,
                        required: false,
                        value: Value::Secret(ct),
                        version: 0,
                        description,
                    },
                    operator: "admin".into(),
                },
                0,
            )
            .unwrap();
    }

    #[tokio::test]
    async fn empty_value_keeps_existing_ciphertext_and_updates_description() {
        let app = state();
        seed_secret_draft(&app, "api-key", fake_ct(), None);

        let res = write_shared_draft(
            &app,
            SharedItemReq {
                key: "api-key".into(),
                r#type: ValueType::Secret,
                secret: true,
                required: false,
                description: Some("新描述".into()),
                value: Value::String("".into()), // 留空 = 保留密文
            },
            "shared_draft_update",
        )
        .await
        .unwrap();

        assert_eq!(res["saved"], true);
        let merged = app
            .sm
            .read()
            .unwrap()
            .get_shared_effective("api-key")
            .unwrap()
            .unwrap();
        assert_eq!(merged.value, Value::Secret(fake_ct()), "密文必须原样保留");
        assert_eq!(merged.description.as_deref(), Some("新描述"), "描述应更新");
    }

    #[tokio::test]
    async fn empty_value_without_existing_item_is_rejected() {
        let app = state();
        let err = write_shared_draft(
            &app,
            SharedItemReq {
                key: "no-such-key".into(),
                r#type: ValueType::Secret,
                secret: true,
                required: false,
                description: None,
                value: Value::String("".into()),
            },
            "shared_draft_update",
        )
        .await
        .unwrap_err();

        assert!(
            err.1 .0.message.contains("首次保存必须输入值"),
            "无既有密文时留空应报错: {}",
            err.1 .0.message
        );
    }

    #[tokio::test]
    async fn empty_value_on_plaintext_item_is_rejected() {
        let app = state();
        // 种子为明文项（非 secret）
        app.sm
            .write()
            .unwrap()
            .apply(
                &Command::SharedDraftUpdate {
                    item: SharedItem {
                        key: "plain-key".into(),
                        ty: ValueType::String,
                        secret: false,
                        required: false,
                        value: Value::String("hello".into()),
                        version: 0,
                        description: None,
                    },
                    operator: "admin".into(),
                },
                0,
            )
            .unwrap();

        let err = write_shared_draft(
            &app,
            SharedItemReq {
                key: "plain-key".into(),
                r#type: ValueType::Secret,
                secret: true,
                required: false,
                description: None,
                value: Value::String("".into()),
            },
            "shared_draft_update",
        )
        .await
        .unwrap_err();

        assert!(
            err.1 .0.message.contains("当前非 secret"),
            "明文项留空转 secret 应报错: {}",
            err.1 .0.message
        );
    }

    #[tokio::test]
    async fn empty_value_with_secret_flag_but_wrong_type_is_rejected() {
        let app = state();
        let err = write_shared_draft(
            &app,
            SharedItemReq {
                key: "k".into(),
                r#type: ValueType::String, // secret=true 但 type 非 secret（既有 F9 校验回归）
                secret: true,
                required: false,
                description: None,
                value: Value::String("".into()),
            },
            "shared_draft_update",
        )
        .await
        .unwrap_err();

        assert!(
            err.1 .0.message.contains("type 必须为 secret"),
            "F9 校验应保持: {}",
            err.1 .0.message
        );
    }
}
