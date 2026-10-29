//! defing 二进制（组装器）：解析 CLI → 装配存储/Raft/状态 → dsh-api 路由 → 监听。
//! HTTP handler 见 dsh-api；发布编排见 dsh-publish；可观测见 dsh-observability；
//! watch 见 dsh-watch；状态机见 dsh-core。本文件仅负责启动装配。

use std::sync::{Arc, RwLock};

use clap::{Parser, Subcommand};
use dsh_api::ApiState;
use dsh_core::{InMemoryStore, StateMachine};
use dsh_crypto::{load_master_key, Cipher};
// 0.10：Raft.metrics() -> openraft rt WatchReceiver trait（changed()/borrow_watched(),
// borrow() gone）
use dsh_raft::openraft::rt::WatchReceiver;
use dsh_raft::{
    HttpNetworkFactory, LogStore, NodeInfo as RaftNodeInfo, RaftServerState, StateMachineStore,
};
use dsh_storage::RedbStorage;
use dsh_watch::WatchHub;

// ---------------- 配置 ----------------

/// `defing admin <子命令>`：管理员运维客户端（design-v2 §13.2 / design-v3 §6）。
#[derive(Subcommand, Debug)]
enum AdminCmd {
    /// 生成新主密钥（base64 32B）并打印指引
    GenMasterKey,
    /// 轮换主密钥（调管理面 API；DEK 重包由后台任务执行）
    RotateMasterKey {
        /// base64 32B 新 KEK
        new_key: String,
    },
    /// 强制下线当前管理员会话（I7 兜底）
    ForceLogout,
    /// 修改管理员密码（旧会话失效；需已持有管理员会话）
    SetPassword {
        /// 新密码（≥6 位）
        password: String,
    },
    /// 忘记管理员密码时免旧密码恢复（需 --ops-token = 节点 --join-token，见 resolve_ops_token）
    ResetPassword {
        /// 新密码（≥6 位）
        password: String,
    },
    /// learner → voter
    Promote {
        #[arg(long)]
        node: u64,
    },
    /// 移除节点
    RemoveNode {
        #[arg(long)]
        node: u64,
    },
    /// 触发备份快照（状态机 KV dump；可 --out 存盘）
    Snapshot {
        #[arg(long)]
        out: Option<String>,
    },
    /// 查看保留策略状态（--version-retention / --audit-retention 配置）
    RetentionStatus,
}

/// 顶层子命令命名空间（当前仅 admin 运维）。
#[derive(Subcommand, Debug)]
enum Command {
    /// 管理员运维（design-v2 §13.2）：gen-master-key / rotate-master-key /
    /// force-logout / set-password / reset-password / promote / remove-node / snapshot / retention-status
    Admin {
        #[command(subcommand)]
        cmd: AdminCmd,
    },
}

#[derive(Parser, Debug)]
#[command(name = "defing", version, about = "Defing 分布式配置文档服务")]
struct Cli {
    /// 单节点联调模式（无 Raft，直接 apply 状态机）
    #[arg(long)]
    dev_single: bool,
    /// 集群模式：节点 ID
    #[arg(long)]
    node_id: Option<u64>,
    /// 集群模式：首节点自举（单节点建群，其余节点用 --join 加入）
    #[arg(long)]
    bootstrap: bool,
    /// 集群模式：静态成员表建群（seed map，推荐）。
    /// 格式：`node_id@raft_addr@http_addr[,node_id@raft_addr@http_addr...]`（三段式必填，
    /// http_addr 用于 leader 重定向/join 跟随）。仅当数据目录为空（首次建群）时生效；
    /// 所有节点必须传【完全相同】的值（不一致会 split-brain）；已有状态自动 resume
    /// （若 seed 与集群成员表不一致会 WARN，不覆盖——运行期成员变更走 join/promote/remove-node）。
    #[arg(long, conflicts_with = "bootstrap", conflicts_with = "join")]
    bootstrap_peers: Option<String>,
    /// 集群模式：加入集群（指定任一实例的 HTTP 端点，如 http://127.0.0.1:8384）
    #[arg(long)]
    join: Option<String>,
    /// HTTP 监听地址（管理面）
    #[arg(long, default_value = "127.0.0.1:8384")]
    http_addr: String,
    /// Raft 内部 RPC 地址
    #[arg(long, default_value = "127.0.0.1:8385")]
    raft_addr: String,
    /// 数据面 gRPC 地址（A1：tonic 服务挂载于此）
    #[arg(long, default_value = "127.0.0.1:8383")]
    grpc_addr: String,
    /// 数据目录（集群模式必填；dev-single 缺省内存）
    #[arg(long)]
    data_dir: Option<String>,
    /// 主密钥文件（raw 32B；或 DSH_MASTER_KEY 环境变量 base64）
    #[arg(long)]
    master_key_file: Option<String>,
    /// 允许无主密钥启动（开发/演示模式；默认拒绝无密钥启动，design-v2 §7.4）
    #[arg(long)]
    allow_no_master_key: bool,
    /// 版本保留数（0=全量保留；后台裁剪任务仅在 >0 时启用）
    #[arg(long, default_value_t = 0)]
    version_retention: u64,
    /// 审计保留条数（0=不裁剪；默认 100k 条，design-v2）
    #[arg(long, default_value_t = 100000)]
    audit_retention: u64,
    /// 进程内广播事件缓冲容量（design-v2 §6.3「最近 10k 事件」；重放仍走版本链）
    #[arg(long, default_value_t = 10000)]
    watch_event_retain: u64,
    /// 灰度自动回滚阈值（本地 HTTP 5xx 比例 %；0=禁用，G5/D33）
    #[arg(long, default_value_t = 0.0)]
    gray_rollback_threshold: f64,
    /// 灰度自动回滚检查间隔秒（测试可调小；默认 60）
    #[arg(long, default_value_t = 60)]
    gray_rollback_interval: u64,
    /// 发布校验策略（G1/D35）：block=校验失败拒绝（默认）| warn=仅记录继续发布
    #[arg(long, value_enum, default_value_t = PolicyArg::Block)]
    publish_policy: PolicyArg,
    /// 共享发布级联模式（G1/D36）：auto=自动级联引用分支（默认）| manual=只更共享版本
    #[arg(long, value_enum, default_value_t = CascadeArg::Auto)]
    shared_cascade: CascadeArg,
    /// 读取模式（G1/D37 修订）：stale=本地直读（默认，零破坏）| linear=ReadIndex 门控
    /// （集群下 follower 读返回 ERR_LEADER_REDIRECT + leader http，客户端跟随）
    #[arg(long, value_enum, default_value_t = ReadArg::Stale)]
    read_mode: ReadArg,
    /// 管理员密码（缺省首启随机生成并打印；admin 客户端模式用于登录）
    #[arg(long, global = true)]
    admin_password: Option<String>,
    /// 会话 TTL 秒数（0 = 不自动过期；默认 24h）
    #[arg(long, default_value_t = 86400)]
    session_ttl: u64,
    /// 集群 join 引导令牌（/api/v1/cluster/join 需 Bearer 匹配；缺省不校验）
    #[arg(long)]
    join_token: Option<String>,
    /// Raft 内部 RPC 共享令牌（缺省不校验；启用后集群内所有节点必须传相同值）
    #[arg(long)]
    raft_token: Option<String>,
    /// 可信代理 CIDR 列表（逗号分隔，如 "10.0.0.0/8,192.168.0.0/16"）：
    /// 仅信任来自这些网段的 X-Forwarded-For 作为登录节流键；未配置时忽略 XFF 用对端地址（F4）
    #[arg(long)]
    trusted_proxy: Option<String>,
    /// 生成新主密钥（base64 32B）并退出
    #[arg(long)]
    gen_master_key: bool,
    /// 轮换主密钥（客户端模式）：向 --admin-endpoint 发起轮换后退出（需 --admin-password）
    #[arg(long)]
    rotate_master_key: Option<String>,
    /// 管理面端点（客户端模式：rotate-master-key / admin <cmd> 用）
    #[arg(long, global = true, default_value = "http://127.0.0.1:8384")]
    admin_endpoint: String,
    /// 管理面会话令牌（客户端模式；缺省时用 --admin-password 登录；单会话下建议直接传 token）
    #[arg(long, global = true)]
    admin_token: Option<String>,
    /// 忘记密码恢复凭据（admin reset-password 用；免旧密码/会话）：值 = 节点 --join-token
    /// （k3s Secret cluster-token / compose DSH_CLUSTER_TOKEN）。缺省回退环境变量
    /// DSH_OPS_TOKEN，再回退 DSH_CLUSTER_TOKEN。
    #[arg(long, global = true)]
    ops_token: Option<String>,
    /// 顶层子命令（defing admin <cmd>；客户端模式，不启动服务）
    #[command(subcommand)]
    cmd: Option<Command>,
}

// ---------------- 工具 ----------------

fn new_token() -> String {
    let b: [u8; 16] = rand::random();
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// 管理员客户端 token：优先 --admin-token（单会话下 login 会 409）；缺省登录获取。
async fn admin_token(cli: &Cli) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(t) = &cli.admin_token {
        return Ok(t.clone());
    }
    let pw = cli
        .admin_password
        .clone()
        .ok_or("需要 --admin-token 或 --admin-password")?;
    let client = reqwest::Client::new();
    let login: serde_json::Value = client
        .post(format!("{}/api/v1/login", cli.admin_endpoint))
        .json(&serde_json::json!({ "password": pw }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let token = login["token"]
        .as_str()
        .ok_or_else(|| "login failed (bad password?)".to_string())?;
    Ok(token.to_string())
}

/// 忘记密码恢复凭据（admin reset-password）：--ops-token 优先；其次环境变量
/// DSH_OPS_TOKEN，再回退 DSH_CLUSTER_TOKEN（compose 用同一变量作 join-token）。
/// 值必须等于节点的 --join-token（k8s Secret cluster-token / compose DSH_CLUSTER_TOKEN）。
fn resolve_ops_token(cli: &Cli) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(t) = &cli.ops_token {
        return Ok(t.clone());
    }
    for var in ["DSH_OPS_TOKEN", "DSH_CLUSTER_TOKEN"] {
        if let Ok(v) = std::env::var(var) {
            if !v.trim().is_empty() {
                return Ok(v);
            }
        }
    }
    Err("reset-password 需要 --ops-token（= 节点 --join-token：k3s Secret cluster-token / compose DSH_CLUSTER_TOKEN）或环境变量 DSH_OPS_TOKEN / DSH_CLUSTER_TOKEN"
        .into())
}

/// `defing admin <cmd>` 分派（客户端模式，调管理面 HTTP）。
async fn run_admin_cmd(
    cli: &Cli,
    cmd: &AdminCmd,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match cmd {
        AdminCmd::GenMasterKey => {
            println!("{}", dsh_crypto::Cipher::generate_master_key());
            return Ok(());
        }
        AdminCmd::RotateMasterKey { new_key } => {
            let token = admin_token(cli).await?;
            let resp = reqwest::Client::new()
                .post(format!(
                    "{}/api/v1/admin/rotate-master-key",
                    cli.admin_endpoint
                ))
                .bearer_auth(&token)
                .json(&serde_json::json!({ "new_key": new_key }))
                .send()
                .await?;
            let status = resp.status();
            let body: serde_json::Value = resp.json().await.unwrap_or_default();
            println!("{body}");
            if !status.is_success() {
                std::process::exit(1);
            }
            return Ok(());
        }
        AdminCmd::ResetPassword { password } => {
            // 忘记密码恢复通道：无需旧密码/管理员会话，仅需 --ops-token（= 节点 --join-token）。
            // 命中 follower 时服务端写转发中间件会透明转 leader。
            let ops_token = resolve_ops_token(cli)?;
            let resp = reqwest::Client::new()
                .post(format!(
                    "{}/api/v1/admin/reset-password",
                    cli.admin_endpoint.trim_end_matches('/')
                ))
                .bearer_auth(&ops_token)
                .json(&serde_json::json!({ "password": password }))
                .send()
                .await?;
            let status = resp.status();
            let body: serde_json::Value = resp.json().await.unwrap_or_default();
            println!("{body}");
            if !status.is_success() {
                eprintln!("重置失败：确认 --ops-token 与节点 --join-token 一致，且节点启动时已配置 --join-token");
                std::process::exit(1);
            }
            eprintln!("管理员密码已重置，旧会话已下线，请用新密码登录");
            return Ok(());
        }
        _ => {}
    }
    let token = admin_token(cli).await?;
    let client = reqwest::Client::new();
    let base = cli.admin_endpoint.trim_end_matches('/');
    match cmd {
        AdminCmd::ForceLogout => {
            let resp = client
                .post(format!("{base}/api/v1/admin/force-logout"))
                .bearer_auth(&token)
                .send()
                .await?;
            let status = resp.status();
            let body: serde_json::Value = resp.json().await.unwrap_or_default();
            println!("{body}");
            if !status.is_success() {
                std::process::exit(1);
            }
        }
        AdminCmd::SetPassword { password } => {
            let resp = client
                .post(format!("{base}/api/v1/admin/set-password"))
                .bearer_auth(&token)
                .json(&serde_json::json!({ "password": password }))
                .send()
                .await?;
            let status = resp.status();
            let body: serde_json::Value = resp.json().await.unwrap_or_default();
            println!("{body}");
            if !status.is_success() {
                std::process::exit(1);
            }
            eprintln!("密码已修改，旧会话已下线，请用新密码重新登录");
        }
        AdminCmd::Promote { node } => {
            let resp = client
                .post(format!("{base}/api/v1/cluster/promote"))
                .bearer_auth(&token)
                .json(&serde_json::json!({ "node_id": node }))
                .send()
                .await?;
            let status = resp.status();
            let body: serde_json::Value = resp.json().await.unwrap_or_default();
            println!("{body}");
            if !status.is_success() {
                std::process::exit(1);
            }
        }
        AdminCmd::RemoveNode { node } => {
            let resp = client
                .post(format!("{base}/api/v1/cluster/remove"))
                .bearer_auth(&token)
                .json(&serde_json::json!({ "node_id": node }))
                .send()
                .await?;
            let status = resp.status();
            let body: serde_json::Value = resp.json().await.unwrap_or_default();
            println!("{body}");
            if !status.is_success() {
                std::process::exit(1);
            }
        }
        AdminCmd::Snapshot { out } => {
            let resp = client
                .get(format!("{base}/api/v1/admin/snapshot"))
                .bearer_auth(&token)
                .send()
                .await?;
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            if !status.is_success() {
                eprintln!("snapshot failed: {text}");
                std::process::exit(1);
            }
            match out {
                Some(path) => {
                    std::fs::write(path, &text)?;
                    eprintln!("快照已写入 {path}");
                }
                None => println!("{text}"),
            }
        }
        AdminCmd::RetentionStatus => {
            let resp = client
                .get(format!("{base}/api/v1/admin/retention-status"))
                .bearer_auth(&token)
                .send()
                .await?;
            let status = resp.status();
            let body: serde_json::Value = resp.json().await.unwrap_or_default();
            println!("{body}");
            if !status.is_success() {
                std::process::exit(1);
            }
        }
        AdminCmd::GenMasterKey
        | AdminCmd::RotateMasterKey { .. }
        | AdminCmd::ResetPassword { .. } => {
            unreachable!()
        }
    }
    Ok(())
}

/// 加入集群：向目标端点发起 join（需命中 leader；带重试与超时）。
/// 命中 follower 时跟随响应中的 leader_hint 切换目标（无需人工改 --join 指向）。
/// join_token 为 Some 时请求携带 `Authorization: Bearer <token>`（与节点 --join-token 匹配）。
async fn join_cluster(
    _raft: &dsh_raft::RaftHandle,
    node_id: u64,
    node: RaftNodeInfo,
    join_url: &mut String,
    join_token: Option<&str>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let mut req = client.post(format!("{join_url}/api/v1/cluster/join"));
        if let Some(token) = join_token {
            req = req.bearer_auth(token);
        }
        let resp = req
            .json(&serde_json::json!({
                "node_id": node_id,
                "http_addr": node.http_addr,
                "raft_addr": node.raft_addr,
            }))
            .send()
            .await;
        match resp {
            Ok(r) if r.status().is_success() => return Ok(()),
            // 409：本节点已在集群成员表（此前 join 已注册但响应丢失/节点崩溃于追赶中，
            // 或重启时数据目录被重置但成员表仍含本节点）。视为幂等成功——本节点 resume
            // 并启动 raft RPC 服务后，leader 会继续向它复制日志追赶，无需重试 join。
            Ok(r) if r.status() == reqwest::StatusCode::CONFLICT => {
                eprintln!(
                    "node {node_id} already in cluster membership (409); resuming to catch up"
                );
                return Ok(());
            }
            // 428 + leader_hint：--join 命中的节点不是 leader（如 leader 已切换）。
            // 跟随 leader_hint 切换目标后继续重试，避免 30s 空转。
            Ok(r) if r.status() == reqwest::StatusCode::PRECONDITION_REQUIRED => {
                let hint: Option<String> = r
                    .json::<serde_json::Value>()
                    .await
                    .ok()
                    .and_then(|b| b["detail"]["leader_hint"].as_str().map(String::from));
                if let Some(h) = hint {
                    if !h.is_empty() && h != *join_url {
                        // NodeInfo.http_addr 无 scheme（如 127.0.0.1:8612 / node2:8384）→ 补 http://
                        let target = if h.starts_with("http://") || h.starts_with("https://") {
                            h
                        } else {
                            format!("http://{h}")
                        };
                        eprintln!("node {node_id} join target not leader; following leader_hint -> {target}");
                        *join_url = target;
                    }
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err("join timed out (no leader responded)".into());
                }
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            }
            _ => {
                if tokio::time::Instant::now() >= deadline {
                    return Err("join timed out (no leader responded)".into());
                }
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            }
        }
    }
}

/// 解析 `--bootstrap-peers`：`node_id@raft_addr@http_addr` 逗号分隔 → 静态成员表。
/// **三段式必填**：http_addr 是 leader 重定向/join 跟随/登录转发的依据，缺失会静默降级
/// （写路径 hint 为空、登录不转发、join 428 无法跟随）。
/// 校验：node_id 非 0；地址为 host:port 且端口合法；raft/http 地址各自不得重复
/// （两个节点共用同一地址 = 复制目标冲突）；拒绝 0.0.0.0/:: 等不可路由通配地址（坑 C1）。
fn parse_bootstrap_peers(
    raw: &str,
) -> Result<std::collections::BTreeMap<u64, RaftNodeInfo>, String> {
    let mut map = std::collections::BTreeMap::new();
    let mut seen_raft = std::collections::HashSet::new();
    let mut seen_http = std::collections::HashSet::new();
    for (i, entry) in raw.split(',').enumerate() {
        let entry = entry.trim();
        if entry.is_empty() {
            continue; // 容忍空段（如尾部逗号）
        }
        let parts: Vec<&str> = entry.split('@').collect();
        let (id_str, raft_addr, http_addr) = match parts.as_slice() {
            [id, raft, http] => (*id, *raft, *http),
            [_, _] => {
                return Err(format!(
                    "--bootstrap-peers 第 {} 项缺少 http_addr（必须为 node_id@raft_addr@http_addr 三段式；http_addr 用于 leader 重定向/join 跟随）: {entry}",
                    i + 1
                ));
            }
            _ => {
                return Err(format!(
                    "--bootstrap-peers 第 {} 项格式错误（应为 node_id@raft_addr@http_addr）: {entry}",
                    i + 1
                ));
            }
        };
        let node_id: u64 = id_str
            .trim()
            .parse()
            .map_err(|_| format!("--bootstrap-peers 第 {} 项 node_id 非法: {id_str}", i + 1))?;
        if node_id == 0 {
            return Err(format!(
                "--bootstrap-peers 第 {} 项 node_id 不能为 0",
                i + 1
            ));
        }
        for (label, addr) in [("raft_addr", raft_addr), ("http_addr", http_addr)] {
            validate_seed_addr(label, addr, i + 1)?;
        }
        if !seen_raft.insert(raft_addr.to_string()) {
            return Err(format!(
                "--bootstrap-peers 第 {} 项 raft_addr 重复: {raft_addr}（两个节点不能共用同一 raft 地址）",
                i + 1
            ));
        }
        if !seen_http.insert(http_addr.to_string()) {
            return Err(format!(
                "--bootstrap-peers 第 {} 项 http_addr 重复: {http_addr}",
                i + 1
            ));
        }
        if map
            .insert(
                node_id,
                RaftNodeInfo {
                    grpc_addr: String::new(),
                    http_addr: http_addr.to_string(),
                    raft_addr: raft_addr.to_string(),
                },
            )
            .is_some()
        {
            return Err(format!(
                "--bootstrap-peers 第 {} 项 node_id 重复: {node_id}",
                i + 1
            ));
        }
    }
    if map.is_empty() {
        return Err("--bootstrap-peers 为空（须包含集群全部节点）".into());
    }
    Ok(map)
}

/// 校验 seed 地址：host:port 形式、端口为 1-65535 数值、host 不得为不可路由通配地址。
fn validate_seed_addr(label: &str, addr: &str, idx: usize) -> Result<(), String> {
    let err = |msg: String| format!("--bootstrap-peers 第 {idx} 项 {label} {msg}: {addr}");
    let parts: Vec<&str> = addr.split(':').collect();
    if parts.len() != 2 {
        return Err(err("须为 host:port 形式（暂不支持 IPv6 字面量）".into()));
    }
    let (host, port) = (parts[0], parts[1]);
    if host.is_empty() || host == "0.0.0.0" || host == "::" {
        return Err(err(
            "host 不能为不可路由通配地址（0.0.0.0/::；容器内请用服务名或具体 IP，见坑 C1）".into(),
        ));
    }
    let port_num: u16 = port
        .parse()
        .map_err(|_| err("端口须为 1-65535 的数值".into()))?;
    if port_num == 0 {
        return Err(err("端口不能为 0".into()));
    }
    Ok(())
}

/// 校验 seed map 与本节点启动参数一致（配置漂移 = split-brain 的根源，启动即失败）。
fn validate_bootstrap_peers(
    map: &std::collections::BTreeMap<u64, RaftNodeInfo>,
    node_id: u64,
    raft_addr: &str,
    http_addr: &str,
) -> Result<(), String> {
    let local = map.get(&node_id).ok_or_else(|| {
        format!("--bootstrap-peers 不含本节点 node_id={node_id}（seed map 必须包含集群全部节点）")
    })?;
    if local.raft_addr != raft_addr {
        return Err(format!(
            "--bootstrap-peers 中本节点 raft_addr({}) 与 --raft-addr({}) 不一致",
            local.raft_addr, raft_addr
        ));
    }
    if local.http_addr != http_addr {
        return Err(format!(
            "--bootstrap-peers 中本节点 http_addr({}) 与 --http-addr({}) 不一致",
            local.http_addr, http_addr
        ));
    }
    Ok(())
}

/// 比对 seed map 与集群当前（恢复后的）成员表，返回差异描述；一致返回 None。
/// 语义（A2 修正）：seed 只用于首次建群；有持久化状态时以共识成员表为准，
/// 不一致仅 WARN（不覆盖不阻断）——要么 seed 已过期（请更新配置），
/// 要么有重整意图（集群在线走 join/promote/remove-node；推倒重建先清空数据目录再以 seed 建群）。
fn membership_diff(
    seed: &std::collections::BTreeMap<u64, RaftNodeInfo>,
    current: &std::collections::BTreeMap<u64, RaftNodeInfo>,
) -> Option<String> {
    let mut diffs: Vec<String> = Vec::new();
    for (id, node) in seed {
        match current.get(id) {
            None => diffs.push(format!(
                "seed 含 node {id} 但集群成员表没有（若为新增节点请用 --join 加入，seed 不驱动运行期成员变更）"
            )),
            Some(cur) => {
                if cur.raft_addr != node.raft_addr {
                    diffs.push(format!(
                        "node {id} raft_addr 不一致：seed={} 集群={}",
                        node.raft_addr, cur.raft_addr
                    ));
                }
                if cur.http_addr != node.http_addr {
                    diffs.push(format!(
                        "node {id} http_addr 不一致：seed={} 集群={}",
                        node.http_addr, cur.http_addr
                    ));
                }
            }
        }
    }
    for (id, node) in current {
        if !seed.contains_key(id) {
            diffs.push(format!(
                "集群成员表含 node {id}({}) 但 seed 没有（seed 已过期，请更新配置）",
                node.raft_addr
            ));
        }
    }
    if diffs.is_empty() {
        None
    } else {
        Some(diffs.join("；"))
    }
}

/// 集群主密钥轮换钩子类型（F7b：入参为 RotateMasterKey 命令，解密由实现方负责）。
type RotationHook = Arc<dyn Fn(&dsh_core::command::Command) + Send + Sync>;

/// 集群轮换钩子：Raft apply 到 RotateMasterKey 时更新本节点 keyring 并持久化 ring 文件
/// （幂等：已含该 KEK 则跳过，重放安全；持久化失败不切换内存，保持可解）。
/// F7b：新命令载荷为 kek_enc（当前 KEK 自加密）——逐个尝试 keyring 内 KEK 解开；
/// 旧日志为 kek 明文，直接使用。
fn cluster_rotation_hook(
    key_file: Option<&str>,
    cipher: Option<Arc<Cipher>>,
) -> Option<RotationHook> {
    let cipher = cipher?;
    let ring_path = key_file.map(dsh_crypto::ring_file_path);
    Some(Arc::new(move |cmd: &dsh_core::command::Command| {
        let dsh_core::command::Command::RotateMasterKey { kek, kek_enc } = cmd else {
            return;
        };
        // 解析明文 KEK（32B）
        let plain: Vec<u8> = if !kek_enc.is_empty() {
            let ring = cipher.keyring();
            // kek_enc 用「提交时刻的当前 KEK」加密；从最新到最旧逐个尝试（节点可能落后/追赶）
            let mut resolved: Option<[u8; 32]> = None;
            for k in ring.entries().iter().rev() {
                if let Ok(kk) = dsh_crypto::Cipher::unwrap_master_key(k, kek_enc) {
                    resolved = Some(kk);
                    break;
                }
            }
            match resolved {
                Some(k) => k.to_vec(),
                None => {
                    tracing::error!("rotate: cannot unwrap kek_enc with any known KEK");
                    return;
                }
            }
        } else {
            kek.clone()
        };
        if plain.len() != 32 {
            return;
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&plain);
        let ring = cipher.keyring();
        if ring.entries().iter().any(|k| k == &arr) {
            return; // 幂等（重放/多节点）
        }
        let mut new_ring = ring;
        new_ring.push(arr);
        if let Some(p) = &ring_path {
            if let Err(e) = dsh_crypto::save_ring(p, &new_ring) {
                tracing::error!("rotate: persist ring file: {e}");
                return; // 持久化失败不切换内存（保持可解）
            }
        }
        cipher.rotate_master_key(arr);
    }))
}

fn resolve_admin_password(cli: &Cli, node_label: &str) -> Arc<str> {
    match &cli.admin_password {
        Some(p) => Arc::from(p.as_str()),
        None => {
            let gen = format!("dsh-admin-{}", new_token());
            eprintln!("{node_label} 管理员密码 = {gen}（请使用 --admin-password 显式设置）");
            Arc::from(gen.as_str())
        }
    }
}

// ---------------- G1/D35-37：CLI 参数枚举（clap value_enum → core 类型） ----------------

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum PolicyArg {
    Block,
    Warn,
}
impl From<PolicyArg> for dsh_core::model::PublishPolicy {
    fn from(v: PolicyArg) -> Self {
        match v {
            PolicyArg::Block => dsh_core::model::PublishPolicy::Block,
            PolicyArg::Warn => dsh_core::model::PublishPolicy::Warn,
        }
    }
}
#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum CascadeArg {
    Auto,
    Manual,
}
impl From<CascadeArg> for dsh_core::model::SharedCascadeMode {
    fn from(v: CascadeArg) -> Self {
        match v {
            CascadeArg::Auto => dsh_core::model::SharedCascadeMode::Auto,
            CascadeArg::Manual => dsh_core::model::SharedCascadeMode::Manual,
        }
    }
}
#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum ReadArg {
    Linear,
    Stale,
}
impl From<ReadArg> for dsh_core::model::ReadMode {
    fn from(v: ReadArg) -> Self {
        match v {
            ReadArg::Linear => dsh_core::model::ReadMode::Linear,
            ReadArg::Stale => dsh_core::model::ReadMode::Stale,
        }
    }
}

// ---------------- 主入口 ----------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    let cli = Cli::parse();
    // `defing admin <cmd>` 客户端模式（不启动服务；需 --admin-endpoint）
    if let Some(Command::Admin { cmd }) = &cli.cmd {
        return run_admin_cmd(&cli, cmd).await;
    }
    let hub = WatchHub::with_capacity(cli.watch_event_retain as usize);
    // 可信代理（F4）：解析失败直接报错退出
    let trusted_proxies = std::sync::Arc::new(match &cli.trusted_proxy {
        Some(s) => {
            dsh_api::TrustedProxies::parse(s).map_err(|e| format!("--trusted-proxy: {e}"))?
        }
        None => dsh_api::TrustedProxies::empty(),
    });
    // 主密钥（secret 项加密/解密；I8）
    let master_key = load_master_key(
        std::env::var("DSH_MASTER_KEY").ok().as_deref(),
        cli.master_key_file.as_deref(),
    )
    .map_err(|e| format!("master key: {e}"))?;
    // 主密钥环：文件密钥 + 环文件历史 KEK（轮换后重启可解旧数据，CRY-002）
    let cipher = master_key.map(|k| {
        let ring_path = cli
            .master_key_file
            .as_deref()
            .map(dsh_crypto::ring_file_path);
        // N3：ring 文件损坏/解析失败不再静默当空处理 —— 旧密文将不可解，必须告警。
        let ring_entries = match ring_path.as_ref() {
            Some(p) => match dsh_crypto::load_ring(p) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("load ring {}: {e}", p.display());
                    Vec::new()
                }
            },
            None => Vec::new(),
        };
        // 去重：ring 文件首项即为文件主密钥本身（首次轮换时写入），
        // 直接 extend 会让每次重启代际 +1 且 keyring 含重复 KEK。
        let mut entries = vec![k];
        entries.extend(ring_entries.into_iter().filter(|e| *e != k));
        Arc::new(Cipher::with_keyring(dsh_crypto::KeyRing::from_entries(
            entries,
        )))
    });

    // ---- 客户端模式（不启动服务）----
    if cli.gen_master_key {
        println!("{}", dsh_crypto::Cipher::generate_master_key());
        return Ok(());
    }
    if let Some(new_key) = &cli.rotate_master_key {
        let client = reqwest::Client::new();
        // 单会话（I7）下已有会话时 login 会 409 → 优先用 --admin-token；缺省才登录
        let token = match &cli.admin_token {
            Some(t) => t.clone(),
            None => {
                let pw = cli
                    .admin_password
                    .clone()
                    .ok_or("--rotate-master-key 需要 --admin-password 或 --admin-token")?;
                let login: serde_json::Value = client
                    .post(format!("{}/api/v1/login", cli.admin_endpoint))
                    .json(&serde_json::json!({ "password": pw }))
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                login["token"]
                    .as_str()
                    .ok_or("login failed (bad password?)")?
                    .to_string()
            }
        };
        let resp = client
            .post(format!(
                "{}/api/v1/admin/rotate-master-key",
                cli.admin_endpoint
            ))
            .bearer_auth(&token)
            .json(&serde_json::json!({ "new_key": new_key }))
            .send()
            .await?;
        let status = resp.status();
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        println!("{body}");
        if !status.is_success() {
            std::process::exit(1);
        }
        return Ok(());
    }

    // design-v2 §7.4：无主密钥默认拒绝启动（含 secret 场景必配）；开发/演示环境显式 --allow-no-master-key 逃生。
    if master_key.is_none() && !cli.allow_no_master_key {
        return Err(
            "缺少主密钥（DSH_MASTER_KEY 或 --master-key-file）；若为无 secret 的开发/演示环境请显式 --allow-no-master-key"
                .into(),
        );
    }

    if cli.dev_single {
        let store: Box<dyn dsh_core::Store> = match &cli.data_dir {
            Some(dir) => Box::new(RedbStorage::open(dir)?),
            None => Box::new(InMemoryStore::new()),
        };
        let sm = StateMachine::new(store);
        let admin_password = resolve_admin_password(&cli, "首次启动");
        // project-token：dev-single 自动生成全局开发数据面 token 并打印（可访问所有项目；
        // 集群模式无此机制——数据面鉴权一律走每项目访问令牌）
        let dev_token = new_token();
        eprintln!("开发数据面 token = {dev_token}（--dev-single 全局有效，可访问所有项目；集群模式无此机制）");
        // hub 将被移入 ApiState，先取 sender 供自动回滚广播（G5/D33）
        let hub_sender = hub.sender().clone();
        let mut app = ApiState::with_retention(
            Arc::new(RwLock::new(sm)),
            hub,
            None,
            None,
            cipher,
            std::time::Duration::from_secs(cli.session_ttl),
            admin_password,
            cli.master_key_file
                .as_deref()
                .map(dsh_crypto::ring_file_path),
            cli.version_retention,
            cli.audit_retention,
            cli.join_token.clone().map(Arc::from),
            trusted_proxies.clone(),
            Some(Arc::from(dev_token.as_str())),
        );
        // G1/D35-37：发布策略/级联/读取模式注入
        app.publish.publish_policy = cli.publish_policy.into();
        app.publish.shared_cascade = cli.shared_cascade.into();
        app.read_mode = cli.read_mode.into();
        // G5/D33：灰度自动回滚（dev-single 恒 leader；threshold 百分比 → 比例）
        if cli.gray_rollback_threshold > 0.0 {
            let (_leader_tx, leader_rx) = tokio::sync::watch::channel(true);
            dsh_jobs::spawn_gray_auto_rollback(
                app.sm.clone(),
                None,
                Some(hub_sender),
                app.audit.clone(),
                Box::new(dsh_jobs::LocalHttp5xxProbe),
                cli.gray_rollback_threshold / 100.0,
                std::time::Duration::from_secs(cli.gray_rollback_interval),
                leader_rx,
            );
        }
        spawn_grpc(&cli, app.clone());
        let router = dsh_api::build_router(app);
        let listener = tokio::net::TcpListener::bind(&cli.http_addr).await?;
        eprintln!("defing --dev-single listening on http://{}", cli.http_addr);
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await?;
        return Ok(());
    }

    // ---------- 集群模式 ----------
    let data_dir = cli.data_dir.clone().ok_or("集群模式需要 --data-dir")?;
    let node_id = cli.node_id.ok_or("集群模式需要 --node-id")?;
    // F3：集群模式强制引导令牌（S2 修复默认生效）——否则任意网络可达者可注册 learner
    // 拉走全量 Raft 日志（含密码哈希/会话哈希/密文）。dev-single 无 raft，不要求。
    let join_token = cli
        .join_token
        .clone()
        .ok_or("集群模式需要 --join-token（join 端点鉴权；集群内所有节点传相同值）")?;
    // S5：raft 内部 RPC 端口同样要求共享令牌（纵深防御；防伪造 vote/append 制造选举抖动）
    let raft_token = cli
        .raft_token
        .clone()
        .ok_or("集群模式需要 --raft-token（raft RPC 鉴权；集群内所有节点传相同值）")?;

    let storage = RedbStorage::open(&data_dir)?;
    let db = storage.raw_db();
    // P2a file-backed SnapshotData:本节点 data 目录（RedbStorage::base_dir anchor），
    // snapshot payload 文件落 `{data_dir}/snapshots/`（StateMachineStore 与 raft router 共用）
    let data_base = storage.clone().base_dir();
    let sm = Arc::new(RwLock::new(StateMachine::new(Box::new(storage))));
    // 集群模式挂主密钥轮换钩子：Raft apply 到 RotateMasterKey 时更新本节点 keyring 并持久化 ring 文件
    // （dev-single 不挂——它走 handler 的本地轮换逻辑，先持久化后切换）。
    let sm_store = Arc::new(StateMachineStore::new_with_rotation(
        sm.clone(),
        db.clone(),
        cluster_rotation_hook(cli.master_key_file.as_deref(), cipher.clone()),
        data_base.clone(),
    ));
    // P4-A：boot 期重放被中断的快照安装（`META_RESTORE_BEGIN`）。必须在 `has_persisted_state`
    // 与 `new_raft_node` 之前——重放会补写 last_applied / membership / snapshot registry。
    // 失败（marker 损坏、payload 缺失/损坏、重放写失败）→ **拒绝启动**（fail-fast）：
    // 以「部分新 + 部分旧」的混合状态继续跑会静默分叉。
    // 注意：`new_with_rotation` 的启动清扫已把 marker.payload 并入 keep-set（设计 §7 A1），
    // 因此顺序是「清扫 → 重放 → 再清扫」，不会先删掉待重放的 payload。
    match dsh_raft::recover_interrupted_restore(&sm, &db, data_base.clone()) {
        Ok(Some(meta)) => eprintln!(
            "node {node_id}: replayed interrupted snapshot restore (last_log_id={:?})",
            meta.last_log_id
        ),
        Ok(None) => {}
        Err(e) => return Err(format!("快照恢复中断且无法重放，拒绝启动：{e}").into()),
    }
    // A2：seed 与集群现实比对用——保留一份 store 引用（sm_store 随后被 new_raft_node 移走）。
    // 直接读持久化成员表而非 raft.metrics()：metrics 在 Raft::new 后可能尚未发布（watch 异步），
    // 读存储层是确定性的。
    let sm_store_check = sm_store.clone();
    // 幂等初始化（重启/崩溃恢复安全）：raft-meta 非空说明该节点已有持久化状态，
    // 此时 --bootstrap/--bootstrap-peers/--join 一律忽略、直接 resume（auto-rejoin）。
    // 由此 compose/k8s 可用静态启动命令（每次启动同一参数），无需 shell 判断数据目录。
    // 崩溃窗口覆盖：
    //   - 建群/join 前崩溃（无任何 raft 状态）→ 重跑建群/join；
    //   - join 已注册但响应丢失/追赶中崩溃（leader 已记为 learner）→ join 幂等成功（leader 侧）+ resume；
    //   - 已有完整状态 → 忽略初始化参数，resume。
    let has_state = sm_store.has_persisted_state();
    if !cli.bootstrap && cli.bootstrap_peers.is_none() && cli.join.is_none() && !has_state {
        return Err("集群模式需要 --bootstrap、--bootstrap-peers、--join 或已有数据目录".into());
    }
    // 集群 watch：raft apply 事件 → hub（SSE）
    hub.spawn_raft_forward(sm_store.clone());
    let log_store = LogStore::new(db.clone());

    let node_info = RaftNodeInfo {
        grpc_addr: cli.grpc_addr.clone(),
        http_addr: cli.http_addr.clone(),
        raft_addr: cli.raft_addr.clone(),
    };
    let network = HttpNetworkFactory::with_token(Some(raft_token.clone()));
    // 启动断点（不改语义）：把 dev_config() 结果局部 bind + 打印实际生效 Raft 配置（与 main
    // 同格式，0.10 字段 superset）。stress harness (scripts/stress/leader_storm.py) 对每个
    // 节点解析这条行后启动并断言值 == dsh-raft raft.rs dev_config() 预期——陈旧 binary /
    // 配置漂移直接 detect，任何「heartbeat 100ms / install_snapshot_timeout 30s」结论
    // 对应实际运行配置。cluster_name 打印 openraft Config 默认（dev_config 未覆盖 → "foo"）。
    let raft_cfg = dsh_raft::dev_config();
    tracing::info!(
        "raft config: heartbeat_interval={} election_timeout_min={} election_timeout_max={} max_payload_entries={} enable_pre_vote={} enable_tick={} install_snapshot_timeout={} snapshot_max_chunk_size={} max_in_snapshot_log_to_keep={} cluster_name={}",
        raft_cfg.heartbeat_interval,
        raft_cfg.election_timeout_min,
        raft_cfg.election_timeout_max,
        raft_cfg.max_payload_entries,
        raft_cfg.enable_pre_vote.unwrap_or(false),
        raft_cfg.enable_tick,
        raft_cfg.install_snapshot_timeout,
        raft_cfg.snapshot_max_chunk_size,
        raft_cfg.max_in_snapshot_log_to_keep,
        raft_cfg.cluster_name,
    );
    let raft = dsh_raft::new_raft_node(
        node_id,
        node_info.clone(),
        log_store,
        sm_store,
        &network,
        raft_cfg,
    )
    .await?;

    // 首次初始化仅在无持久化状态时执行；已有状态一律 resume（忽略建群参数）。
    if cli.bootstrap && !has_state {
        dsh_raft::initialize_single(&raft, node_id, node_info.clone()).await?;
        eprintln!("node {node_id} bootstrap done (first init)");
    } else if let Some(seed_raw) = &cli.bootstrap_peers {
        if has_state {
            // A2：seed 只用于首次建群；有持久化状态时以共识成员表为准。seed 与成员表不一致
            // 仅 WARN（不覆盖不阻断）：要么 seed 过期（更新配置），要么有重整意图（在线走 API，
            // 推倒重建先清卷再以 seed 建群）。
            match parse_bootstrap_peers(seed_raw) {
                Ok(seed) => {
                    // 持久化成员表为空 = 崩溃于追平前（vote 已落盘但成员表未到），resume 后会自动
                    // 追平，此时跳过比对（避免瞬态误报）。
                    if let Ok(Some(current)) = sm_store_check.persisted_membership() {
                        if let Some(diff) = membership_diff(&seed, &current) {
                            eprintln!("WARNING: --bootstrap-peers 与集群当前成员表不一致：{diff}");
                            eprintln!("         seed 仅用于首次建群；运行期成员变更请走 join/promote/remove-node；");
                            eprintln!("         如需重整拓扑：集群在线走 API；推倒重建请先清空数据目录再以 seed 建群。");
                        }
                    }
                }
                Err(e) => {
                    eprintln!("WARNING: --bootstrap-peers 解析失败（本节点 resume 不受影响）：{e}")
                }
            }
            eprintln!("node {node_id} has persisted state; ignoring --bootstrap-peers and resuming (auto-rejoin)");
        } else {
            // 静态成员表建群：解析 + 校验本地一致性后，所有节点并行 initialize 全量 map
            // （openraft：同 map 并发安全；先到者首写，其余节点收到良性 NotAllowed 后经复制追平，
            // 全员 voter，无需 join/promote）。
            let seed = parse_bootstrap_peers(seed_raw)?;
            validate_bootstrap_peers(&seed, node_id, &cli.raft_addr, &cli.http_addr)?;
            let n = seed.len();
            match dsh_raft::initialize_cluster(&raft, seed).await? {
                true => eprintln!("node {node_id} cluster initialized from seed map ({n} peers, all voters)"),
                false => eprintln!("node {node_id} cluster bootstrap delegated to a peer (catching up via replication)"),
            }
        }
    } else if let Some(join_url) = &cli.join {
        if has_state {
            eprintln!(
                "node {node_id} has persisted state; ignoring --join and resuming (auto-rejoin)"
            );
        } else {
            let mut target = join_url.clone();
            join_cluster(
                &raft,
                node_id,
                node_info.clone(),
                &mut target,
                Some(join_token.as_str()),
            )
            .await?;
            eprintln!("node {node_id} join requested -> {target}");
        }
    } else if has_state {
        eprintln!("node {node_id} resuming from persisted state (auto-rejoin)");
    }

    // 后台任务（仅 leader 执行）：版本裁剪 + 审计保留
    {
        // 由 raft.metrics() watch 推导 is_leader
        let (leader_tx, leader_rx) = tokio::sync::watch::channel(false);
        let metrics = raft.metrics();
        tokio::spawn(async move {
            let mut rx = metrics;
            while rx.changed().await.is_ok() {
                let m = rx.borrow_watched().clone();
                let _ = leader_tx.send(m.current_leader == Some(node_id));
            }
        });
        let mut scheduler = dsh_jobs::JobScheduler::new();
        if cli.version_retention > 0 {
            scheduler.add(dsh_jobs::VersionRetention {
                keep: cli.version_retention as usize,
            });
        }
        if cli.audit_retention > 0 {
            scheduler.add(dsh_jobs::AuditRetention {
                keep: cli.audit_retention as usize,
            });
        }
        scheduler.spawn(sm.clone(), leader_rx.clone());
        // G5/D33：灰度自动回滚（可选，仅 leader；threshold 为百分比 → 转比例）
        if cli.gray_rollback_threshold > 0.0 {
            let audit = dsh_observability::AuditLog::new(sm.clone(), Some(raft.clone()));
            dsh_jobs::spawn_gray_auto_rollback(
                sm.clone(),
                Some(raft.clone()),
                Some(hub.sender().clone()),
                audit,
                Box::new(dsh_jobs::LocalHttp5xxProbe),
                cli.gray_rollback_threshold / 100.0,
                std::time::Duration::from_secs(cli.gray_rollback_interval),
                leader_rx,
            );
        }
    }

    // B1：长时间无 leader 时周期提示（集群静默空转的可观测性兜底）。
    // 适用：seed 建群但 quorum 未达成（如只有部分节点启动）、重启后多数派不可达等；
    // 有成员表但 leader 未知持续 15s 以上 → 每 10s 提示一次（每段失联只提示一次，恢复后重置）。
    {
        let raft_warn = raft.clone();
        let warn_node_id = node_id;
        tokio::spawn(async move {
            let mut warned = false;
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;
            loop {
                let m = raft_warn.metrics().borrow_watched().clone();
                if m.current_leader.is_some() {
                    warned = false;
                } else {
                    let voters = m.membership_config.membership().voter_ids().count();
                    // voters == 0 = 尚无成员表（如 join 等待中）——join 流程有自己的超时退出，不在此提示
                    if voters > 0 && !warned {
                        eprintln!("WARNING: node {warn_node_id} 长时间未确认 leader（集群 quorum 未达成，voter 数 = {voters}）。请检查其他节点是否在线；leader 恢复后此提示自动停止。");
                        warned = true;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            }
        });
    }

    // Raft RPC 服务（raft_addr）
    let raft_state = RaftServerState::with_token(
        raft.clone(),
        Some(Arc::from(raft_token.as_str())),
        data_base.clone().join(dsh_raft::store::SNAP_SUBDIR),
    );
    let raft_router = dsh_raft::raft_router(raft_state);
    let raft_addr = cli.raft_addr.clone();
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&raft_addr)
            .await
            .expect("bind raft addr");
        axum::serve(listener, raft_router)
            .await
            .expect("raft server");
    });

    let admin_password = resolve_admin_password(&cli, &format!("节点 {node_id}"));
    let mut app = ApiState::with_retention(
        sm.clone(),
        hub,
        Some(raft.clone()),
        Some(node_id),
        cipher,
        std::time::Duration::from_secs(cli.session_ttl),
        admin_password,
        cli.master_key_file
            .as_deref()
            .map(dsh_crypto::ring_file_path),
        cli.version_retention,
        cli.audit_retention,
        Some(Arc::from(join_token.as_str())),
        trusted_proxies,
        None, // 集群模式无 dev token：数据面鉴权一律走每项目访问令牌
    );
    // G1/D35-37：发布策略/级联/读取模式注入
    app.publish.publish_policy = cli.publish_policy.into();
    app.publish.shared_cascade = cli.shared_cascade.into();
    app.read_mode = cli.read_mode.into();
    // P4-A A3：restore 失败的 fail-stop 句柄（store 置位 → HTTP 读写 503 / gRPC UNAVAILABLE）
    app.set_poison(sm_store_check.poison_flag());
    spawn_grpc(&cli, app.clone());
    let router = dsh_api::build_router(app);
    let listener = tokio::net::TcpListener::bind(&cli.http_addr).await?;
    eprintln!(
        "defing node {node_id} listening on http://{} (raft {})",
        cli.http_addr, cli.raft_addr
    );
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}

/// 数据面 gRPC 服务（模块 05）：ConfigService 挂载于 --grpc-addr（默认 :8383）。
fn spawn_grpc(cli: &Cli, state: ApiState) {
    let svc = dsh_api::grpc::config_service_server::ConfigServiceServer::new(
        dsh_api::grpc::ConfigGrpcService { state },
    );
    let addr: std::net::SocketAddr = match cli.grpc_addr.parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("invalid --grpc-addr {}: {e}", cli.grpc_addr);
            return;
        }
    };
    let grpc_addr = cli.grpc_addr.clone();
    tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(svc)
            .serve(addr)
            .await
        {
            eprintln!("grpc server on {grpc_addr} failed: {e}");
        } else {
            eprintln!("defing gRPC data plane listening on {grpc_addr}");
        }
    });
}

// ---------------- 单元测试 ----------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_map() -> std::collections::BTreeMap<u64, RaftNodeInfo> {
        parse_bootstrap_peers(
            "1@node1:8385@node1:8384,2@node2:8385@node2:8384,3@node3:8385@node3:8384",
        )
        .unwrap()
    }

    #[test]
    fn parse_full_map_three_part_required() {
        let m = sample_map();
        assert_eq!(m.len(), 3);
        assert_eq!(m[&1].raft_addr, "node1:8385");
        assert_eq!(m[&1].http_addr, "node1:8384");
        assert_eq!(m[&3].http_addr, "node3:8384");
        assert_eq!(m[&2].grpc_addr, ""); // 与 join 模型一致，grpc 不落成员表
    }

    #[test]
    fn parse_rejects_bad_entries() {
        // 两段式（缺 http_addr）→ 拒绝（A1：http 是重定向/join 跟随的依据）
        assert!(parse_bootstrap_peers("1@a:1").is_err());
        assert!(parse_bootstrap_peers("1@node1:8385").is_err());
        // 段数不对
        assert!(parse_bootstrap_peers("1@a:1@b:2@c").is_err());
        assert!(parse_bootstrap_peers("1").is_err());
        // node_id 非法 / 为 0
        assert!(parse_bootstrap_peers("x@a:1@b:2").is_err());
        assert!(parse_bootstrap_peers("0@a:1@b:2").is_err());
        // 地址非 host:port / 端口非法
        assert!(parse_bootstrap_peers("1@nodomain@b:2").is_err());
        assert!(parse_bootstrap_peers("1@a:1@b").is_err()); // http 段非 host:port
        assert!(parse_bootstrap_peers("1@a:0@b:2").is_err()); // 端口 0
        assert!(parse_bootstrap_peers("1@a:99999@b:2").is_err()); // 端口越界
        assert!(parse_bootstrap_peers("1@a:1@b:abc").is_err()); // 端口非数值
                                                                // 不可路由通配地址（坑 C1）
        assert!(parse_bootstrap_peers("1@0.0.0.0:1@b:2").is_err()); // raft 0.0.0.0
        assert!(parse_bootstrap_peers("1@a:1@0.0.0.0:2").is_err()); // http 0.0.0.0
        assert!(parse_bootstrap_peers("1@:1@b:2").is_err()); // 空 host
                                                             // 重复 node_id
        assert!(parse_bootstrap_peers("1@a:1@a:2,1@b:3@b:4").is_err());
        // 重复 raft_addr（两节点共用同一 raft 地址）
        assert!(parse_bootstrap_peers("1@a:1@a:2,2@a:1@b:4").is_err());
        // 重复 http_addr
        assert!(parse_bootstrap_peers("1@a:1@b:2,2@c:3@b:2").is_err());
        // 空
        assert!(parse_bootstrap_peers("").is_err());
        assert!(parse_bootstrap_peers(",").is_err());
    }

    #[test]
    fn parse_tolerates_whitespace_and_trailing_comma() {
        let m = parse_bootstrap_peers(" 1@a:1@x:1 , 2@b:2@y:2 ,").unwrap();
        assert_eq!(m.len(), 2);
        assert_eq!(m[&2].raft_addr, "b:2");
        assert_eq!(m[&2].http_addr, "y:2");
    }

    #[test]
    fn validate_local_consistency() {
        let m = sample_map();
        // 本节点在 map 中且地址一致 → ok
        assert!(validate_bootstrap_peers(&m, 1, "node1:8385", "node1:8384").is_ok());
        assert!(validate_bootstrap_peers(&m, 3, "node3:8385", "node3:8384").is_ok());
        // 本节点不在 map 中
        assert!(validate_bootstrap_peers(&m, 9, "node9:8385", "node9:8384").is_err());
        // raft 地址不一致
        assert!(validate_bootstrap_peers(&m, 1, "wrong:8385", "node1:8384").is_err());
        // http 地址不一致（三段式下必填比对）
        assert!(validate_bootstrap_peers(&m, 1, "node1:8385", "wrong:8384").is_err());
        assert!(validate_bootstrap_peers(&m, 2, "node2:8385", "whatever:8384").is_err());
    }

    fn node(raft: &str, http: &str) -> RaftNodeInfo {
        RaftNodeInfo {
            grpc_addr: String::new(),
            http_addr: http.into(),
            raft_addr: raft.into(),
        }
    }

    #[test]
    fn membership_diff_reports_inconsistencies() {
        let seed: std::collections::BTreeMap<u64, RaftNodeInfo> =
            std::collections::BTreeMap::from([
                (1, node("n1:8385", "n1:8384")),
                (2, node("n2:8385", "n2:8384")),
            ]);
        // 完全一致 → None
        let same: std::collections::BTreeMap<u64, RaftNodeInfo> =
            std::collections::BTreeMap::from([
                (1, node("n1:8385", "n1:8384")),
                (2, node("n2:8385", "n2:8384")),
            ]);
        assert_eq!(membership_diff(&seed, &same), None);
        // seed 多出节点（想用 config 加节点 → 应走 join）
        let cur1: std::collections::BTreeMap<u64, RaftNodeInfo> =
            std::collections::BTreeMap::from([(1, node("n1:8385", "n1:8384"))]);
        let d = membership_diff(&seed, &cur1).expect("diff");
        assert!(d.contains("seed 含 node 2"), "{d}");
        // 集群多出节点（seed 过期）
        let cur2: std::collections::BTreeMap<u64, RaftNodeInfo> =
            std::collections::BTreeMap::from([
                (1, node("n1:8385", "n1:8384")),
                (2, node("n2:8385", "n2:8384")),
                (3, node("n3:8385", "n3:8384")),
            ]);
        let d = membership_diff(&seed, &cur2).expect("diff");
        assert!(d.contains("集群成员表含 node 3"), "{d}");
        // 地址不一致
        let cur3: std::collections::BTreeMap<u64, RaftNodeInfo> =
            std::collections::BTreeMap::from([
                (1, node("n1:8385", "n1:8384")),
                (2, node("n2-new:8385", "n2:8384")),
            ]);
        let d = membership_diff(&seed, &cur3).expect("diff");
        assert!(d.contains("raft_addr 不一致"), "{d}");
        // 多类差异合并为一条
        let cur4: std::collections::BTreeMap<u64, RaftNodeInfo> =
            std::collections::BTreeMap::from([
                (1, node("n1:8385", "n1-new:8384")),
                (3, node("n3:8385", "n3:8384")),
            ]);
        let d = membership_diff(&seed, &cur4).expect("diff");
        assert!(
            d.contains("http_addr 不一致")
                && d.contains("seed 含 node 2")
                && d.contains("集群成员表含 node 3"),
            "{d}"
        );
    }
}
