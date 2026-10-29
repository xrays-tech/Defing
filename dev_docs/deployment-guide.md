# Defing 配置与部署指南

> 落档日期：2026-08-20
> 适用范围：当前 `main` 分支（二进制已更名 `defing`，集群建群推荐静态成员表 `--bootstrap-peers`）。
> 本文档覆盖 **单机 / 集群 / Docker / docker-compose** 全部部署形态的配置方法。
> 配套材料：[dev_docs/defing-cluster.md](defing-cluster.md)（容器化集群踩坑记录）、[dev_docs/research-cluster-bootstrap.md](research-cluster-bootstrap.md)（静态成员表建群设计）。

---

## 0. 配置方式总览（重要）

**Defing 没有配置文件**。服务端（`defing` 二进制）的全部配置通过 **CLI 参数**传入，
唯一例外是主密钥：可用环境变量 `DSH_MASTER_KEY`（base64 32B）注入，或 `--master-key-file`
（raw 32B 文件）传入，两者等价（env 优先）。

| 配置来源 | 作用 | 优先级 |
|---|---|---|
| CLI 参数 | 运行模式 / 端口 / 存储 / 认证 / 保留策略 / 灰度策略 | 唯一入口（除主密钥） |
| `DSH_MASTER_KEY` 环境变量 | 主密钥（base64 32B），secret 项 AES-256-GCM 加密 | env > `--master-key-file` |
| `--master-key-file` | 主密钥文件（raw 32B） | 次选 |
| compose 环境变量（`DSH_BOOTSTRAP_PEERS` 等） | **仅 docker-compose 自身插值**，最终仍展开为 CLI 参数 | — |
| SDK 环境变量（`DSH_ENDPOINT` 等） | 客户端/示例程序读取的配置坐标 | 仅客户端侧 |

> 相比旧版本的配置方法变化，见 [§11 变更记录](#11-配置方法变更记录)。

---

## 1. 端口与默认地址

| 端口 | 用途 | 参数 | 默认值 |
|---|---|---|---|
| **8384** | HTTP 管理面：Admin UI（/admin）、REST API、/metrics、/healthz、/readyz | `--http-addr` | `127.0.0.1:8384` |
| **8385** | Raft 内部 RPC（节点间复制/选举，集群模式） | `--raft-addr` | `127.0.0.1:8385` |
| **8383** | gRPC 数据面（SDK 首选通道） | `--grpc-addr` | `127.0.0.1:8383` |

> 单机联调（`--dev-single`）不启用 Raft，`--raft-addr` 无实际监听，但 `--grpc-addr` 仍生效。
> 生产部署中三个端口都需放通/暴露；对外只需暴露 8384 与 8383，8385 仅集群内网可达。

---

## 2. 运行模式

`defing` 有三种运行形态，通过 CLI 参数区分：

| 模式 | 判定 | 说明 |
|---|---|---|
| 单机联调 | `--dev-single` | 无 Raft，状态机直接 apply；数据默认**内存**，加 `--data-dir` 可持久化 |
| 集群-静态建群（推荐） | `--node-id N --bootstrap-peers "<seed>"` | 全员 voter，并行启动直接选举，无需 join/promote |
| 集群-动态扩容 | `--node-id N --bootstrap`（首节点）或 `--node-id N --join http://<host>:8384`（后续节点） | learner 加入 → promote 为 voter |

**集群模式硬性要求**（缺一启动即报错）：
- `--node-id` 与 `--data-dir`（数据目录，redb 持久化）；
- `--join-token` 与 `--raft-token`（join 端点与 raft RPC 鉴权，**集群内所有节点必须传相同值**，生产用强随机串）；
- `--bootstrap` / `--bootstrap-peers` / `--join` 三者之一（或数据目录已有持久化状态，直接 resume）。

---

## 3. CLI 参数全表

### 3.1 运行模式与集群

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `--dev-single` | bool | — | 单节点联调（无 Raft） |
| `--node-id <id>` | u64 | — | 集群模式：本节点 ID（非 0） |
| `--bootstrap` | bool | — | 集群模式：首节点自举（单节点建群，其余节点 `--join`）。与 `--bootstrap-peers`、`--join` 互斥 |
| `--bootstrap-peers <seed>` | string | — | **推荐**：静态成员表建群。格式 `node_id@raft_addr@http_addr[,…]`（三段式必填）。所有节点必须传**完全相同**的值；仅首次建群（数据目录为空）生效，已有状态自动 resume，seed 与成员表不一致仅 WARN 不覆盖。校验：地址查重、拒绝 0.0.0.0/::、端口 1-65535、本节点必须在表中且地址与 `--raft-addr`/`--http-addr` 一致 |
| `--join <url>` | string | — | 集群模式：加入集群（指定任一实例 HTTP 端点，如 `http://127.0.0.1:8384`）。命中 follower 自动跟随 428 leader_hint；409（已在集群）视为幂等成功 |

### 3.2 网络

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `--http-addr <host:port>` | string | `127.0.0.1:8384` | HTTP 监听地址（管理面）。集群/容器内必须用**可路由地址**（服务名或具体 IP，不可用 0.0.0.0，见坑 C1） |
| `--raft-addr <host:port>` | string | `127.0.0.1:8385` | Raft 内部 RPC 地址 |
| `--grpc-addr <host:port>` | string | `127.0.0.1:8383` | 数据面 gRPC 地址（SDK） |

### 3.3 存储

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `--data-dir <path>` | string | — | 数据目录（redb）。**集群模式必填**；dev-single 缺省为内存存储（重启数据丢失） |
| `--version-retention <n>` | u64 | `0` | 版本保留数（0=全量保留；后台裁剪任务仅在 >0 时启用） |
| `--audit-retention <n>` | u64 | `100000` | 审计保留条数（0=不裁剪） |
| `--watch-event-retain <n>` | u64 | `10000` | 进程内广播事件缓冲容量（重放仍走版本链） |

### 3.4 主密钥（secret 项加密）

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `--master-key-file <path>` | string | — | 主密钥文件（raw 32B）；环文件自动落在同目录 `<name>.ring.json`（0600） |
| `--allow-no-master-key` | bool | — | 允许无主密钥启动（无 secret 的开发/演示环境逃生门；**生产禁止**） |
| `--gen-master-key` | bool | — | 生成新主密钥（base64 32B）并退出 |
| `--rotate-master-key <key>` | string | — | 客户端模式：向 `--admin-endpoint` 发起主密钥轮换后退出（需 `--admin-password` 或 `--admin-token`） |

> 主密钥解析：`DSH_MASTER_KEY`（base64 32B）优先，否则 `--master-key-file`（raw 32B）；
> 两者都缺且未给 `--allow-no-master-key` → **拒绝启动**（design-v2 §7.4）。

### 3.5 认证与安全

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `--admin-password <pw>` | string | 首启随机生成并打印 | 管理员密码（global，客户端模式也用于登录） |
| `--session-ttl <sec>` | u64 | `86400`（24h） | 会话 TTL（0=不自动过期） |
| `--join-token <token>` | string | — | join 端点鉴权 Bearer；**集群模式强制**，全集群相同 |
| `--raft-token <token>` | string | — | Raft RPC 共享令牌；**集群模式强制**，全集群相同（防伪造 vote/append） |
| `--trusted-proxy <cidrs>` | string | — | 可信代理 CIDR 列表（逗号分隔，如 `10.0.0.0/8,192.168.0.0/16`）；仅信任来自这些网段的 X-Forwarded-For 作登录节流键（F4） |
| `--admin-endpoint <url>` | string | `http://127.0.0.1:8384` | 客户端模式管理面端点（global） |
| `--admin-token <token>` | string | — | 客户端模式会话令牌（global）；缺省用 `--admin-password` 登录获取 |
| `--ops-token <token>` | string | — | 客户端模式**忘记密码恢复**凭据（global，仅 `reset-password` 用）；值 = 节点 `--join-token`；缺省回退环境变量 `DSH_OPS_TOKEN` / `DSH_CLUSTER_TOKEN` |

> **数据面鉴权（project-token）**：数据面 `/v1/*` 与 gRPC 一律要求**项目访问令牌**（`Authorization: Bearer <token>` 或 SSE `?token=`）。
> 令牌在 Admin UI 项目页「访问令牌」Tab 或 `POST /api/v1/projects/{p}/tokens` 创建（**仅超级管理员**）；
> 每项目多令牌并存、可独立吊销（轮换零中断）、SHA-256 落盘（明文仅创建响应一次）。
> `--dev-single` 启动时自动生成全局开发 token 打印（仅 dev 模式）。已移除 `--data-plane-token`。

### 3.6 发布策略与灰度

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `--publish-policy <block|warn>` | enum | `block` | 发布校验策略：block=校验失败拒绝（默认）；warn=仅记录继续发布 |
| `--shared-cascade <auto|manual>` | enum | `auto` | 共享发布级联模式：auto=自动级联引用分支；manual=只更共享版本 |
| `--read-mode <stale|linear>` | enum | `stale` | 读取模式：stale=本地直读（默认，零破坏）；linear=ReadIndex 门控（follower 读返回 ERR_LEADER_REDIRECT + leader http，客户端跟随） |
| `--gray-rollback-threshold <pct>` | f64 | `0.0` | 灰度自动回滚阈值（本地 HTTP 5xx 比例 %；0=禁用） |
| `--gray-rollback-interval <sec>` | u64 | `60` | 灰度自动回滚检查间隔（测试可调小） |

### 3.7 客户端运维子命令（不启动服务）

`defing admin <cmd>`（需 `--admin-endpoint`，配合 `--admin-password`/`--admin-token`；唯一例外是 `reset-password`——免旧密码/会话，改用 `--ops-token` = 节点 `--join-token`）：

| 子命令 | 说明 |
|---|---|
| `gen-master-key` | 生成新主密钥（base64 32B）并打印指引（等价顶层 `--gen-master-key`） |
| `rotate-master-key <new_key>` | 轮换主密钥（调管理面 API；DEK 重包由后台任务执行） |
| `force-logout` | 强制下线当前管理员会话（I7 兜底） |
| `set-password <password>` | 修改管理员密码（旧会话失效；≥6 位，需已持有会话） |
| `reset-password <password>` | **忘记密码恢复**：免旧密码/会话，仅需 `--ops-token`（= 节点 `--join-token`）；改后全部管理员会话下线 |
| `promote --node <id>` | learner → voter |
| `remove-node --node <id>` | 移除节点 |
| `snapshot [--out <path>]` | 触发备份快照（状态机 KV dump；可存盘） |
| `retention-status` | 查看保留策略状态（`--version-retention`/`--audit-retention` 配置） |

---

## 4. 主密钥配置

### 4.1 生成

```bash
defing --gen-master-key            # 输出一行 base64（32B），如 q4UbfiDw6b7wRGnKepAu2Xa0msYz/hIQOGlGr2uhcy8=
# 等价：defing admin gen-master-key
```

### 4.2 注入（二选一）

```bash
# 方式 A：环境变量（推荐，容器/CI 友好）
export DSH_MASTER_KEY="<上面生成的 base64>"
defing --dev-single --data-dir ./data ...

# 方式 B：文件（raw 32B）
head -c 32 /dev/urandom > /etc/defing/master.key
chmod 600 /etc/defing/master.key
defing --dev-single --data-dir ./data --master-key-file /etc/defing/master.key ...
```

### 4.3 轮换

```bash
# 客户端模式轮换（需要管理面可达）
defing admin rotate-master-key "<新 base64 32B>" --admin-endpoint http://127.0.0.1:8384 --admin-password <pw>
# 或顶层参数形式
defing --rotate-master-key "<新 base64 32B>" --admin-endpoint http://127.0.0.1:8384 --admin-password <pw>
```

- 轮换后旧 KEK 保留在密钥环文件（`<master-key-file>.ring.json`，0600），旧密文仍可解；
  环文件损坏/解析失败会告警但不清空（N3）。
- 集群模式轮换经 Raft apply，各节点自动更新 keyring 并持久化环文件（幂等、重放安全）。

### 4.4 无 secret 的演示环境

```bash
defing --dev-single --admin-password admin123 --allow-no-master-key --http-addr 127.0.0.1:8384
```

> ⚠️ 生产环境必须配置主密钥（否则 secret 类型配置项无法创建/读取），且**不要**使用
> 仓库 compose 里的开发默认密钥（泄露即全部 secret 可解）。

---

## 5. 部署方式一：单机

### 5.1 开发联调（`--dev-single`，内存存储）

```bash
defing --dev-single --admin-password admin123 --allow-no-master-key --http-addr 127.0.0.1:8384
# 管理面:  http://127.0.0.1:8384  （/admin 内嵌控制台，/metrics，/healthz，/readyz）
# 数据面:  GET  /v1/projects/{p}/branches/{b}/snapshot   （SDK 拉配置，纯值+版本号）
#          SSE  /v1/projects/{p}/branches/{b}/watch       （订阅发布事件）
#          gRPC 127.0.0.1:8383                            （SDK 首选通道）
```

> 无 `--data-dir` 时数据在内存，**重启即清空**——只适合联调/测试。

### 5.2 单机持久化（`--dev-single` + `--data-dir`）

适合小规模单实例生产（无 Raft 冗余，故障即服务中断）：

```bash
export DSH_MASTER_KEY="$(defing --gen-master-key)"
defing --dev-single --data-dir /var/lib/defing \
  --http-addr 0.0.0.0:8384 --grpc-addr 0.0.0.0:8383 \
  --admin-password '<强密码>' --session-ttl 86400 \
  --version-retention 50 --audit-retention 200000
```

> 数据面鉴权改为**项目访问令牌**（无 `--data-plane-token`）：启动后登录 Admin UI，
> 在每个项目页「访问令牌」Tab 创建令牌并分发给对应 SDK 客户端。

### 5.3 systemd 服务示例

```ini
# /etc/systemd/system/defing.service
[Unit]
Description=Defing Config Service
After=network.target

[Service]
User=defing
Group=defing
EnvironmentFile=/etc/defing/defing.env     # 内含 DSH_MASTER_KEY=...
ExecStart=/usr/local/bin/defing --dev-single --data-dir /var/lib/defing \
  --http-addr 0.0.0.0:8384 --grpc-addr 0.0.0.0:8383 \
  --admin-password ${ADMIN_PASSWORD}
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

---

## 6. 部署方式二：集群（3 节点为例）

> 集群 = 强一致 Raft。推荐**奇数节点**（3/5），容忍 (N-1)/2 节点故障。
> 两种建群方式不可混用：同集群要么全部用 `--bootstrap-peers`，要么 `--bootstrap`+`--join`。

### 6.1 方式 A（推荐）：静态成员表 `--bootstrap-peers`

三节点传**完全相同**的三段式成员表 `node_id@raft_addr@http_addr`，并行启动直接选举，
全员 voter，无需 join/promote：

```bash
SEED="1@127.0.0.1:8385@127.0.0.1:8384,2@127.0.0.1:8387@127.0.0.1:8386,3@127.0.0.1:8389@127.0.0.1:8388"

defing --node-id 1 --bootstrap-peers "$SEED" --http-addr 127.0.0.1:8384 --raft-addr 127.0.0.1:8385 \
  --data-dir ./n1 --admin-password admin123 --allow-no-master-key --join-token demo --raft-token demo
defing --node-id 2 --bootstrap-peers "$SEED" --http-addr 127.0.0.1:8386 --raft-addr 127.0.0.1:8387 \
  --data-dir ./n2 --admin-password admin123 --allow-no-master-key --join-token demo --raft-token demo
defing --node-id 3 --bootstrap-peers "$SEED" --http-addr 127.0.0.1:8388 --raft-addr 127.0.0.1:8389 \
  --data-dir ./n3 --admin-password admin123 --allow-no-master-key --join-token demo --raft-token demo
```

要点：
- seed 仅首次建群生效；已有数据（重启/crash 恢复）自动 resume，seed 与成员表不一致只 WARN（不覆盖）；
- seed 校验失败**启动即报错**：三段式必填、raft/http 地址各自不得重复、拒绝 0.0.0.0/::、
  端口 1-65535、本节点必须在表中且 `raft_addr`/`http_addr` 与本地参数一致；
- 运行期扩缩容不走 seed，见 §6.3。

### 6.2 方式 B：`--bootstrap` + `--join`（动态扩容）

```bash
# 节点 1：自举建群
defing --node-id 1 --bootstrap --http-addr 127.0.0.1:8384 --raft-addr 127.0.0.1:8385 \
  --data-dir ./n1 --admin-password admin123 --allow-no-master-key --join-token demo --raft-token demo
# 节点 2、3：加入（可指向任意已在线节点，自动跟随 leader）
defing --node-id 2 --join http://127.0.0.1:8384 --http-addr 127.0.0.1:8386 --raft-addr 127.0.0.1:8387 \
  --data-dir ./n2 --admin-password admin123 --allow-no-master-key --join-token demo --raft-token demo
defing --node-id 3 --join http://127.0.0.1:8384 --http-addr 127.0.0.1:8388 --raft-addr 127.0.0.1:8389 \
  --data-dir ./n3 --admin-password admin123 --allow-no-master-key --join-token demo --raft-token demo
```

加入后为 **learner**，需提升为 voter：

```bash
# 管理员登录（任意节点，非 leader 自动转发；token 集群级有效）
TOKEN=$(curl -s -X POST http://127.0.0.1:8384/api/v1/login \
  -H 'Content-Type: application/json' -d '{"password":"admin123"}' | python3 -c "import json,sys;print(json.load(sys.stdin)['token'])")
AUTH="Authorization: Bearer $TOKEN"
curl -s -H "$AUTH" -X POST http://127.0.0.1:8384/api/v1/cluster/promote \
  -H 'Content-Type: application/json' -d '{"node_id": 2}'
curl -s -H "$AUTH" -X POST http://127.0.0.1:8384/api/v1/cluster/promote \
  -H 'Content-Type: application/json' -d '{"node_id": 3}'
```

### 6.3 集群运维

| 操作 | 方法 |
|---|---|
| 查看成员/leader | `GET /api/v1/cluster/members`（Bearer） |
| 新增节点（扩容） | 新节点 `--join http://<任一在线节点>:8384`（learner 加入，幂等）→ promote 为 voter |
| 移除节点（缩容） | `POST /api/v1/cluster/remove {"node_id": 4}`（Bearer）；或 `defing admin remove-node --node 4` |
| 重启/崩溃恢复 | **同 data-dir 直接启动**（无需再传建群参数，自动 resume；join 409 幂等成功） |
| 备份 | `defing admin snapshot --out backup.json` 或 `GET /api/v1/admin/snapshot` |
| 密码修改 | `defing admin set-password '<新密码>'`（旧会话下线） |
| 忘记密码恢复 | `defing admin reset-password '<新密码>' --ops-token '<join-token>'`（免旧密码；节点需已配置 `--join-token`，未配置恒 401；旧会话全下线） |
| 强制下线 | `defing admin force-logout` |

> **重启恢复幂等（坑 C3/C4 根治）**：二进制内部以 raft-meta 持久化状态判断——已有状态即忽略
> `--bootstrap`/`--bootstrap-peers`/`--join` 直接 resume。因此 docker/k8s 的启动命令可以
> **静态书写**（每次启动同一参数），无需 shell 判断数据目录。

### 6.4 集群配置要点（坑 C1/C2 摘要）

1. **地址必须可路由**：容器/多机场景 `--http-addr`/`--raft-addr` 与成员表一律用服务名或具体 IP，
   禁用 `0.0.0.0`/`127.0.0.1` 上报（NodeInfo 会被 leader 用来复制/转发，通配地址会指向错节点）；
2. **healthcheck 探测同一地址**：bind 到服务名后容器内自检也要用服务名（不是 127.0.0.1）；
3. **令牌一致性**：`--join-token`/`--raft-token` 集群内所有节点必须相同（F3/S5 强制）；
4. 详细踩坑见 [dev_docs/defing-cluster.md](defing-cluster.md)。

### 6.5 Follower 永久落后排障（snapshot RPC 与 request body limit）

- **Raft RPC snapshot 分块流（P2b2:0.10 streaming msgpack frames）**：追落后 follower 经
  `InstallSnapshot` 流帧（`Header`/`Chunk`/`End`,wire = `[u32 BE len][frame]`),chunk payload
  ≤ `snapshot_max_chunk_size` = 1MiB（msgpack binary,no JSON numeric-array inflation — old
  0.9's ~3.1x resolved by streaming).**注意 body limit 的粒度**：一次 `POST /raft/full-snapshot`
  请求承载**整份快照**（可到数百 MB），不是一帧一个请求；服务端该路由用
  `DefaultBodyLimit::disable()`，其余 `/raft/*` RPC 仍是 **32MiB**。因此反向代理/网关
  （nginx `client_max_body_size`、ingress、ALB 等）必须按**最大快照尺寸**配置
  （design §8.4：上限 ≥ 最大快照，且建议 `proxy_request_buffering off` 以流式转发），
  只按「per-frame ~1MiB+4KiB」配置会让 snapshot POST 恒 HTTP 413 `length limit exceeded`，
  follower 经 snapshot 路径无法追平。服务端 `snapshot_stream` 的 `MAX_SNAPSHOT_BYTES = 640MiB`
  是单次 in-flight 快照的硬上限；追平期间磁盘峰值约为**快照的 3 倍**（旧 canonical + 收到的
  payload + install 的 canonical 副本），规划 data 分区时按此预留。
- **follower 长期落后，排查顺序**：
  1. `GET /api/v1/cluster/members` 看 per-member `lag`（= `last_log_index` − `last_applied`；
     恒 `null` 时查询非 leader 节点——leader 观察的 replication 数据只在 leader）；
  2. leader `/metrics` 看 `dsh_raft_replication_lag{target="<node_id>"}`；
  3. 节点日志 grep 413 / `InstallSnapshot RPC timed out`（→ 代理 body limit 或 timeout 配置）。
- **相关配置**（内置,`dsh-raft` `dev_config()`）:`install_snapshot_timeout = 30s`（默认
  200ms too short — streamed chunk transfer + disk fsync exceed a single-chunk 200ms budget →
  追平循环失败）；`snapshot_max_chunk_size = 1MiB`（msgpack frame payload ≤1MiB,与 frame-level
  body limit 裕头；P3a-C1 also caps log-reader batches at `MAX_BATCH_BYTES` = 6MiB stored
  bytes — that bounds AppendEntries RPC size, not snapshot frames).

### 6.6 快照 payload v2（msgpack）发布与「恢复中断」运维语义（P4）

**两阶段发布（不要跳步）**：新格式（v2：20B 头 `DSHSNP | version | flags | total_len | crc32` +
msgpack body）在**读端**已全部兼容（v1 无头 JSON / v1 带头 JSON / v2 都认），**写端默认仍是 v1**。

1. 先滚动升级到「读端双兼容」的版本（本文对应的版本），确认全集群无旧二进制；
2. 再给**所有节点**加 `DSH_RAFT_SNAPSHOT_PAYLOAD=msgpack` 并滚动重启（写端 v2）；启动日志会打印
   `snapshot payload writer: v2 (msgpack + total_len/crc32 header)`；
3. **回滚**：去掉该环境变量并重启即可（写端回到无头 JSON）；已落盘的 v2 payload 仍可被读/安装，
   不需要回滚数据。**不要**让旧于「读端双兼容」的二进制去读 v2 payload（会报
   `unsupported snapshot payload version 2`，该节点无法经快照追平）。

**payload 完整性**：带头的 payload 在**发送前**（`verify_payload_file`）、**安装时**、**boot 重放时**
三处都校验 `total_len + crc32`；校验失败报错并**保留文件**（不静默删除）。无头旧文件没有完整性头，
只能靠帧级 CRC 与传输层。

**恢复中断（restore-in-progress）语义**：安装快照的 restore 是分块写（无单事务原子性）。安装开始时
在 raft-meta 写 `restore_begin{meta, payload}`，registry 提交后清除；重启若见到该标记：

- 用同一 payload **重放一次全量 restore**（幂等）后才继续启动 → 异常重启可能多花一次 restore 时间
  （400MB 级别约 1–2s），运维手册按此预留启动探针的 `initialDelaySeconds`；
- payload 缺失/损坏、标记损坏 → **拒绝启动**（日志给出 payload 路径 + 从备份恢复或清卷重建的指引），
  不会带着「部分新 + 部分旧」的混合状态起来；
- `DSH_RAFT_RESTORE_MARKER=0` **只关闭写标记**（发布期兜底；崩溃窗口重新变大，WARN 日志会说明
  「崩溃不可重放」），已有的标记**仍会被重放**。
- `DSH_RAFT_RESTORE_FORCE=1` 才是「忽略标记、允许混合状态启动」的逃生门。实际保证（经 Raft 启动
  路径测试固定）：
  - 节点**没有 purge 过的日志、且没有未应用的 committed 条目** → 能以 poison 启动，HTTP 503、
    `/healthz` 200、`/readyz` 503；ERROR 日志 + 需人工确认数据一致性后使用。
  - 节点**已有 purge 过的日志**（快照追平后的正常状态）或 `last_applied < committed` → openraft
    启动期会强制重建快照/重放日志，而 poisoned 状态拒绝这两件事 → **启动失败（fail-fast）**，
    这是有意的安全行为（混合状态不能拿来构建快照或继续 apply）。此时只能**清卷重建/从备份恢复**，
    不要再指望 FORCE 把节点拉起来。
  - 该 fail-stop 是**持久化**的（raft-meta 的 `restore_poison` 键）：重启后仍是 503；FORCE 会
    **保留** restore 标记——下次不带 FORCE 启动时走正常重放（安全路径）并自动解除 poison。
  - 已 poison 启动的节点若 leader 先给它发的是普通日志（而不是快照），openraft 会把 apply 的
    Err 当 fatal：**Raft core 停止、进程仍在**（`/healthz` 仍 200、`/readyz` 与 API 503，
    k8s 不会因此重启 pod）→ 需要人工重启/清卷重建；「等 leader 重发快照自愈」只在 leader 的
    下一条消息是快照时成立。

**恢复失败后的 fail-stop（poison）**：非崩溃型 restore 错误（如磁盘 IO）会让节点进入 fail-stop：

- HTTP 管理面/数据面（除 `/healthz`、`/metrics`、`/admin*`）返回 **503 `ERR_RESTORE_POISONED`**，
  `/readyz` 503（LB 摘除），gRPC 数据面 UNAVAILABLE；日志有 `state machine POISONED (fail-stop)`；
- 处置：**重启该节点**（带标记 → boot 重放；无标记 → 需要重装快照/重建）。不要直接放流量。

**内存/磁盘**：
- 读端（install / boot 重放）按 **16MiB** 分批恢复（v2 流式，内存 ≈ 一批 + redb 事务缓冲；
  v1 无头 JSON 仍是整份解析的升级期路径）。
- 写端（v2）按 **64MiB** 分页 dump + 逐页编码直接落盘，**不再整份驻留**；代价是快照构建期间
  leader 的写命令会等待一次写盘时间（日志行 `snapshot built: ... elapsed=...` 可观测；
  生产 p99 写延迟标定留 P5）。
- 闸门口径：storm 用**绝对净增量 RSS**（`max(窗口采样 0.4s, VmHWM) - 重启基线`），预算
  `192MiB + payload`（随规模放大；`--max-rss-delta-mb>0` 只能收紧）：固定开销 192MiB + 允许一份
  payload 副本，「整份额外驻留」回归会超预算。payload/redb 比值只作观测（JSON payload 会被编成
  十进制数组放大 ~4x，`dsh.redb` 含 raft 表/free page 且按 region 取整，都不适合当判定基准）。
- **install_snapshot_timeout**：`DSH_RAFT_INSTALL_SNAPSHOT_TIMEOUT_MS`（默认 30000ms）是 openraft
  0.10 传给 InstallSnapshot RPC 的 `hard_ttl`。v1 JSON 快照可达 GB 级，30s 内传不完会反复超时
  重试（滚动演练里 1GB payload 实测 30s 不足）。按 `timeout ≥ payload_bytes / 带宽 + 安装时间`
  配置（例如 1GB / 35MB/s ≈ 30s，建议 ≥ 120000；v2 msgpack 约 1/4 大小，30s 通常够）。
- 磁盘峰值仍是快照的约 3 倍（旧 canonical + 收到的 payload + install 的 canonical 副本）。

**降级到 P4 之前的二进制**：旧二进制不认识 v2 payload（`DSHSNP` 头 → `unsupported snapshot
payload version 2`），无法安装/读取。步骤：先停旧版本滚动、把**所有节点**的
`DSH_RAFT_SNAPSHOT_PAYLOAD` 去掉并重启（写端回 v1）→ 删除各节点 `{data-dir}/snapshots/*.snap`
（registry 指向的文件让 leader 重发）→ 再降级。不要在 registry 仍指向 v2 payload 时启动旧二进制。

---

## 7. 部署方式三：Docker / docker-compose

### 7.1 镜像构建（两种方式）

**A. 多阶段自构建（deploy/Dockerfile）**：rust:1.97 编译 → debian:bookworm-slim 运行
（非 root 用户 `defing` uid 10001，entrypoint 内降权）：

```bash
docker build -t defing:latest -f deploy/Dockerfile .
```

**B. 根 Dockerfile（拷贝预构建二进制）**：先本地编译，再打进 ubuntu:24.04 镜像：

```bash
cd server && cargo build --release -p dsh-cli && cd ..
docker build -t defing:latest .
# 根 Dockerfile 内容即：FROM ubuntu:24.04 + COPY server/target/release/defing /usr/local/bin/defing
```

> 部署镜像：镜像内命令为 `defing ...`（二进制已在 PATH），入口 `/docker-entrypoint.sh`
> 负责 root → chown /data → su-exec defing 降权；EXPOSE 8383 8384 8385。

### 7.2 单实例 docker-compose（本机开发/测试）

仓库根 `docker-compose.local.yml`（compose project：`dsh-local`），包含 4 个服务：

| 服务 | 作用 |
|---|---|
| `builder` | 一次性构建器（profile: tools）：容器内 cargo build --release，产物拷到宿主 `local-bin/defing` |
| `defing` | 单实例运行：`--dev-single --http-addr 0.0.0.0:8384 --admin-password dev-admin-pass --data-dir /app/data`，端口映射 `127.0.0.1:8384:8384`，主密钥经 `DSH_MASTER_KEY` 注入 |
| `example-setup` | 一次性初始化 example 站点配置（profile: tools） |
| `example-site` | example 站点（http://127.0.0.1:8000），经 SDK 拉配置 + watch 热更新 |

```bash
# 1) 构建并拷贝二进制到 local-bin/
docker compose -f docker-compose.local.yml run --rm builder
# 2) 启动单实例（HTTP API 映射本机 8384）
docker compose -f docker-compose.local.yml up -d defing
# 3) （可选）初始化 example 站点并启动
docker compose -f docker-compose.local.yml run --rm example-setup
docker compose -f docker-compose.local.yml up -d example-site
# 4) 冒烟
curl -s -X POST http://127.0.0.1:8384/api/v1/login -H 'Content-Type: application/json' -d '{"password":"dev-admin-pass"}'
```

> ⚠️ compose 内置的 `DSH_MASTER_KEY` 默认值仅供本机测试；生产务必用 `DSH_MASTER_KEY=<你的密钥>`
> 覆盖（泄露即全部 secret 可解）。

### 7.3 3 节点集群 docker-compose（生产示例）

`deploy/docker-compose.yml`（3 节点，静态成员表建群，含坑 C1/C3 修复）与
`dev_docs/docker-compose.yml.demo`（基于已构建镜像的变体）：

```bash
# 宿主环境变量：强密码 + 集群令牌（join/raft 共用）
export DSH_ADMIN_PASSWORD='<强密码>'
export DSH_CLUSTER_TOKEN='<随机串>'
docker compose -f deploy/docker-compose.yml up --build
# 端口：18384 / 28384 / 38384 → 各节点 HTTP API
```

compose 要点：
- 成员表用**服务名**（`1@node1:8385@node1:8384,2@node2:8385@node2:8384,3@node3:8385@node3:8384`），
  经 `DSH_BOOTSTRAP_PEERS` 环境变量传入（compose 插值 → CLI 参数）；
- `DSH_ADMIN_PASSWORD` / `DSH_CLUSTER_TOKEN` 由宿主环境注入（compose 默认 changeme/demo，生产必改）；
- 每个节点独立 named volume（`n1`/`n2`/`n3`）挂 `/data`；
- healthcheck 探测各自服务名（坑 C2 修复）；启动命令静态幂等（坑 C3/C4 修复）；
- 需要主密钥时给每个服务加 `DSH_MASTER_KEY` 环境变量（全部节点相同），并去掉演示用的
  `--allow-no-master-key`。

### 7.4 容器化注意事项

1. **不要用 0.0.0.0 作为 NodeInfo**：容器内 `--http-addr`/`--raft-addr` 与成员表用服务名；
2. **healthcheck 用服务名探测**：bind 到服务名后 127.0.0.1 探测必失败（坑 C2）；
3. **静态启动命令**：重启自动 resume，勿用 shell 条件包装（坑 C3/C4）；
4. **数据卷权限**：entrypoint 自动 chown /data（root 启动 → 降权 defing）；若显式指定非 root user，
   需自行保证数据卷属主；
5. **host 端口冲突**：宿主已有 8384 占用时映射到其他端口（示例用 18xxx）。

---

## 8. 环境变量参考

### 8.1 服务端（defing 进程读取）

| 变量 | 必填 | 说明 |
|---|---|---|
| `DSH_MASTER_KEY` | 有 secret 时必填 | 主密钥（base64 32B）；与 `--master-key-file` 二选一，env 优先 |

### 8.2 docker-compose 插值变量（compose 语法，非 defing 读取）

| 变量 | 用于 | 说明 |
|---|---|---|
| `DSH_BOOTSTRAP_PEERS` | compose 内 `$${DSH_BOOTSTRAP_PEERS}` | 静态成员表（各服务 environment 里定义，command 引用） |
| `DSH_ADMIN_PASSWORD` | `${DSH_ADMIN_PASSWORD:-changeme}` | 管理员密码（宿主注入） |
| `DSH_CLUSTER_TOKEN` | `${DSH_CLUSTER_TOKEN:-demo}` | 同时作为 join-token 与 raft-token（宿主注入） |

### 8.3 SDK / 示例程序（客户端侧）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DSH_ENDPOINT` | `http://dsh:8384` | 配置服务地址（example/app.py；也接受端点列表） |
| `DSH_PROJECT` / `DSH_BRANCH` / `DSH_GROUP` | `example-site` / `dev` / `site` | 配置坐标（项目/分支/分组） |
| `DSH_ENDPOINTS` / `DSH_GRPC` / `DSH_HTTP` | — | Go/Python SDK 的端点列表与通道地址 |
| `ADMIN_PASSWORD` | — | example-setup 初始化时使用的管理员密码 |

---

## 9. SDK 接入配置

三语言 SDK 均以**端点列表**配置（含 gRPC 时优先走 gRPC 数据面，纯 HTTP 端点自动降级 HTTP/SSE）：

```ts
// TypeScript
import { ConfigClient } from './sdk/ts/src/index.ts';
const c = new ConfigClient([{ grpc: '127.0.0.1:8383', http: 'http://127.0.0.1:8384' }], { token: '<项目访问令牌>' });
const snap = await c.get('my-app', 'dev');        // 读活动版本
c.watch('my-app', 'dev', (e) => console.log(e));  // 订阅发布事件（断线 after_version 续传）
await c.listMembers();                            // 集群成员（端点池刷新）
```

```go
// Go
import "github.com/.../sdk/go/configclient"
c := configclient.NewGrpc("127.0.0.1:8383", "<项目访问令牌>")  // gRPC 数据面
// 或 c := configclient.New([]string{"http://127.0.0.1:8384"}, "<项目访问令牌>")  // HTTP 降级
```

```python
# Python
from sdk.python import ConfigClient
c = ConfigClient([{'grpc': '127.0.0.1:8383', 'http': 'http://127.0.0.1:8384'}], token='<项目访问令牌>')
snap = c.get('my-app', 'dev')
```

> 数据面鉴权（project-token）：SDK 调用需带 `authorization: Bearer <token>`（gRPC metadata 同构；
> HTTP SSE 亦支持 `?token=`）。令牌在 Admin UI 项目页「访问令牌」Tab 创建（仅超级管理员）。
>
> **构建脚本取值（curl）**：`GET /v1/projects/{p}/branches/{b}/config?format=yaml|json|toml|env&version=<n>`
> （`env` = .env 文件格式：`KEY=VALUE`，键大写、无分组前缀，可直接 `> .env` 落盘）
> 输出指定格式配置（secret 掩码；`reveal=true` 需管理面会话）。
> `curl -s "http://<host>:8384/v1/projects/{p}/branches/dev/config?format=yaml" -H "Authorization: Bearer <token>"`
> Admin UI 项目页「访问令牌」Tab 展示可复制命令。

---

## 10. 生产安全配置清单

| 项 | 要求 |
|---|---|
| 主密钥 | 必配（`DSH_MASTER_KEY` 或 `--master-key-file`），强随机 32B；勿用仓库演示默认值 |
| 管理员密码 | `--admin-password` 强密码（≥12 位混合）；勿用默认；这是**超级管理员**（根账号）的凭据 |
| 全局管理员（GA） | 由超级管理员在 Admin UI「全局管理员」页创建/删除/改密；可管理全部项目与共享配置但**不能管理账号、集群写、访问令牌**；GA 丢密由超级管理员重置 |
| 集群升级顺序 | **先全集群升级到含 GA 的版本，再创建第一个 GA**：旧节点无法反序列化 `GlobalAdmin*` 命令变体（日志整条解码失败）|
| 集群令牌 | `--join-token`/`--raft-token` 强随机（≥32 hex），全集群一致，仅内网可达 raft 端口 |
| 项目访问令牌 | **每个项目都配置访问令牌**（Admin UI/API 创建，仅超级管理员）；SDK 携带 Bearer；泄露即吊销重建 |
| 监听绑定 | 生产绑定内网地址/防火墙限流，8385 raft 端口绝不暴露公网 |
| 可信代理 | 前置反代时配 `--trusted-proxy`（否则登录节流键可被伪造 XFF 绕过，F4） |
| 会话 | `--session-ttl` 按需（默认 24h；0=永不过期，谨慎） |
| 密钥环 | `<master-key-file>.ring.json` 权限 0600，勿入库/勿入镜像层 |
| 运行用户 | 容器内已降权 `defing`（uid 10001）；裸机部署建议专用用户 + systemd |

---

## 11. 配置方法变更记录

近期提交改变了以下配置方式，升级/迁移时注意（对应 git 提交见括号内）：

| 变更 | 旧（dsh 时代） | 新（defing） |
|---|---|---|
| 二进制名 | `dsh` | **`defing`**（58aa312）；compose/脚本/systemd 中命令与镜像内路径需同步改名 |
| 集群建群 | bootstrap + join + promote（唯一方式） | **推荐静态成员表 `--bootstrap-peers`**（2d125e8，全员 voter 无需 promote）；bootstrap+join 保留为动态扩容路径 |
| 集群令牌 | 可选 | **`--join-token`/`--raft-token` 集群模式强制**（F3/S5） |
| 主密钥 | 可选（缺省无加密） | **默认拒绝无密钥启动**；`--allow-no-master-key` 仅为开发/演示逃生门（design-v2 §7.4） |
| 构建依赖 | 需 C++ 工具链 + CXXFLAGS（rocksdb） | **纯 Rust redb**，无需 C++ 工具链（4b07d2a）；CXXFLAGS 仅为构建脚本遗留 |
| 新增配置旋钮 | — | `--publish-policy` / `--shared-cascade` / `--read-mode` / `--gray-rollback-threshold` / `--gray-rollback-interval`（G1/G5） |
| 重启恢复 | shell 判目录 | **二进制内自动 resume**（raft-meta 判断），compose/k8s 可静态命令（坑 C3/C4） |
| 容器地址 | 示例曾用 0.0.0.0 | **必须服务名/可路由地址**（坑 C1），healthcheck 同步（坑 C2） |

---

## 12. 相关文件索引

| 文件 | 说明 |
|---|---|
| `server/crates/dsh-cli/src/main.rs` | CLI 参数定义与启动装配（权威参数表） |
| `docker-compose.local.yml` | 本机单实例 compose（构建/运行/example） |
| `deploy/docker-compose.yml` | 3 节点集群 compose（生产示例） |
| `deploy/Dockerfile` / `Dockerfile` | 多阶段自构建 / 预构建二进制拷贝 |
| `deploy/docker-entrypoint.sh` | 容器入口（chown + 降权） |
| `dev_docs/docker-compose.yml.demo` | 基于已构建镜像的集群 compose 变体 |
| `dev_docs/defing-cluster.md` | 容器化集群部署踩坑记录（坑 A/B/C 系列） |
| `scripts/cluster-demo.sh` / `dev-single-demo.sh` / `seed-demo.sh` | 端到端演示脚本（可作配置范本） |
| `scripts/build-env.sh` / `build-linux-x86.sh` | 构建环境与交叉编译 |
