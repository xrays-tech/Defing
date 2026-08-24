---
layout: default
title: 01 部署与启动
prev: {title: 快速开始, url: /quickstart/}
next: {title: 02 项目与分支, url: /02-project/}
---
# 01 部署与启动

## 1.1 单机联调（--dev-single）

最快的方式是内存模式，适合体验与联调：

```bash
defing --dev-single --admin-password admin123 --allow-no-master-key --http-addr 127.0.0.1:8384
# 管理面: http://127.0.0.1:8384/admin
# 数据面: /v1/projects/{p}/branches/{b}/snapshot（SDK 拉配置）
#         /v1/projects/{p}/branches/{b}/watch   （订阅事件）
#         gRPC 127.0.0.1:8383                   （SDK 首选通道）
```

> 不带 `--data-dir` 时数据在内存，**重启即清空**。
> 启动时会打印一行 **开发数据面 token**（`--dev-single` 全局有效），SDK / curl 拉取配置需要携带。

## 1.2 单机持久化

```bash
defing --dev-single --data-dir /var/lib/defing \
  --http-addr 0.0.0.0:8384 --grpc-addr 0.0.0.0:8383 \
  --admin-password '<强密码>' --master-key-file /etc/defing/master.key
```

- `--data-dir`：持久化数据目录（redb 存储）
- `--master-key-file` / `DSH_MASTER_KEY`：主密钥（secret 配置项必需，`defing --gen-master-key` 生成）

## 1.3 集群（3 节点）

推荐静态成员表建群，三节点传**完全相同**的成员表并并行启动：

```bash
SEED="1@127.0.0.1:8385@127.0.0.1:8384,2@127.0.0.1:8387@127.0.0.1:8386,3@127.0.0.1:8389@127.0.0.1:8388"
defing --node-id 1 --bootstrap-peers "$SEED" --http-addr 127.0.0.1:8384 --raft-addr 127.0.0.1:8385 --data-dir ./n1 --admin-password admin123 --join-token demo --raft-token demo
defing --node-id 2 --bootstrap-peers "$SEED" --http-addr 127.0.0.1:8386 --raft-addr 127.0.0.1:8387 --data-dir ./n2 --admin-password admin123 --join-token demo --raft-token demo
defing --node-id 3 --bootstrap-peers "$SEED" --http-addr 127.0.0.1:8388 --raft-addr 127.0.0.1:8389 --data-dir ./n3 --admin-password admin123 --join-token demo --raft-token demo
```

| 参数 | 说明 |
|---|---|
| `--bootstrap-peers` | 三段式成员表 `node_id@raft_addr@http_addr`，全员 voter |
| `--join-token` / `--raft-token` | 集群模式**强制**，全集群相同（join 端点鉴权 / raft RPC 鉴权） |
| `--node-id` | 节点唯一 ID |
| `--join` | 追加节点加入既有集群：指定任一实例 HTTP 端点（如 `--join http://127.0.0.1:8384`） |
| `--data-dir` | 集群模式必需（Raft 日志 + 状态机） |

## 1.4 常用参数速查

| 参数 | 默认 | 说明 |
|---|---|---|
| `--http-addr` | `127.0.0.1:8384` | 管理面 + 数据面 HTTP |
| `--grpc-addr` | `127.0.0.1:8383` | 数据面 gRPC |
| `--raft-addr` | `127.0.0.1:8385` | Raft 内部 RPC（集群模式） |
| `--admin-password` | 首启随机生成 | 管理员密码（登录 Admin UI / API） |
| `--master-key-file` / `DSH_MASTER_KEY` | — | 主密钥（secret 必需；生产必配） |
| `--session-ttl` | `86400` | 管理会话有效期（秒） |
| `--publish-policy` | `block` | 发布校验失败策略：`block` / `warn` |
| `--shared-cascade` | `auto` | 共享发布级联：`auto` 自动级联 / `manual` |
| `--read-mode` | `stale` | 读取模式：`stale` 本地直读 / `linear` ReadIndex 门控 |

> **运维子命令**：`defing admin <cmd>`（客户端模式，不启动服务）——`gen-master-key` / `rotate-master-key` / `force-logout` / `set-password` / `promote` / `remove-node` / `snapshot` / `retention-status`。客户端模式经 `--admin-endpoint`（默认 `http://127.0.0.1:8384`）+ `--admin-token`（或 `--admin-password` 登录）调用管理面。
>
> **构建版本标记**：`/healthz`、`/readyz` 返回 `build` 信息（git 短哈希 + 构建时间），Admin UI 页脚同步显示 `Defing · build <commit> · <时间>`，便于确认部署产物是否为最新构建。

## 1.5 数据面鉴权（项目访问令牌）

数据面（SDK / curl 拉取配置）一律要求**项目访问令牌**：

1. 登录 Admin UI → 进入项目 → 「访问令牌」页签
2. 点「创建令牌」，**明文仅展示这一次**（服务端只存 SHA-256），立即复制保存
3. SDK / curl 携带 `Authorization: Bearer <令牌>`

每个项目可创建多个令牌（轮换零中断：新令牌先分发、旧令牌下次发版后吊销）；令牌**永不过期**，泄露即吊销重建。详见 [07 访问令牌与 SDK]({{ site.baseurl }}/07-tokens/)。
