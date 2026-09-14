# Defing —— 分布式配置文档服务

> 📖 **使用文档**：https://alexli.eu.org/Defing/ （项目介绍 + 分章节操作教程，含 Admin UI 截图）

单二进制分布式配置服务：**Rust 主服务（Raft 集群 + 内嵌 Admin UI + 三语言 SDK）**。
配置按 项目 → 分支（dev/test/prod + 自定义）→ 分组 → item 组织；
修改走"草稿 → 版本 → 发布 → 通知"闭环；结构项目级强一致，仅值按分支不同。

## 快速开始

### 单节点联调（--dev-single）

```bash
server/target/debug/defing --dev-single --admin-password admin123 --allow-no-master-key --http-addr 127.0.0.1:8384
# 管理面:  http://127.0.0.1:8384  （/admin 内嵌控制台，/metrics，/healthz）
# 数据面:  GET  /v1/projects/{p}/branches/{b}/snapshot  （SDK 拉配置，纯值+版本号）
#          SSE  /v1/projects/{p}/branches/{b}/watch      （订阅发布事件）
# 鉴权:    数据面一律需要访问令牌 —— dev-single 启动时打印「开发数据面 token」（全局有效）；
#          生产/集群模式在 Admin UI 项目页「访问令牌」Tab 创建（仅超级管理员，每项目独立、可吊销）
```

### 集群（3 节点）

**方式一（推荐）：静态成员表 `--bootstrap-peers`** —— 三节点传**完全相同**的三段式成员表，
并行启动直接选举，全员 voter，无需 join/promote（研究/设计见 `dev_docs/research-cluster-bootstrap.md`）：

```bash
SEED="1@127.0.0.1:8385@127.0.0.1:8384,2@127.0.0.1:8387@127.0.0.1:8386,3@127.0.0.1:8389@127.0.0.1:8388"
defing --node-id 1 --bootstrap-peers "$SEED" --http-addr 127.0.0.1:8384 --raft-addr 127.0.0.1:8385 --data-dir ./n1 --admin-password admin123 --allow-no-master-key --join-token demo --raft-token demo
defing --node-id 2 --bootstrap-peers "$SEED" --http-addr 127.0.0.1:8386 --raft-addr 127.0.0.1:8387 --data-dir ./n2 --admin-password admin123 --allow-no-master-key --join-token demo --raft-token demo
defing --node-id 3 --bootstrap-peers "$SEED" --http-addr 127.0.0.1:8388 --raft-addr 127.0.0.1:8389 --data-dir ./n3 --admin-password admin123 --allow-no-master-key --join-token demo --raft-token demo
# 三段式必填：node_id@raft_addr@http_addr；条目校验：地址查重、拒绝 0.0.0.0、端口 1-65535；
# 已有数据（重启/crash 恢复）自动 resume，seed 与集群成员表不一致会 WARN（不覆盖）；
# 运行期扩缩容走 --join / promote / remove-node
```

**方式二：bootstrap + join（动态扩容）**

```bash
defing --node-id 1 --bootstrap --http-addr 127.0.0.1:8384 --raft-addr 127.0.0.1:8385 --data-dir ./n1 --admin-password admin123 --allow-no-master-key --join-token demo --raft-token demo
defing --node-id 2 --join http://127.0.0.1:8384 --http-addr 127.0.0.1:8386 --raft-addr 127.0.0.1:8387 --data-dir ./n2 --admin-password admin123 --allow-no-master-key --join-token demo --raft-token demo
defing --node-id 3 --join http://127.0.0.1:8384 --http-addr 127.0.0.1:8388 --raft-addr 127.0.0.1:8389 --data-dir ./n3 --admin-password admin123 --allow-no-master-key --join-token demo --raft-token demo
# 提升为 voter：
# POST /api/v1/cluster/promote {"node_id": 2} / {"node_id": 3}（需管理员 Bearer）
# 重启自动恢复：同 data-dir 直接启动（无需 --bootstrap/--join）
```
> 安全（F3）：集群模式强制要求 `--join-token`（join 端点鉴权）与 `--raft-token`（raft RPC 鉴权），
> 集群内所有节点须传相同值；生产环境请使用强随机值。

### SDK（TS / Go / Python）

```ts
import { ConfigClient } from './sdk/ts/src/index.ts';
const c = new ConfigClient([{ grpc: '127.0.0.1:8383', http: 'http://127.0.0.1:8384' }], {
  token: '<项目访问令牌>',   // 数据面鉴权：每项目独立令牌（Admin UI 项目页创建）
});
const snap = await c.get('my-app', 'dev');          // 读活动版本（gRPC 数据面）
c.watch('my-app', 'dev', (e) => console.log(e));    // 订阅发布事件（gRPC 流，断线 after_version 续传）
await c.listMembers();                              // 集群成员（端点池刷新）
```
Go：`sdk/go`（`configclient.NewGrpc(addr, token)` / `configclient.New(endpoints, token)` HTTP 降级）；
Python：`sdk/python`（`ConfigClient([{'grpc': ..., 'http': ...}], token=...)`）。
数据面鉴权：每项目访问令牌（`Authorization: Bearer <token>`，gRPC metadata 同构）；`--dev-single` 自动生成全局开发 token 打印。
端点带 `grpc` 地址时优先走 gRPC 数据面（:8383），纯字符串端点自动降级 HTTP/SSE；
gRPC 契约测试：`bash scripts/sdk-grpc-contract-test.sh`（依赖：npm install、pip install grpcio、go mod tidy）。

### 构建脚本取值（curl，无需 SDK）

编译/构建脚本可预先拉取指定分支的配置（纯 HTTP，输出 yaml/json/toml/env 任意格式，带项目访问令牌鉴权）：
**项目访问令牌即机器凭据，持有者可读取该项目的全部配置明文（含 secret 解密值）** —— 请妥善保管令牌。

```bash
# 拉取 my-app 项目 dev 分支的 YAML 配置（Bearer 鉴权；secret 解密返回）
curl -s "http://<host>:8384/v1/projects/my-app/branches/dev/config?format=yaml" \
  -H "Authorization: Bearer <项目访问令牌>"

# 或查询参数鉴权（URL 会含令牌，注意保管——URL 可能进入代理/日志）：
curl -s "http://<host>:8384/v1/projects/my-app/branches/dev/config?format=json&token=<项目访问令牌>"

# 输出 .env 文件（可直接重定向保存）：KEY=VALUE，键大写，无分组前缀
curl -s "http://<host>:8384/v1/projects/my-app/branches/dev/config?format=env" \
  -H "Authorization: Bearer <项目访问令牌>" > .env

# 指定版本：&version=<n>；分支名按需替换（dev/test/prod/自定义）
```

项目访问令牌在 Admin UI 项目页「访问令牌」Tab 创建（仅超级管理员，明文仅创建时展示一次）；
该 Tab 同时展示当前项目的可复制 curl 命令。
> 说明：渲染端点（/config）对数据面 token 授权请求默认解密 secret；SDK 快照（/snapshot、gRPC）保持恒掩码（proto masked 语义），如需明文请走 /config。

## 核心能力

- **集群**：Raft 强一致、静态成员表建群（--bootstrap-peers，全员 voter 无需 promote）、
  join/promote 动态扩容、leader 击杀容错、节点重启自动恢复
- **配置模型**：项目→分支→分组→item；结构强一致（仅值按分支）
- **发布闭环**：草稿 → 版本（不可变）→ 发布 → 通知；回滚；共享配置项（扁平库，含描述字段）与级联——结构页将配置项标记为「引用共享」（声明类型约束），各分支在草稿页按下拉选择引用的共享项（值由共享库物化、只读；共享项更新发布自动级联到绑定它的分支）
- **安全**：secret 项 AES-256-GCM 信封加密（主密钥 env/文件）、多会话并存（每会话独立管理 + 草稿乐观锁防并发编辑冲突）、审计、CSP、
  join/raft 集群令牌（--join-token/--raft-token 集群模式强制）、数据面每项目访问令牌（Admin UI/API 管理，SHA-256 落盘）、
  三级管理员（**超级管理员** / **全局管理员（GA）** / 项目管理员）：GA 由超级管理员创建/删除/改密，可管理**全部项目配置与全部共享配置**，
  但不能管理账号、集群写与访问令牌；三角色均可自助改密
- **多格式**：YAML / TOML / JSON 渲染
- **可观测**：/healthz、/readyz、/metrics（Prometheus）、审计 API
- **Admin UI**：内嵌 /admin（项目/配置/watch）

## 构建与测试

```bash
cd server
source ../scripts/build-env.sh   # CARGO_HOME + CXXFLAGS（本机 /home 只读环境）
cargo build --workspace
cargo test --workspace           # 172 测试（core/storage/raft/crypto/render/jobs/watch/api…）
# 端到端：
bash ../scripts/dev-single-demo.sh   # 单节点全流程（含 watch）
bash ../scripts/cluster-demo.sh      # 3 进程集群 kill 容错
bash ../scripts/chaos-test.sh        # leader 击杀/重启追赶混沌
bash ../scripts/sdk-contract-test.sh # 三语言 SDK 契约
bash ../scripts/check-contracts.sh   # proto/openapi/schema lint
```

## 文档

- 需求演进：[dev_docs/proposl.md](dev_docs/proposl.md) → [dev_docs/proposal-v4.md](dev_docs/proposal-v4.md)
- 可行性分析：[dev_docs/feasibility-report.md](dev_docs/feasibility-report.md)
- 详细设计：[dev_docs/design-v3.md](dev_docs/design-v3.md)、[dev_docs/design-modules/](dev_docs/design-modules/)（15 份模块规格）
- 进度记录：[dev_docs/progress.md](dev_docs/progress.md)
- 生态集成调研：[dev_docs/research-ecosystem-integration.md](dev_docs/research-ecosystem-integration.md)（综合结论 + 路线图）
  - K8s/K3s：[dev_docs/research-k8s-k3s-integration.md](dev_docs/research-k8s-k3s-integration.md)
  - Spring Cloud：[dev_docs/research-spring-cloud-integration.md](dev_docs/research-spring-cloud-integration.md)
  - 竞品对标：[dev_docs/research-competitor-benchmark.md](dev_docs/research-competitor-benchmark.md)

## 许可

Apache-2.0
