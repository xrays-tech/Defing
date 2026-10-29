# P4 设计：快照安装的崩溃原子性（restore-in-progress）

状态：草案（待 gate）。前置：P3 已闭合 install 预算（269MB/1.78s、RSS 1.37x），
但 `restore_all` 是**分块提交**的，崩溃点上是部分恢复。本文给出可施工方案与验收口径。

## 1. 问题（复核 major，P3 gate 明确要求 P4 处理）

`StateMachineStore::install_snapshot` 当前顺序：

1. `read_payload_pairs`（流式解析 payload 文件）；
2. `copy_payload_file`（把 payload 落成 canonical 文件，进程内唯一命名）；
3. `restore_all`（`Store::write_batch`，按 64MiB 分块，**每块一个 redb 事务**）；
4. `write_last_applied` / `write_membership`；
5. `persist_snapshot_registry`（registry 指向新文件）；
6. 删除旧 canonical 文件。

崩溃窗口与后果：

| 崩溃点 | 状态机 | last_applied | registry | 后果 |
|---|---|---|---|---|
| 2 之后 3 之前 | 旧 | 旧 | 旧 | 安全（下次重来） |
| 3 中间 | **新旧混合** | 旧 | 旧 | 重启后 openraft 从旧 last_applied 继续 append/apply，覆盖在混合状态上 → **静默分叉** |
| 3 之后 4 之前 | 新 | 旧 | 旧 | 同上（apply 重放覆盖已恢复的值） |
| 4 之后 5 之前 | 新 | 新 | 旧（旧快照元数据） | boot 时 snapshot meta 与 last_applied 不一致 |

旧实现（逐 key put/delete）有同样的窗口，只是更慢、窗口更大；本次改造把窗口缩到毫秒级，
但**没有改变语义**，不能算关闭。

## 2. 约束

- 不能把整份 restore 塞进单个 redb 事务：400MB payload 的脏页会重新推高 RSS（P3 刚把 2.05x 降到 1.37x），
  且大事务的 WAL/内存行为不可控。
- 不能要求快照文件在重启后依赖 leader 重发：leader 可能已经 purge，重发要等新的 snapshot。
- 不允许 0.9/0.10 混跑；但**同一版本内的升级必须容忍旧的 payload 文件**（JSON 格式，见 P4 msgpack 方案）。
- 安装期不能对外提供半状态读（当前靠「install 在 SM worker 内完成」隐式保证）。

## 3. 方案对比

| 方案 | 做法 | 优点 | 缺点 |
|---|---|---|---|
| A. 单事务 | 整个 restore 一个 txn | 原子 | RSS/WAL 风险，400MB 不可接受 |
| B. 影子库 + 指针切换 | 恢复到 `<data>/state.restore.redb`，fsync 后在 raft-meta 单事务切换「当前 state 文件」指针 | 真原子、内存可控 | 需要 StateMachine 支持双库生命周期；启动期要处理两库并存；磁盘 2x |
| C. 标记 + 重启重放（推荐） | restore 前写 `RESTORE_BEGIN{meta, payload_path}`（fsync）；成功后写 `RESTORE_END`（或直接清标记）；boot 若见 BEGIN → 用 payload 文件**重新执行 restore**（幂等全量替换）再继续 | 无 2x 磁盘、无大事务；实现最小；payload 已先落盘（步骤 2） | 恢复期多一次 restore（可接受：403MB/1.8s）；需要 boot 路径接入 |

选 **C**。理由：payload 文件在 restore 之前就已落盘且命名唯一（步骤 2），这使「重放 restore」安全且
不需要外部依赖；恢复成本有界（一次 restore 的耗时），并且可以直接复用同一段代码路径与测试。

## 4. 详细设计（C）

### 4.1 标记格式与写入点

新增 raft-meta 键 `META_RESTORE_BEGIN`，值 = `serde_json/RESTORE_BEGIN{ meta: SnapshotMeta, payload: PathBuf }`。
写入点：步骤 2（copy 完成）之后、步骤 3（restore）之前，**单事务 + fsync**。

清理点：步骤 5（registry 提交）之后，删除该键（单事务）。顺序保证：
- registry 已指向新 payload 且 last_applied/membership 已写 → 标记删除是纯幂等收尾；
- 若清理失败/崩溃，boot 见到 BEGIN 会再 restore 一次同一 payload（幂等），再清标记。

### 4.2 boot 处理

`StateMachineStore::new`（或 `dsh-cli` 装配处，二选一，倾向 store 内聚）：
若存在 `META_RESTORE_BEGIN`：

1. 校验 payload 文件存在且可解析（`read_payload_pairs` 头部校验）；
2. 执行 `restore_all` + `write_last_applied` + `write_membership` + `persist_snapshot_registry`；
3. 清标记；
4. 记录一条 `WARN restore replay on boot: meta=... elapsed=...`。

失败处理（payload 缺失/损坏）：**fail fast** —— 拒绝启动并给出明确错误（「快照恢复中断且 payload 不可用，
需要从备份恢复或清卷重建」）。理由：静默降级到「无快照」会丢数据；自动向 leader 请求快照需要网络与成员表，
超出 boot 期职责。

### 4.3 开关与回滚

- 环境变量 `DSH_RAFT_RESTORE_MARKER=0` 关闭（默认开启），用于发布期兜底；
- 关闭时行为回到 P3（分块 restore，无标记），日志明确 WARN。

## 5. 验收（P4 gate 用）

1. **单测（必然覆盖）**：模拟「chunk 2 之后 abort」→ 重开 store → 自动重放 → 状态 == payload、
   `last_applied`/registry 一致、标记已清。
2. **单测**：panic/abort 在 4（last_applied 已写）与 5 之间 → boot 重放后一致（幂等）。
3. **单测**：BEGIN 标记存在但 payload 文件被删 → 启动返回明确错误（不静默继续）。
4. **单测**：`DSH_RAFT_RESTORE_MARKER=0` 时行为与 P3 相同（不写标记、boot 不重放）。
5. **e2e（复用 `voter_catches_up_via_http_snapshot_after_being_down`）**：正常路径 marker 不残留。
6. **storm 回归**：install 预算与 RSS 不得回退（269MB ≤ 1.8s、RSS ≤ 1.5x 作为回归线）。

## 6. 风险与未决

- 重放耗时计入启动时间：最坏一次 restore（400MB ≈ 2s），运维手册需说明「异常重启可能慢 2s」。
- 双写 payload（输入 + canonical）在 P4 msgpack 方案里会合并成流式写入，届时标记里的 payload 指向需同步调整。
- 未决（gate 决策）：标记放 raft-meta 还是独立小文件（`{data_dir}/RESTORE_BEGIN.json`）。
  倾向 raft-meta（与 registry 同库同事务语义）；独立文件的好处是 store 之外的装配也能看到。
