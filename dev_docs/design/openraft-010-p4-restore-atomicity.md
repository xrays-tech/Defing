# P4 设计：快照安装的崩溃原子性（restore-in-progress）

状态：APPROVE WITH CHANGES（gate attempt 2）。基线以 `P4base` 为准：358.2MB / episode 3.611s（末次 2.474s）/ net 1.00x、raw 1.31x。
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

1. **单测（必然覆盖）**：模拟「chunk 2 之后 abort」→ 走 dsh-cli 侧的 `recover_interrupted_restore()`（fallible）→ 重放 → 状态 == payload、
   `last_applied`/registry 一致、标记已清。
2. **单测**：panic/abort 在 4（last_applied 已写）与 5 之间 → boot 重放后一致（幂等）。
3. **单测**：BEGIN 标记存在但 payload 文件被删 → 启动返回明确错误（不静默继续）。
4. **单测**：`DSH_RAFT_RESTORE_MARKER=0` 只关闭「写标记」；**存在 marker 时仍必须重放**（忽略需显式 `DSH_RAFT_RESTORE_FORCE=1` 并大声 WARN）——见 §7 A2。
5. **e2e（复用 `voter_catches_up_via_http_snapshot_after_being_down`）**：正常路径 marker 不残留。
6. **storm 回归**：以 `P4base` 为回归线（episode ≤ 30s 预算；net ≤ 1.2x、raw ≤ 1.5x）。

## 6. 风险与未决

- 重放耗时计入启动时间：最坏一次 restore（400MB ≈ 2s），运维手册需说明「异常重启可能慢 2s」。
- 双写 payload（输入 + canonical）在 P4 msgpack 方案里会合并成流式写入，届时标记里的 payload 指向需同步调整。
- 未决（gate 决策）：标记放 raft-meta 还是独立小文件（`{data_dir}/RESTORE_BEGIN.json`）。
  倾向 raft-meta（与 registry 同库同事务语义）；独立文件的好处是 store 之外的装配也能看到。

## 7. 修订（P4 design gate attempt 1 之后，必须按此实现）

复核给了 REJECT，两条 blocker + 若干 major，本节覆盖它们：

- **A1（blocker）boot 重放与启动清扫冲突**：`StateMachineStore::new_with_rotation` 会先跑
  `sweep_snapshot_payloads`，而 `RESTORE_BEGIN` 指向的 payload 在 registry 提交前是 orphan →
  被删 → §4.2 fail-fast 反而砖化节点。**实现约束**：
  1. `sweep_snapshot_payloads` 增加参数「保留路径集合」，把 `RESTORE_BEGIN.payload`
     （canonicalize 后）并入保留集；
  2. 恢复流程顺序固定为「读 marker → 校验 payload → 重放 → 写 registry → 清 marker → 再清扫」；
  3. 验收必须**开着 sweep** 跑：崩溃在 3-4 之间 → 重启后 replay 成功且 payload 未被删。
- **A2（blocker）kill-switch 不得掩盖 marker**：`DSH_RAFT_RESTORE_MARKER=0` 只关闭「写 marker」；
  若启动时**存在** marker，必须照样重放（或显式 `DSH_RAFT_RESTORE_FORCE=1` 才允许忽略并大声 WARN）。
  否则会以混合状态启动。
- **A3（major）非崩溃型 restore 错误**：restore 返回 Err（非进程死亡）时，节点必须 fail-stop
  （poison 状态 + 拒绝服务写/读），不能带着混合状态继续；测试：注入 restore 中段 IO 错误。
- **A4（major）不变量前置条件**：marker 必须在「payload 完整且已 fsync（含目录 fsync）」之后写，
  且写 marker 前不得修改状态机。P3j 的 copy 路径已满足；P4b 若改为直写 canonical，必须由写方
  fsync 完成后交棒（见 §6 未决与 msgpack 文档 §5）。
- **A5（major）与 P4b 直写 canonical 的交互**：marker.payload 必须指向「已 fsync 的最终文件」，
  且 registry 与 marker 指向同一路径；删除任一方都要先清 marker。
- **A6（major）基线更新**：以 P3m 为准 —— 384.2MB、episode 3.739s（attempts=2，末次 2.53s）、
  窗口净增量 RSS 0.62x（1024 换算 0.64x）、raw VmHWM 1.24x。本文 §3 里 269MB/1.78s/1.37x 等
  旧数字作废。
- minors 采纳：验收 #1 要写明「子进程 SIGKILL 注入点」；#5 追加「marker 键不存在 + registry/尺寸
  校验」；重放走流式 reader（P4b 之后）以约束 boot 内存；marker 路径与 sweep 比较都用 canonicalize。
- 集成点定案：`dsh-cli` 在 `has_persisted_state` 判定之前调用 **fallible 的**
  `recover_interrupted_restore()`（`new_with_rotation` 保持 infallible，不动测试面）。

## 8. 实现落地（P4-A，2026-10-29，提交 0800f8f）

已按 §4/§7 实现（worktree `migrate/openraft-010`）。实现细节与偏差：

- **marker**：raft-meta 键 `restore_begin`（JSON `{meta, payload}`）。`install_snapshot` 在
  `copy_payload_file`（内含文件 + 目录 fsync）之后、`restore_all` 之前**单事务**写入；
  registry 提交 + 旧 payload 清理之后删除（删除失败只 warn —— boot 重放同一 payload 幂等）。
- **sweep keep-set**：`sweep_snapshot_payloads` 同时保留 registry `file` 与 marker.payload
  （canonicalize 后比较）；坏 marker → `Err` 且不删任何文件。
- **重放入口**：free fn `dsh_raft::recover_interrupted_restore(&sm, &db, data_dir)`。与 §4.2 的
  偏差：签名多一个 `&sm` —— 重放必须写状态机，而 `StateMachineStore` 实例在 dsh-cli 侧，且
  `new_with_rotation` 必须保持 infallible。顺序与 §7 A1 一致：清扫（ctor）→ 重放 → 再清扫。
  失败（marker 损坏 / payload 缺失或不可解析 / 重放写失败）→ `Err`，dsh-cli 拒绝启动。
- **A2 kill-switch**：`DSH_RAFT_RESTORE_MARKER=0` 只关「写 marker」（构造时读一次 + WARN）；
  已存在的 marker 启动时仍重放；忽略必须显式 `DSH_RAFT_RESTORE_FORCE=1`（ERROR 日志 + 清 marker +
  不重放）。
- **A3 fail-stop 的 enforcement 点**：
  1. `StateMachineStore::apply` —— 先 drain 整条流并逐条 `responder.send(Err)`，再返回 Err
     （不挂客户端）；
  2. `SnapshotBuilder::build_snapshot` / `get_current_snapshot` —— 返回 Err；
  3. dsh-api —— `poison_gate` 中间件（auth 之前、count_http 之内）对除 `/healthz`、`/metrics`、
     `/admin*` 外一律 503 `ERR_RESTORE_POISONED`；`readyz` 503；`linearized_read`（gRPC 数据面入口）
     与 `ApiState::write` 直接拒绝。
     解除：一次完整成功的 install（整体替换状态机）；否则重启走重放。
- **验收对照（§5）**：#1/#2 由 `boot_replay_after_interrupted_restore_with_sweep_on` /
  `recover_after_registry_commit_is_idempotent` 覆盖（单进程模拟重启 + 开着 sweep）；#3
  `recover_refuses_missing_payload`；#4 `kill_switch_disables_marker_write` +
  `recover_force_ignores_marker_without_replay`；#5 `install_snapshot_clears_restore_marker`
  （e2e 走同一 install 路径，未单独加键断言）；#6 storm 回归待 P4b/P4c 之后重跑。
- **残留**：(a) 没有子进程 SIGKILL e2e —— 用「崩溃现场（marker + 混合状态）+ ctor 清扫 + recover」
  等价模拟；(b) boot 重放仍把 payload 整份读成 `Vec`（P4b 的 `restore_stream` 改流式后同步调整）；
  (c) `restore_all` 分块语义未变：运行期错误由 poison 拦、崩溃由 marker 重放补；
  (d) gRPC 只有读接口（写走 HTTP），故只接 `linearized_read`。
- 顺带修 `voter_catches_up_via_http_snapshot_after_being_down` 的读日志竞态：SM 里 value 可见时
  core 尚未推进 last_applied / 触发 purge，先 `poll_metrics`（last_applied ≥ snapshot_index 且
  purged ≥ snapshot_index）再断言前缀为空。
