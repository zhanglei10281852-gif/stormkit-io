# Webhook 投递认领与部署幂等性 - 产品需求文档

## Overview
- **Summary**: 为自动部署 Webhook 入口（GitHub / GitLab / Bitbucket → `/app/webhooks/{provider}`）建立基于供应商投递身份（delivery identity）的持久化认领状态，使重复投递、并发请求、进程崩溃、数据库瞬时故障与部署入队失败都只产生一个可重试的部署结果；同一提交匹配多个应用/环境时各自独立留痕；不携带投递身份的旧调用继续走现有提交去重。
- **Purpose**: 现状 `TriggerDeploy` 仅靠部署前的 `commitHasBeenBuilt`（按 commit_id 去重，且多数 push 事件没有 commit sha）与进程内执行顺序保证幂等：供应商重投同一 delivery、两个请求同时到达会在同一应用环境重复启动部署；进程在执行中途重启会丢失事件；Deploy 阶段失败后重试会插入第二条 deployment 记录并二次入队。需要数据库持久化的认领与结果状态来闭合这些窗口。
- **Target Users**: 配置了自动部署的 Stormkit 实例运营者与应用开发者（间接受益：GitHub/GitLab/Bitbucket 的投递方需要稳定 HTTP 语义）。

## Goals
- 带投递身份的事件：重复投递与并发请求对每个匹配的（应用, 环境）只产生一次外部部署（deployment 行 + 构建队列任务）。
- 认领状态可持久化、可追踪：事件原文、规范化字段、每个环境的处理状态/部署关联/错误/尝试次数落库，进程重启不丢。
- 领取之后崩溃、数据库暂时失败、部署启动失败均可通过供应商重新投递或后台重试恢复，且恢复不产生第二次外部部署。
- 供应商在「已处理 / 处理中 / 可安全重试」三种情形下都收到稳定、可预期的 HTTP 响应。
- 同一提交合法对应多个应用或环境时，每个（应用, 环境）保留独立结果并关联回原始事件，不被错误合并。
- 不携带投递身份的旧 GitLab / 兼容调用保持现有提交去重行为；签名校验、非匹配事件、部署查询结果不回归。

## Non-Goals
- 不改动手动部署入口（`handler_deploy_start`）、部署回调、发布（publish）、部署查询接口的行为与响应结构。
- 不改变供应商 payload 解析规则、白名单事件集合、分支/路径过滤规则本身。
- 不为认领记录做数据保留/清理任务（不新增清理 job；记录量级与部署量级相当，后续可单独迭代）。
- 不引入分布式锁服务或新中间件；认领的唯一事实来源是 PostgreSQL，重试调度沿用现有 gocron leader 任务机制。
- 不改变 GitLab/Bitbucket 每应用 URL 密钥的校验方式与 GitHub 的 HMAC 校验方式。

## Background & Context
- 入口：[handler_inbound_webhooks.go](file:///f:/swe/098803/project-01/src/ce/api/app/apphandlers/handler_inbound_webhooks.go) 的 `handlerInboundWebhooks → processMessage → TriggerDeploy`；三家解析分别在 `handler_inbound_webhooks_{github,gitlab,bitbucket}.go`。
- 现有去重：`commitHasBeenBuilt → deploy.Store.IsDeploymentAlreadyBuilt`（SQL：`commit_id = $1 AND ($2 = 0 OR app_id = $2)`），命中返回 `208 Already Reported`；该检查主要对携带 commit sha 的 GitLab MR 事件有效（GitHub/GitLab push 一般不填 CommitSha）。
- 外部部署副作用由 `deployservice.DefaultDeployer.Deploy` 产生：先 `InsertDeployment` 落 deployment 行，再 `tasks.Enqueue(DeploymentStart, TaskID="deployment-<id>")` 入 asynq 队列。两步之间失败/崩溃会留下「有 deployment 无任务」的窗口；盲目重试会再插一条 deployment。
- asynq v0.25.1 对相同 TaskID 的在队/在执行任务返回 `asynq.ErrTaskIDConflict`（`client.go:228`），因此同一 deployment 的重复入队可被识别为「已派发」。
- 投递身份头（已核实官方文档）：
  - GitHub：`X-GitHub-Delivery`，GUID 在同一 delivery 的所有重投中保持不变（GitHub 官方自动重投指南明确按 GUID 归并）。
  - GitLab：`X-Gitlab-Event-UUID`，非递归 webhook 的事件唯一 ID；旧版本/兼容客户端不发送。
  - Bitbucket Cloud：`X-Request-UUID`，标识同一次投递（其自动重试以 `X-Attempt-Number` 递增，Request UUID 不变）。
- 后台：`src/ce/workerserver` 使用 gocron + leader 选举注册主任务（如 `TimedOutDeployments` 每分钟），EE worker 复用 CE `jobs.Server()`；在其中注册恢复任务即可同时覆盖 CE/EE。
- 测试：真实 Postgres + 迁移文件驱动（`databasetest.InitTx`，每个用例一个事务），webhook 测试通过 `deployservice.MockDeployer` 避免依赖 Redis；新增迁移文件 `0029_*.up.sql` 会被测试自动应用。
- 迁移写法：幂等 SQL（`IF NOT EXISTS`/`IF EXISTS`）+ 注释说明；`structure.sql` 是过期快照（缺少 is_priority 等近期列），近期迁移均不维护它。
- Windows 本机无法直接编译 Linux 专有依赖（nixstore、aliyun CLI），验证以 `GOOS=linux go vet`/`go test -c` 编译检查 + Linux golang 容器内跑真实测试为准。

## Functional Requirements

- **FR-1 投递身份提取**：三家 provider 处理器从请求头提取投递身份（GitHub `X-GitHub-Delivery`、GitLab `X-Gitlab-Event-UUID`、Bitbucket `X-Request-UUID`），并随解析结果携带 provider 名；身份为空时整段逻辑走旧路径。
- **FR-2 原始事件留存**：对带身份且通过签名校验、解析为可部署事件（非 sample/fork/非匹配事件的短路场景）的请求，将原始 payload 与规范化字段（repo、checkoutRepo、branch、message、eventType、commitSha、PR 号、isFork、changesComplete、changedFiles）以「首次收到为准」 upsert 到事件父表；父表唯一键为 `(provider, provider_delivery_id)`。
- **FR-3 每环境结果行**：父事件下，每个匹配到的（应用, 环境）对应一行结果，唯一键 `(delivery_id, app_id, env_id)`，记录 status（`processing`/`succeeded`/`retryable`）、deployment 关联、attempts、last_error、claimed_at（租约）、next_retry_at、finished_at 与时间戳。
- **FR-4 原子认领**：认领通过 `INSERT ... ON CONFLICT DO NOTHING` + 条件 `UPDATE ... RETURNING` 完成；并发的后来者读到新鲜租约下的 `processing` 时不得创建 deployment 或入队。
- **FR-5 部署与认领绑定**：自动部署创建的 deployment 在插入时携带其结果行 ID；数据库以部分唯一索引保证「一个结果行至多一条 deployment」。Deploy 拆分为「创建 deployment」与「派发到队列」两步，恢复时若 deployment 已存在则只重新派发同一 deployment；asynq TaskID 冲突视为已派发成功。
- **FR-6 失败标记**：Deploy 创建/派发阶段返回错误时，结果行置为 `retryable`、记录 last_error 与 next_retry_at（指数退避），HTTP 返回可重试状态；已取得的 deployment ID 必须持久化到结果行（含崩溃窗口下经 `deployments.delivery_result_id` 反查）。
- **FR-7 后台恢复**：leader 专属任务每分钟扫描「租约过期仍 processing」与「next_retry_at 到期且 attempts 未达上限」的结果行，`SKIP LOCKED` 取行后复用与 HTTP 相同的处理逻辑：有 deployment → 仅派发；无 deployment → 用父事件存储字段重建输入并重新选取候选后创建+派发。
- **FR-8 重新投递恢复**：供应商重新投递（相同身份）在任何状态下都应被接受：succeeded → 不重复部署；retryable/过期 processing → 同步认领并继续；新鲜 processing → 返回处理中响应。重新投递的恢复不受自动重试次数上限约束。
- **FR-9 响应语义**：
  - 全部匹配环境已派发或此前已成功：`200 OK`（含本次新建数为 0 但有成功结果的重投）；
  - 存在被其他在飞请求持有的认领：`202 Accepted`，且本次不产生任何外部部署；
  - 签名通过但无需部署（非匹配事件、tag、fork、sample、无候选等，与现状一致）：`204 No Content`；
  - 旧路径命中提交去重：`208 Already Reported`（现状不变）；delivery 路径仍保留跨事件提交守卫，命中时同样 208；
  - 认领持久化、deployment 创建/派发等可恢复故障：`503 Service Unavailable`；
  - 应用查询失败：维持 `500`；签名失败：维持 `403`。
- **FR-10 多环境扇出**：一次 delivery 匹配多个应用/环境时，各自独立认领、独立部署、独立留结果；per-app 密钥（GitLab/Bitbucket）仍限定只部署该 app；默认分支不匹配时 `ShouldPublish=false` 等现有候选处理逻辑不变。
- **FR-11 旧路径保留**：投递身份缺失时，`TriggerDeploy` 走与今天完全一致的流程（`commitHasBeenBuilt` → 候选循环 → `Deploy`），不写任何认领表，响应码与现状一致。
- **FR-12 不回归约束**：签名校验顺序（拒签前不读/少读 body）、白名单与非匹配事件 204、部署列表/详情查询、手动部署与优先级重排队等既有能力不因新列/新表回归；新增列必须 nullable，既有 INSERT 使用显式列清单且不受影响。
- **FR-13 跨事件提交守卫**：delivery 路径在创建认领前保留 `IsDeploymentAlreadyBuilt` 守卫，使 GitLab 因评论等触发的「不同事件 UUID、同一 commit」冗余推送仍被 208 拦截；守卫不区分环境的现状语义保持不变（同一请求内的多环境扇出发生在守卫之后，不受影响）。

## Non-Functional Requirements
- **NFR-1 持久化与重启安全**：认领状态只存 PostgreSQL；不依赖进程内存与 Redis 即可在重启后判断事件是否已被认领/部署。
- **NFR-2 并发安全**：同一（delivery, app, env）的并发请求至多一个进入部署创建路径，由数据库唯一约束 + 条件更新保证，不使用进程内锁。
- **NFR-3 可观测性**：结果行保留 last_error、attempts、claimed_at、next_retry_at、finished_at，可通过 SQL 直接追踪每个 delivery 在每个环境的终态。
- **NFR-4 测试隔离**：新测试沿用 txdb 事务回滚与 `MockDeployer`，不要求 Redis；MockDeployer 扩展后既有测试无需批量改写。
- **NFR-5 代码约定**：遵循 AGENTS.md（store/statements/model 分层、handler 命名与同级测试、逻辑块间空行、导出函数注释、Conventional Commits）。
- **NFR-6 性能**：认领路径每次请求仅增加常数次主键/唯一索引查询；恢复任务单次批量上限（50 行）并使用 `SKIP LOCKED`，避免长事务与重复执行。

## Constraints
- **Technical**:
  - PostgreSQL（`pq`、`database.IsDuplicate`、`ON CONFLICT`、部分唯一索引、`FOR UPDATE SKIP LOCKED`）。
  - 新迁移编号 `0029`，日期 2026-10-08，文件内嵌 `//go:embed *.up.sql` 自动生效。
  - asynq TaskID 与 `ErrTaskIDConflict` 语义；不新增队列。
  - Go 1.26；Windows 开发机用交叉编译检查 + Linux 容器执行测试。
- **Business**: 不改变供应商接入方式与 URL 形态；旧客户端零感知。
- **Dependencies**: `deploy`、`deployservice`、`app`、`lib/database`、`lib/tasks`、`ce/workerserver` 既有能力。

## Assumptions
- 供应商身份头缺失即视为「旧/兼容调用」，对三家一视同仁（现有测试均未发送身份头，将继续走旧路径）。
- 租约时长取 2 分钟（HTTP 路径仅做 DB 插入与入队，秒级完成）；后台自动重试上限 10 次，退避 `min(1m * 2^attempts, 1h)`；供应商重新投递可突破上限再次尝试。
- 处理中重复请求返回 `202 Accepted`（2xx 保证 GitHub/GitLab/Bitbucket 均不会因非 2xx 触发额外重投；GitHub 仅接受 2xx）。
- 可恢复故障返回 `503`（Bitbucket 对 5xx 自动重试两次；GitHub/GitLab 可手动/自动重投，且后台任务不依赖重投也会自愈）。
- 父事件表在提交守卫命中时也留底（可追踪每次已验签的可部署事件）；非匹配事件、fork、sample 短路不写表，避免噪声。
- 恢复时对「deployment 已存在」的派发内容以该 deployment 的 `config_snapshot` 为准重建，避免环境配置在事件之后发生变化改变本次部署内容。

## Acceptance Criteria

### AC-1: 同一 delivery 重复投递只部署一次
- **Type**: `rule`
- **Given**: 一个携带合法投递身份头、且匹配某应用某环境自动部署的已验签事件
- **When**: 同一身份头与同一 payload 连续投递两次
- **Then**: 第一次返回 200 且调用一次 Deployer.Deploy；第二次返回 200 且不再调用 Deploy、不新增 deployment、不再入队
- **Pass Condition**: `mockDeployer.AssertNumberOfCalls("Deploy", 1)`，结果表恰有 1 行 succeeded 并关联 1 条 deployment
- **Evidence**: apphandlers 测试输出（github/gitlab/bitbucket 各至少一例）与数据库断言

### AC-2: 并发请求得到处理中响应且不重复部署
- **Type**: `rule`
- **Given**: 同一（delivery, app, env）已存在一行 claimed_at 新鲜的 `processing` 结果
- **When**: 同一身份请求再次到达
- **Then**: 返回 202，不创建 deployment、不调用 Deploy/ Dispatch，已有 processing 行不被重置
- **Pass Condition**: HTTP 202 且 mock 零调用；处理者完成后重投转为 200
- **Evidence**: apphandlers 并发/租约测试

### AC-3: 每个匹配的应用与环境独立留结果并关联原始事件
- **Type**: `rule`
- **Given**: 一次 delivery 匹配同一仓库的多个应用/环境
- **When**: 事件处理完成
- **Then**: 父事件 1 行（含 provider、delivery id、原始 payload），每个（app, env）各 1 行结果并各自关联自己的 deployment；唯一约束拒绝重复 (delivery, app, env)
- **Pass Condition**: 结果行数 == 匹配候选数；尝试插入重复键报 23505；每行可经 delivery_id 追溯父事件
- **Evidence**: 多环境扇出测试 + webhookdelivery store 测试

### AC-4: 同一提交的多个合法环境不被合并
- **Type**: `rule`
- **Given**: 同一 commit 在一次 delivery 中匹配两个环境（或两个 app）
- **When**: 处理该 delivery
- **Then**: 两个环境分别调用 Deploy 各一次，产生两条 deployment 与两个 succeeded 结果，互不被 `IsDeploymentAlreadyBuilt` 或认领逻辑短路
- **Pass Condition**: Deploy 调用数 == 2，deployment 数 == 2，env_id 各不相同
- **Evidence**: 多环境测试

### AC-5: 领取后进程崩溃可经重投/后台恢复且无第二次外部部署
- **Type**: `rule`
- **Given**: 结果行为 `processing` 且 claimed_at 已超过租约；并且 deployment 行已存在（带 delivery_result_id）但队列任务缺失
- **When**: 供应商重新投递（或后台恢复任务扫描到该行）
- **Then**: 不插入新 deployment；仅以该 deployment 的 config_snapshot 重建并派发一次队列任务（TaskID 冲突视为成功）；结果置 succeeded
- **Pass Condition**: Deploy 零调用、Dispatch 至多一次（ErrTaskIDConflict 计为成功），deployment 总数不变，结果 succeeded
- **Evidence**: 恢复路径测试（HTTP 重投 + Recover 函数各一例）

### AC-6: deployment 创建前崩溃可恢复
- **Type**: `rule`
- **Given**: 过期 `processing` 结果行无 deployment 关联，且 deployments 表无该结果行的 deployment
- **When**: 后台恢复或重新投递处理该行
- **Then**: 从父事件字段重建输入并重新选取候选，创建 deployment（绑定结果行）并入队，结果置 succeeded，全程仅一次外部部署
- **Pass Condition**: Deploy 路径执行一次，新 deployment.delivery_result_id 指向该结果行
- **Evidence**: 恢复路径测试

### AC-7: 部署启动失败留下可重试结果并可恢复
- **Type**: `rule`
- **Given**: Deployer.Deploy 在首次投递时返回错误
- **When**: 首次请求返回后，相同 delivery 重新投递（或后台任务重试）
- **Then**: 首次响应 503，结果行为 retryable 并记录 last_error/attempts/next_retry_at；恢复调用成功后仅产生一次外部部署并置 succeeded
- **Pass Condition**: 首次 503；重试后 200；Deploy 总调用导致的 deployment 总数 == 1
- **Evidence**: 失败注入测试（MockDeployer 先失败后成功）

### AC-8: 数据库瞬时失败给可重试响应且不产生悬空成功
- **Type**: `rule`
- **Given**: 认领/结果落库或 deployment 插入发生临时数据库错误
- **When**: 请求处理中
- **Then**: 返回 503（应用查询错误仍为 500）；已落 processing 认领由租约回收；未落下任何认领时重投等价首次处理；无路径返回 2xx 却未持久化
- **Pass Condition**: 故障注入下状态码为 503 且重试成功后无重复 deployment
- **Evidence**: 存储层错误路径测试 + 代码审查证据

### AC-9: 后台恢复任务注册且行为正确
- **Type**: `rule`
- **Given**: worker 以 leader 身份运行
- **When**: 每分钟主任务执行
- **Then**: 扫描过期 processing 与到期 retryable（attempts < 10）结果行，批量 ≤50、SKIP LOCKED；成功置 succeeded，仍失败按退避更新；succeeded/新鲜 processing/未到期 retryable 不被触碰
- **When（续）**: attempts 已达 10 的 retryable 行不再被自动重试（供应商重投仍可恢复）
- **Pass Condition**: 注册于 master 任务列表；job 测试覆盖选取与忽略两类集合
- **Evidence**: workerserver job 测试 + 注册代码

### AC-10: 无投递身份的旧调用保持提交去重
- **Type**: `rule`
- **Given**: GitLab/兼容请求不携带投递身份头，且该 commit 在同 app 已有 deployment（现状 `Test_DoNotRebuildSameCommits` 场景）
- **When**: 请求到达
- **Then**: 返回 208，不调用 Deploy，不写事件/结果表
- **Pass Condition**: 既有用例（三家无身份头的全部测试）零修改通过；认领表无记录
- **Evidence**: 既有测试套件全绿

### AC-11: 签名校验与非匹配事件不回归
- **Type**: `rule`
- **Given**: 缺签名/错签名/未配置 secret/非白名单事件/tag/fork/sample/无候选等场景
- **When**: 请求到达
- **Then**: 响应与今天完全一致（403/204 等），且非可部署事件不落任何认领数据；错签名请求 body 不被提前读取的保证不变
- **Pass Condition**: 既有 github/gitlab/bitbucket 测试全部通过；新增针对「非匹配事件不写表」断言
- **Evidence**: 既有 + 新增测试

### AC-12: 迁移安全且部署查询不回归
- **Type**: `rule`
- **Given**: 0029 迁移在空库与已应用库上执行
- **When**: 迁移运行（含重复执行）
- **Then**: 两张新表、约束、索引与 deployments 新 nullable 列创建成功且可重复执行；既有部署列表/详情/手动部署测试全绿
- **Pass Condition**: 迁移在测试库自动应用成功；`go test` 相关包通过；手动部署产生的 deployment 新列为 NULL
- **Evidence**: 迁移执行输出 + deploy/deployhandlers 测试

### AC-13: provider 维度与身份头识别正确
- **Type**: `rule`
- **Given**: 三家各自的身份头
- **When**: 解析请求
- **Then**: provider 名正确参与唯一键（不同 provider 相同 GUID 不冲突）；头值被 trim、空串视为缺失；GitLab 缺头自动降级旧路径
- **Pass Condition**: 单元测试覆盖各头与空值/缺失；不同 provider 同 GUID 可各自部署
- **Evidence**: provider 解析测试

### AC-14: 代码与约定一致性
- **Type**: `rubric`
- **Dimension**: 代码质量与代码库一致性
- **Scale**: 1-5
- **Anchors**: 1 = 分层混乱、绕过既有 store/响应约定、缺测试或注释；3 = 功能正确但风格与周边代码有可见偏差；5 = store/model/statements 分层与 deployhooks/deploy 一致，错误处理与状态码遵循 shttp 约定，命名/空行/注释/测试组织完全贴合 AGENTS.md，新增逻辑被同级测试覆盖
- **Pass Threshold**: >= 4
- **Evidence**: 独立审查对照 AGENTS.md 与周边包

## Open Questions
- 无（实现参数取 Assumptions 中的默认值；如对租约时长、退避上限或 202/503 状态码选择有不同偏好，可在审批时指出）。
