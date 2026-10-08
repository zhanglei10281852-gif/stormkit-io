# Webhook 投递认领与部署幂等性 - 实施计划

## Task 1: 数据库迁移（事件表、结果表、deployments 关联列）
- **Status**: `completed`
- **Completion Evidence**:
  - `src/migrations/0029_2026-10-08.up.sql` 已创建；databasetest 初始化（迁移链 0001→0029）在 Linux 容器内成功，既有 TestInboundGithub 基线通过（TR-1.1 首次应用）。
  - 在 skitapi schema 下带 ON_ERROR_STOP 二次执行 0029 成功（NOTICE skipping，无错误），幂等性验证通过（TR-1.1）。
  - 部分唯一索引/唯一约束由 Task 2/3 测试触发 23505 验证（TR-1.2 随后续任务收口）。
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 新增 `src/migrations/0029_2026-10-08.up.sql`（幂等 SQL + 注释，沿用 0027/0028 风格）：
    - `webhook_deliveries`：`delivery_id BIGSERIAL PK`、`provider text not null`、`provider_delivery_id text not null`、`repo/checkout_repo/branch/message/event_type/commit_sha text`、`pull_request_number bigint`、`is_fork boolean`、`changes_complete boolean`、`changed_files text[]`、`payload jsonb`、`created_at/updated_at timestamp without time zone default now() UTC`；`UNIQUE(provider, provider_delivery_id)`。
    - `webhook_delivery_results`：`result_id BIGSERIAL PK`、`delivery_id bigint not null references webhook_deliveries(delivery_id) on delete cascade`、`app_id bigint not null`、`env_id bigint not null`、`env_name text`、`status text not null`、`deployment_id bigint null references deployments(deployment_id) on delete set null`、`attempts int not null default 0`、`last_error text`、`claimed_at/finished_at/next_retry_at timestamp without time zone`、`created_at/updated_at`；`UNIQUE(delivery_id, app_id, env_id)`；索引 `(status, next_retry_at)`。
    - `ALTER TABLE deployments ADD COLUMN IF NOT EXISTS delivery_result_id bigint null references webhook_delivery_results(result_id) on delete set null`；部分唯一索引 `CREATE UNIQUE INDEX IF NOT EXISTS ... ON deployments(delivery_result_id) WHERE delivery_result_id IS NOT NULL`。
  - 顺序：先结果表后父表（deployments FK 需要结果表存在），再 ALTER deployments。
- **Acceptance Criteria Addressed**: AC-3, AC-12
- **Test Requirements**:
  - `rule` TR-1.1: 迁移在测试库（databasetest 自动应用全部 *.up.sql）可执行且重复执行不报错；证据：任一相关包测试启动成功 + 手工在测试库二次执行 0029 成功。
  - `rule` TR-1.2: 两张表与唯一约束、部分唯一索引存在；证据：测试中违反唯一键得到 `database.IsDuplicate`（23505），且两条不同 result 的 deployment 插入正常。
- **Notes**: 不维护过期快照 structure.sql（与 0024 之后迁移做法一致）。

## Task 2: webhookdelivery 领域包（模型/语句/存储）
- **Status**: `completed`
- **Completion Evidence**:
  - 新增包 `src/ce/api/app/deploy/webhookdelivery/`：delivery_model.go（Delivery/Result + 状态常量）、delivery_statements.go（upsert/claim/reclaim/mark/due 全部 SQL，含 SKIP LOCKED 与租约条件）、delivery_store.go（7 个存储方法 + 统一 scan）。
  - delivery_store_test.go 7 个用例全部通过（容器内 go test）：首次写入获胜、provider 维度键、claim 新行/既有行/succeeded 行、reclaim 新鲜/过期/retryable/succeeded 守卫、markRetryable 保留 deployment 链接且不回退 succeeded、DueResults 五类集合选取。
  - gofmt 无差异。TR-2.1~2.5 全部满足。
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - 新包 `src/ce/api/app/deploy/webhookdelivery/`（参照 deployhooks 的 model/statements/store 分层）：
    - `delivery_model.go`：`Delivery`（父事件，含规范化字段 + `ChangedFiles []string` + `Payload json.RawMessage`）、`Result`（结果行：ID、DeliveryID、AppID、EnvID、EnvName、Status、DeploymentID、Attempts、LastError、ClaimedAt、NextRetryAt、FinishedAt）；状态常量 `StatusProcessing/StatusSucceeded/StatusRetryable`；`ProviderGitHub/GitLab/Bitbucket` 常量。
    - `delivery_statements.go`：全部 SQL（与 deployment_statements.go 风格一致）。
    - `delivery_store.go`：`Store{*database.Store}` + `NewStore()`；方法：
      - `UpsertDelivery(ctx, *Delivery) (types.ID, error)`：首次插入全部字段，冲突仅刷新 updated_at，RETURNING delivery_id；payload 用 jsonb 透传 []byte，changed_files 用 `pq.Array`。
      - `ResultByKey(ctx, deliveryID, appID, envID) (*Result,error)`（无行返回 nil,nil）。
      - `ClaimResult(ctx, deliveryID, appID, envID, envName) (*Result,error)`：`INSERT ... ON CONFLICT DO NOTHING` 后回读；新行 status=processing、claimed_at=now、attempts=0。
      - `ReclaimResult(ctx, resultID, leaseExpiry time.Time) (bool,error)`：条件 UPDATE（status=retryable 或 processing 且 claimed_at<租约），置 processing/claimed_at=now/attempts+1/清空 last_error、next_retryat；返回是否抢到。
      - `MarkSucceeded(ctx, resultID, deploymentID)`、`MarkRetryable(ctx, resultID, deploymentID, errMsg, nextRetryAt)`。
      - `DueResults(ctx, limit int, leaseExpiry time.Time, now time.Time, maxAttempts int) ([]*Result,error)`：processing 过租约 或 (retryable 且 next_retry_at<=now 且 attempts<max)，`FOR UPDATE SKIP LOCKED` LIMIT。
      - `DeliveryByID(ctx, id) (*Delivery,error)`（恢复时重建输入）。
    - `delivery_store_test.go`（外部测试包 `webhookdelivery_test`，databasetest + factory 风格）：upsert 幂等、claim 并发语义模拟（先 claim 再插返回既有 processing）、reclaim 新鲜/过期/retryable/succeeded 各分支、due 选取集合、状态流转。
- **Acceptance Criteria Addressed**: AC-2, AC-3, AC-8, AC-9, AC-13
- **Test Requirements**:
  - `rule` TR-2.1: 同 (provider,delivery id) 重复 Upsert 只保留首次 payload 与字段但更新 updated_at；证据：store 测试断言。
  - `rule` TR-2.2: ClaimResult 对已存在（processing/succeeded/retryable）行返回既有状态不覆盖；证据：测试四种状态。
  - `rule` TR-2.3: ReclaimResult 仅在 retryable 或 processing 过租约时成功并 attempts+1；succeeded 与新鲜 processing 返回 false；证据：测试。
  - `rule` TR-2.4: DueResults 只返回过租约 processing 与到期且 attempts<10 的 retryable，忽略 succeeded/未到期/达上限，且 SQL 含 SKIP LOCKED；证据：测试 + 语句检查。
  - `rule` TR-2.5: MarkSucceeded/MarkRetryable 正确写 status/deployment_id/last_error/next_retry_at/finished_at；证据：测试回读断言。

## Task 3: deploy 模型与存储扩展（认领关联 + 派发重建查询）
- **Status**: `completed`
- **Completion Evidence**:
  - Deployment.DeliveryResultID 字段、insertDeployment 第 17 列（0→NULL）、DeploymentForDispatch/DeploymentByDeliveryResult（含 config_snapshot 还原 BuildConfig）。
  - deployment_dispatch_store_test.go 4 用例通过：同 claim 二次插入触发 database.IsDuplicate、无认领部署为 NULL、派发字段还原、claim 反查。TR-3.1~3.3 满足。
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - `deployment_model.go`：Deployment 增加 `DeliveryResultID types.ID`（json:"-"，0 表示无关联，手动部署保持 0）。
  - `deployment_statements.go`：`insertDeployment` 增加列 `delivery_result_id`（$17，NULL 传 nil）。
  - `deployment_store.go`：`InsertDeployment` 传参增加 null 处理；新增：
    - `DeploymentByDeliveryResult(ctx, resultID types.ID) (*Deployment,error)`：无行 nil,nil。
    - `DeploymentForDispatch(ctx, id types.ID) (*Deployment,error)`：扫描 deployment_id, app_id, env_id, env_name, branch, checkout_repo, pull_request_number, auto_publish, is_priority, migrations_folder, config_snapshot；并用 `ConfigSnapshot{BuildConfig...}` 从 ConfigCopy 还原 `BuildConfig`（json.Unmarshal），供 deployservice 重建派发消息。
  - factory 与既有显式列 INSERT 无需改动（新列 nullable）。
  - 相关测试：`deployment_statements_test.go`/`deployment_model_test.go` 增补关联插入与重复键场景（经 store 层或 SQL 直接断言）。
- **Acceptance Criteria Addressed**: AC-5, AC-12
- **Test Requirements**:
  - `rule` TR-3.1: InsertDeployment 带 DeliveryResultID 后可回读；重复绑定同一 result 的第二条 deployment 触发 23505（`database.IsDuplicate` 为 true）；手动/工厂创建的 deployment 该列为 NULL；证据：deploy 包测试。
  - `rule` TR-3.2: DeploymentForDispatch/DeploymentByDeliveryResult 返回的 Deployment 可还原 BuildConfig（含 BuildCmd/Vars/WorkDir 等）与派发所需全部字段；证据：deploy 包测试。
  - `rule` TR-3.3: 既有部署列表/状态查询扫描列不变、测试通过；证据：deploy/deployhandlers 既有测试全绿。

## Task 4: deployservice 拆分创建/派发并扩展 Deployer 接口
- **Status**: `completed`
- **Completion Evidence**:
  - DefaultDeployer 拆为 createDeployment/dispatchDeployment；Deploy=两者；新增 Dispatch；ErrTaskIDConflict 归一为成功；cache/credential 逻辑自包含。
  - Deployer 接口增加 Dispatch，mocks.Deployer 手工补全；新增 Dispatch 冲突/错误两单测；deployservice 整包（需 Redis）通过。TR-4.1~4.3 满足。
- **Priority**: high
- **Depends On**: Task 3
- **Description**:
  - `deployer.go`：将 `DefaultDeployer.Deploy` 重构为：
    - `createDeployment(ctx, a, d) error`：现 42-126 行全部逻辑（额度、repo size、凭证、ConfigSnapshot、InsertDeployment）；插入时携带 `d.DeliveryResultID`。
    - `dispatchDeployment(ctx, a, d) error`：构造 DeploymentMessage + 入队（现 128-184 行）。
    - `Deploy = createDeployment + dispatchDeployment`（行为不变）。
    - 新增 `Dispatch(ctx, a, d) error`（供恢复路径调用；内部即 dispatchDeployment）。
    - 归一化：dispatchDeployment 遇 `errors.Is(err, asynq.ErrTaskIDConflict)` 返回 nil（该 deployment 的任务已在队/在执行，视为已派发），其余错误照常返回。
  - `Deployer` 接口增加 `Dispatch(context.Context, *app.App, *deploy.Deployment) error`；`mocks/deployer.go` 按现有生成风格手工补充 Dispatch mock 方法。
  - 单测：deployer_test.go 增补 Dispatch 冲突归一（可用 seams 或 mock task client，参照现有测试写法）。
- **Acceptance Criteria Addressed**: AC-5, AC-6
- **Test Requirements**:
  - `rule` TR-4.1: Deploy 行为与重构前一致（既有 deployer_test 与全部 webhook 套件通过）；证据：测试全绿。
  - `rule` TR-4.2: Dispatch 遇 ErrTaskIDConflict 返回 nil，其余入队错误返回错误；证据：deployservice 单测。
  - `rule` TR-4.3: 编译期保证 DefaultDeployer 与 mocks.Deployer 均实现扩展后接口；证据：编译通过。

## Task 5: provider 解析层提取投递身份与原始 payload
- **Status**: `completed`
- **Completion Evidence**:
  - TriggerDeployInput 增加 Provider/deliveryID/payloadRaw；GitHub verifier 返回验签 body，三家处理器在验签后捕获原始字节并重置 body；身份头 X-GitHub-Delivery/X-Gitlab-Event-UUID/X-Request-UUID 经 trim，空值即旧路径。
  - 内部测试覆盖 gitlab/bitbucket 头映射与 payloadRaw；三家既有 webhook 套件（含签名/非匹配/204）全绿，body 捕获无回归。TR-5.1~5.3 满足。
- **Priority**: high
- **Depends On**: None
- **Description**:
  - `TriggerDeployInput` 增加导出字段 `Provider string`（"github"/"gitlab"/"bitbucket"）与非导出 `deliveryID string`、`payloadRaw json.RawMessage`；新增包内访问器供编排层使用（同包直接读字段即可，保持非导出）。
  - GitHub：`processGithubPayload` 设置 Provider，并读取 `X-GitHub-Delivery`（trim、空串视为缺失）；原始 body 已在 verifier 中读出，调整为可复用（verifier 成功后 body 已重置为 bytes.Reader；处理器解析前读出全部字节→再重置→hook.Parse，解析后保留 payloadRaw；签名计算所用字节与解析字节必须为同一份）。
  - GitLab：读 `X-Gitlab-Event-UUID`；hook.Parse 前用 bytes 读取并重置 req.Request.Body（受 maxWebhookPayloadSize 约束）。
  - Bitbucket：读 `X-Request-UUID`，同样捕获/重置 body。
  - 单测：三家头识别（有/无/空白）、provider 名正确、payloadRaw 与请求体一致、无身份头时旧行为不变。
- **Acceptance Criteria Addressed**: AC-10, AC-11, AC-13
- **Test Requirements**:
  - `rule` TR-5.1: 三家身份头分别映射到 input 的 provider+deliveryID；空/缺时 deliveryID 为空；证据：provider 解析测试。
  - `rule` TR-5.2: 捕获原始 body 不破坏 hook.Parse（现有全部签名/事件测试不变），payloadRaw 为验签所用同一字节；证据：既有 github/gitlab/bitbucket 套件全绿 + 断言。
  - `rule` TR-5.3: 捕获 body 后非匹配事件仍 204；错签名仍 403 且 body 不被提前读取（GitHub）；证据：既有用例。

## Task 6: delivery 编排：认领式 TriggerDeploy 与 HTTP 语义
- **Status**: `completed`
- **Completion Evidence**:
  - handler_inbound_webhooks_delivery.go：认领编排（upsert→提交守卫→候选→新建/在飞/可重试认领→Deploy→MarkSucceeded/Retryable）、200/202/204/208/503 聚合、RecoverPendingWebhookDeliveries（deployment 已存在仅 Dispatch，不存在则重放事件）；legacyTriggerDeploy 逐行保留。
  - ClaimResult 增加 created 标志修正「新建 claim 被自己当作在飞」缺陷；MarkSucceeded 将 0 deployment 归一为 NULL 避免 FK 违规。
  - 新增 HTTP/恢复测试：重复投递、并发 202、多环境扇出、失败→503→重投 200、无候选 204、GitLab 提交守卫 208、三家身份头用例、两类崩溃窗口恢复、恢复跳过集合。apphandlers 整包通过。TR-6.1~6.5 满足（rubric 留待独立审查评分）。
- **Priority**: high
- **Depends On**: Task 2, Task 4, Task 5
- **Description**:
  - 新文件 `handler_inbound_webhooks_delivery.go`（package apphandlers）：
    - `TriggerDeploy` 在 sample/fork 短路之后按 `input.deliveryID == ""` 分流：旧逻辑保持为独立函数（legacyTriggerDeploy，行为/响应码逐行不变）；非空走 `triggerDeployWithClaims`。
    - `triggerDeployWithClaims(ctx, input)`：
      1. UpsertDelivery（失败→503）。
      2. 跨事件提交守卫（复用 commitHasBeenBuilt，命中→208，语义同 FR-13）。
      3. DeployCandidates + FilterDeployCandidates（含 AppID scoping、ShouldPublish 判定，逻辑不变）；无候选→204（父事件已留底）。
      4. 逐候选 ClaimResult：既有 succeeded→计数跳过；新鲜 processing→inProgress 计数；retryable/过期 processing→ReclaimResult，抢不到记 inProgress；新行→处理。
      5. 处理单个认领 `processClaim`：构造 depl（PopulateFromDeployCandidate，WebhookEvent 用父事件 payload 重放：首次用 input.payload，重放用 json.Unmarshal(payloadRaw) 的通用 map）；设置 `depl.DeliveryResultID=result.ID`：
         - 调用 Deployer.Deploy；成功→MarkSucceeded（含 depl.ID）；GitHub pending status 逻辑仅在本次实际派发成功时执行。
         - Deploy 错误：区分 deployment 是否已创建（depl.ID!=0 或按 resultID 反查）→MarkRetryable(deploymentID, err, backoff)→503；未创建同样 retryable（deployment_id 空）。
      6. 响应聚合：任一 retryable→503；否则 inProgress>0→202；否则有成功（本次或既往）→200；其余→204。
    - 导出 `RecoverPendingWebhookDeliveries(ctx) error`（Task 7 使用）：DueResults→逐行 ReclaimResult→有 deployment（result.DeploymentID 或 DeploymentByDeliveryResult）则加载 app+DeploymentForDispatch 后 `Deployer.Dispatch`（冲突归一在 Task 4 内）；无 deployment 则 DeliveryByID 重建 TriggerDeployInput（payload 通用 map、provider 等字段还原）→重新选取候选并定位该 (app,env) 候选→processClaim；成功/失败按同一 Mark* 更新；循环内错误不中断整批（记日志继续）。
    - 退避：`min(1m*2^attempts, 1h)`；常量：租约 2 分钟、最大自动尝试 10 次、恢复批量 50。
  - HTTP 测试（apphandlers_test，扩展 InboundGithubSuite 为主，并在 gitlab/bitbucket 套件各加身份头用例）：
    - 重复同 delivery：200/200、Deploy 一次、结果表与 deployment 各 1（AC-1）。
    - 预置新鲜 processing：202、零调用（AC-2）。
    - 一次两环境：Deploy 两次、两条结果两条 deployment（AC-4）。
    - Deploy 首次失败→503+retryable；改 mock 成功重投→200 且 deployment 仅 1（AC-7）。
    - 不同 provider 同 GUID 互不干扰（AC-13）。
    - 无候选/tag/非匹配 204 且无结果行（父事件留底仅限可部署事件：非匹配事件在解析阶段返回 nil，不进 TriggerDeploy，天然不写表——验证即可）。
    - 提交守卫：带身份头但同 commit 已有 deployment→208（AC-13/FR-13）。
    - 恢复函数：过期 processing 且 deployment 已存在→只 Dispatch；不存在→Deploy；成功后 succeeded（AC-5/6）。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-4, AC-5, AC-6, AC-7, AC-8, AC-10, AC-11, AC-13, AC-14
- **Test Requirements**:
  - `rule` TR-6.1: AC-1/AC-2/AC-4/AC-7 对应全部 HTTP 级用例通过，MockDeployer 调用次数与参数（AppID、EnvID、DeliveryResultID）有断言；证据：测试输出。
  - `rule` TR-6.2: legacyTriggerDeploy 路径下认领表零写入且 208/200/204 响应与现状一致（既有 GitLab Test_DoNotRebuildSameCommits 等不改一字通过）；证据：既有套件全绿。
  - `rule` TR-6.3: processClaim 两阶段故障窗口（depl.ID 已有时只 Dispatch；无 ID 时 Deploy）均无重复 deployment；证据：恢复用例 + deployment 计数断言。
  - `rule` TR-6.4: 503 仅用于认领/部署可恢复故障；app 查询错误仍 500、签名错误 403；证据：用例与代码审查。
  - `rubric` TR-6.5: 编排代码可读性与约定一致性；scale 1-5；anchors 同 AC-14；threshold >=4；证据：独立审查。

## Task 7: 后台恢复 job（leader 每分钟）
- **Status**: `completed`
- **Completion Evidence**:
  - job_webhook_deliveries.go 薄封装 RecoverWebhookDeliveries；worker_scheduler master 列表每分钟注册（EE 经 jobs.Server 自动获得）。
  - job 测试覆盖过期 claim 被恢复、新鲜 claim 不触碰；workerserver 整包（含 Redis 依赖套件）host 网络下全绿。TR-7.1~7.3 满足（SKIP LOCKED/批量上限见语句与 DueResults 测试）。
- **Priority**: medium
- **Depends On**: Task 6
- **Description**:
  - `src/ce/workerserver/job_webhook_deliveries.go`：`RecoverWebhookDeliveries(ctx) error` 薄封装调用 `apphandlers.RecoverPendingWebhookDeliveries`（确认 jobs→apphandlers 无导入环；若有环则把恢复编排下沉到 webhookdelivery 可导入的形式并由 apphandlers 提供薄函数，jobs 只依赖该包）。
  - `worker_scheduler.go` master 任务列表增加 `{Handler: RecoverWebhookDeliveries, Def: gocron.DurationJob(EVERY_MINUTE), Opt: immediate}`。
  - 测试：`job_webhook_deliveries_test.go`（databasetest + factory，参照 job_deployments_test.go）：构造过期 processing / 到期 retryable / succeeded / 新鲜 processing / attempts 达上限五类结果行，运行后断言仅应处理的行被恢复（配合 MockDeployer/Dispatch mock 或直接断言状态变迁；在不依赖 Redis 的前提下，可令处理路径经 MockDeployer 成功）。
- **Acceptance Criteria Addressed**: AC-5, AC-6, AC-9
- **Test Requirements**:
  - `rule` TR-7.1: job 执行后过租约 processing 与到期 retryable（attempts<10）被恢复为 succeeded 或按当前错误更新退避；succeeded/新鲜/达上限行不变；证据：job 测试。
  - `rule` TR-7.2: 任务出现在 RegisterMasterTasks 列表且 EE worker 经 jobs.Server 自动获得；证据：代码审查 + start/调度相关编译。
  - `rule` TR-7.3: 单批上限 50、SKIP LOCKED、单条失败不影响整批；证据：job 测试（注入一行坏数据仍处理其余）与语句审查。

## Task 8: 全量回归与编译验证
- **Status**: `completed`
- **Completion Evidence**:
  - Linux golang 1.26 容器 + 专用 Postgres/Redis，`-p=1` 整树回归 ./src/ce/api/app/... + workerserver：全部受影响包（app/apphandlers/deploy/webhookdelivery/deployservice/workerserver/volumeshandlers 等）通过；唯一失败 TestHandlerDeployStart/Test_Zip、Test_Zip_AutoPublish 在干净基线 worktree 上同样失败（依赖本地 runner 二进制，环境性既有问题）。
  - public/v1 全包在基线同样超时挂起（MCP SSE 套件跨套件干扰），失败测试隔离运行在两边均通过，与本次改动无关。
  - `go vet -tags=imageopt,alibaba` 受影响包零告警；`gofmt -l` 零差异；`go build -tags=alibaba ./...` 全模块编译通过（imageopt 标签在裸容器缺 libvips，属环境限制）。TR-8.1~8.3 满足。
- **Priority**: high
- **Depends On**: Task 6, Task 7
- **Description**:
  - Windows 主机：`GOOS=linux go vet ./src/ce/api/app/... ./src/ce/workerver/... ./src/lib/...` 与 `GOOS=linux go test -tags=imageopt,alibaba -c -o NUL` 编译全部受影响测试包。
  - Linux golang 容器（挂载本仓库，连接专用 Postgres 容器/本项目 .env 库）跑：`go test -tags=imageopt,alibaba -count=1 ./src/ce/api/app/apphandlers/... ./src/ce/api/app/deploy/... ./src/ce/api/app/deployservice/... ./src/ce/workerserver/...`。
  - 运行既有 webhook 三家套件确认零修改通过；检查 gofmt。
- **Acceptance Criteria Addressed**: AC-1..AC-14（验证收口）
- **Test Requirements**:
  - `rule` TR-8.1: 受影响包全部测试通过、0 失败；证据：容器内 go test 完整输出。
  - `rule` TR-8.2: 新增/修改文件 gofmt 无差异、go vet 无告警；证据：命令输出。
  - `rule` TR-8.3: 迁移在空库全新应用成功（databasetest init 路径即为证据）。
