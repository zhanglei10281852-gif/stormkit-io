# Independent Review Report - Webhook 投递认领与部署幂等性

- **Reviewer**: 独立子代理（fresh context，只读）
- **R1 Date**: 2026-10-08
- **R2 Date**: 2026-10-08（修复后复审）
- **Scope**: spec.md/tasks.md、0029 迁移、webhookdelivery 新包、deploy/deployservice 改造、apphandlers 编排、workerserver job 及全部新测试
- **Run Context**: Linux golang 1.26 容器（专用 Postgres stormkit-pg-01 + Redis，skit-net/host），`-tags=imageopt,alibaba`

## Checkpoint Results（R2 最终结论）

| Checkpoint | 覆盖 AC | R1 | R2 | Evidence |
|---|---|---|---|---|
| CP-R1 | AC-1 | PASS | PASS | Test_Delivery_DuplicateRequestDeploysOnce（三家各一例）：200/200、Deploy=1、succeeded 1 行；ON CONFLICT + succeeded 短路 |
| CP-R2 | AC-2 | PASS | PASS | Test_Delivery_ConcurrentRequestAccepted：预置新鲜 processing → 202、零部署；ClaimResult created 标志区分新插/既有 |
| CP-R3 | AC-3 | PARTIAL | PASS | 增补显式 23505 断言（Test_ClaimResult_NewAndExisting）；fan-out 测试 + DueResults 回读 |
| CP-R4 | AC-4 | PASS | PASS | Test_Delivery_MultipleEnvironmentsFanOut：Deploy=2、env_id 互异、2 行 succeeded |
| CP-R5 | AC-5 | FAIL（F-1） | PASS | HTTP/后台统一 resumeClaimedResult：deployment 已存在仅 Dispatch；Test_Delivery_RedeliveryWithDeploymentDispatchesOnly + Test_RecoverStaleClaim_DispatchesExistingDeployment；ErrTaskIDConflict 归一 |
| CP-R6 | AC-6 | PASS | PASS | Test_RecoverStaleClaim_CreatesMissingDeployment：重放事件、DeliveryResultID 绑定、Deploy=1 |
| CP-R7 | AC-7 | PASS | PASS | Test_Delivery_DeployFailureThenRedelivery（插入前失败）+ Dispatch-only 用例（入队失败窗口）；503→retryable→200 |
| CP-R8 | AC-8 | FAIL（F-2/F-3） | PASS | DeployCandidates 失败恢复 500；deploymentForClaim 错误上抛；终态写失败回落 503 |
| CP-R9 | AC-9 | PARTIAL | PASS | 新增 Test_RecoverDueRetryable / Test_RecoverSkipsExhaustedRetry / Test_Recover_BadClaimDoesNotStopBatch；leader 每分钟注册 |
| CP-R10 | AC-10 | PASS | PASS | legacyTriggerDeploy 逐行保留；Test_DoNotRebuildSameCommits 等零修改通过；无身份头零认领写入 |
| CP-R11 | AC-11 | PASS | PASS | 拒签不读 body、非匹配/tag/fork/sample 短路不写表，三家既有套件全绿 |
| CP-R12 | AC-12 | PARTIAL | PASS | 迁移幂等；新列 nullable；显式列 INSERT 不受影响；软删除谓词测试；整树回归与基线失败集一致 |
| CP-R13 | AC-13 | PASS | PASS | 内部测试 + store provider 维度测试 + 三家用例；trim/空串降级 |
| CP-R14 | AC-14 | 4/5 | 4/5 | 分层/命名/注释/shttp 约定/测试组织符合 AGENTS.md |

## Findings 处置

| ID | Severity | 问题 | 处置 |
|---|---|---|---|
| F-1 | major | HTTP reclaim 路径无条件 Deploy，deployment 已存在时撞唯一键返 503 | 已修：新增 resumeClaimedResult，HTTP/后台共用「有 deployment 仅 Dispatch」；补 HTTP 用例 |
| F-2 | major | DeployCandidates 错误返 503（FR-9 要求 500） | 已修：恢复 shttp.Error=500 |
| F-3 | minor | deploymentForClaim 吞 DB 错误 | 已修：返回 (depl, error) 并上抛 |
| F-4 | minor | job 测试证据缺口 | 已补：到期 retryable、attempts=10 跳过、坏行不中断整批 |
| F-5 | suggestion | SKIP LOCKED 在自动提交下不持锁 | 已加注释说明真正门闩是 ReclaimResult 条件 UPDATE |
| F-6 | suggestion | 部分唯一索引未排除软删除 deployment | 已修：谓词加 deleted_at IS NULL；两个 dispatch 查询同步过滤；补 Test_InsertDeployment_SoftDeletedFreesClaim |
| F-8 | suggestion | MarkSucceeded 无状态守卫 | 已修：AND status='processing'（reclaim 后 processing 的状态机由测试固化） |
| F-7 | suggestion | 确定性失败（额度/体积）也重试 10 次 | 接受：FR-6 字面行为，运营可观测（attempts/last_error） |
| F-9 | suggestion | mock 成功不插 deployment，HTTP 层未断言 deployment_id | 接受：deploy 包唯一索引/绑定测试兜底；dispatch-only 用例已用真实 deployment |

## R2 非阻塞观察（记录，不影响合入）
1. MarkSucceeded 守卫可进一步强化为 owner-token（claimed_at 纪元比对）；当前残余竞态需 HTTP 处理卡超 2 分钟租约且秒级流程下不可达，且唯一索引保证无第二条外部部署。
2. dispatch-only 分支不再发 GitHub pending status（装饰性，首投通常已发）。
3. app 在认领窗口内被删除时 HTTP 计成功（200）而行状态 retryable；该环境已无部署意义，attempts 上限后停车。

## 回归验证（实现者执行，审查者静态核对）
- 通过包：apphandlers、deploy、deploy/webhookdelivery、deployservice、workerserver 及整树 ./src/ce/api/app/...（`-p=1`，host 网络，2026-10-08）。
- 与干净基线（git worktree @976537e）对照，唯一复现的既有失败为 TestHandlerDeployStart/Test_Zip、Test_Zip_AutoPublish（依赖本地 runner 二进制）与 public/v1 全包 MCP 套件挂起；隔离运行均通过，与本次改动无关。
- `go vet -tags=imageopt,alibaba` 零告警；`gofmt -l` 零差异；`go build -tags=alibaba ./...` 通过。

## Review History
- **R1: FAIL** → 2 major + 4 minor/suggestion 必修/建议项；实现者修复并补测试。
- **R2: PASS** → F-1/F-2 及 F-3/F-4/F-5/F-6/F-8 全部以代码+测试证据闭合，无阻塞性新缺陷。
