# 安全补丁实现与验证记录

基线 `51b58d9f6`；工作分支 `codex/indagent-security-extensions`。48 个场景、PR 归属和方法位置见 [覆盖清单](security-test-matrix.json) 与 [更新后的核查报告](SecurityPatchAudit.md)。本记录区分新增验收、原测试回归和外部 CI；不把基线已有失败称为全部通过。

## 新增验收的实测

证据位于本地 `target/indagent-tests/`、`target/security-acceptance/` 及各模块 `target/surefire-reports/`。JVM、ZooKeeper、etcd 和 FileSystem 均使用测试专用目录与端口，不修改实际部署。

|                       验证内容                       |                                             实际结果及证据                                              |
|--------------------------------------------------|--------------------------------------------------------------------------------------------------|
| 会话 ID、版本撤销、并发、独立 SQL/RPC 权限表                     | 单元/组件测试通过；`acceptance-round7.log`                                                                |
| 无效会话、四个 SQL 入口、复合语句、上传/查询所有权、配置与密钥               | 5 方法通过；`acceptance-round4.log`                                                                   |
| 五账户 × 34 SQL 与 39 RPC；真实注册/取消与 pip 副作用           | 4 方法通过；`acceptance-round10.log`；权限期望表独立于生产表                                                      |
| 显式/通配保留路径、90 列分页计数、全部删除形式、边界与轮换                  | 5 方法通过；`acceptance-round8.log`                                                                   |
| 真实 SQL/Python Transform、定时作业清理、保留路径失败、COMPACT    | `InternalRequestIT` 通过；`acceptance-round6.log`                                                   |
| 全 REST 路由畸形认证、五角色矩阵、save_as、注解三类型与长查询撤销          | 3 方法通过；`focused-round12.log`                                                                     |
| root 改密后 RPC/REST/MQTT、实际重启和恢复                   | 通过；`acceptance-round5.log`                                                                       |
| MQTT 认证、读写/保留路径/订阅权限及持续发布撤销                      | 2 方法通过；早期验收日志；旧模式对照 1 方法通过                                                                       |
| IGT-30 ZooKeeper                                 | 最终 20 轮、180 个节点样本、最大 32 ms；断网 20 秒恢复、跨节点会话拒绝和线程检查通过；`rollout-revoke-final.log`                   |
| IGT-30 etcd                                      | 20 轮、180 个节点样本、最大 170 ms，线程检查通过；`etcd-revoke-final.log`；另两方法见 `etcd-revoke.log`                  |
| ZooKeeper 全路径 ACL、匿名遍历/getACL、持久锁、隔离、真实迁移脚本      | 3 方法通过；`acceptance-round8.log`                                                                   |
| 三节点 ZooKeeper open → creator → 回退                | 通过；`acceptance-round10.log`                                                                      |
| etcd 三节点认证、CRUD/watch、撤销、隔离、幂等迁移和回退              | 通过；`focused-round11.log`                                                                         |
| FileSystem 四 RPC、物理数据与三节点 optional → required 滚动 | 最终 2 方法通过；`rollout-revoke-final.log`                                                             |
| 首启副本数、不足引擎拒绝、已有元数据、两个副本/客户端分区                    | `ReplicaNumIT` 4 方法全部通过，含通配查询故障切换与全部副本离线拒绝；`replica-final.log`                                   |
| 扩容、存储修改、逐节点重启、1000 行关闭/撤销/REST 和实际主存储停机          | 扩容/重启通过见 round10；最终千行写入及故障后完整读回通过见 `replica-final.log`，该轮合计 5/5 通过                               |
| 完整原始基线协议反例 + 旧 MQTT                              | 3/3 通过（验证原缺陷确实存在）；`baseline-comparison-round3.log`                                               |
| 原 IMetaManagerTest，creator ACL，临时 Surefire 3.5.2 | 28/28 通过；`metadata-surefire.log` 与 `metadata-surefire-reports/`                                  |
| 原 FileSystem JUnit 5，临时 Surefire 3.5.2           | 原基线和当前均为 43 项、39 通过、3 失败、1 错误；`baseline-jupiter-all.log` 与 `filesystem-jupiter-full-current.log` |

为了稳定隔离 IGT-30 的撤销测量，先在首节点预置查询数据，再等待三节点元数据和查询就绪；这不把启动或首次分片初始化耗时计入撤销窗口。

最终使用带 `dependency` 模块的 CI 构建命令又运行了 14 项安全单元/组件回归，全部通过，见 `ci-dependency-command-check.log`。

发布后继续检查 CI，并修复了 CTE 语句对象循环引用导致的权限检查栈溢出、空投影跳过后续聚合、合法花括号列名被误解析为标签三个兼容问题。新增 CTE 读权限/保留路径拒绝和花括号列名回归。原 `SQLSessionIT` 在启用保留路径保护的隔离服务上 62/62 通过，见 `ci-recovery-original-full.log`；服务时区为 Asia/Shanghai，线程池参数与原测试预期一致，未修改原断言。

受影响的 SQL 权限、代码注册、保留路径及跨节点 FileSystem 验收共 13/13 方法通过，见 `ci-recovery-entry.log`。安装正向对照单独允许最多 60 秒 RPC 等待；拒绝路径和撤销时限不变。

完整 Maven etcd 验收组最终 5/5 方法通过，执行器核对 4 个文档场景，见 `etcd-maven-recovery-final.log`。新版 ZooKeeper 撤销断言亦通过 20 轮、180 样本、最大 110 ms，见 `ci-recovery-zk.log`。仅含 PR 1 实现的隔离服务上 SQL 入口测试 5/5 通过，见 `early-stage-entry-regression.log`；保留路径拒绝断言从 PR 3 引入，不提前混入 PR 1、2。

IGT-16 的正向取消先等待作业 `JOB_IDLE`，IGT-34 在连接其它节点前验证新账户已传播。IGT-30 保存线程栈，针对缓存线程池 60 秒空闲回收周期最多等待 70 秒，然后同时断言线程总数不增长、测量期间新增的线程全部退出；撤销窗口仍固定 5 秒。更新后的 etcd 测量 20 轮、180 样本、最大 206 ms，全部断言通过，见 `ci-recovery-etcd.log`。真实 pip 正向对照独立复测通过，见 `ci-recovery-pip.log`。

PR 1–8 各累计版本的完整 `test` 模块 reactor 编译均通过，见 `integration-stack-validation.log`；其后生产修复的 5 种阶段变体另行编译通过，见 `ci-variant-compile.log`。`LegacyBehaviorIT` 已改用兼容测试模块 Thrift 版本的 socket 构造方式；FileSystem 的 etcd 验收补齐 Guava `failureaccess` 测试依赖。

PR 0 已收窄为原单方法的 JUnit 4 迁移和清单修正。该原方法在未打补丁服务上独立执行：发现 1 项、失败 1 项，暴露旧 `GRANT` 解析错误，见 `pr0-scope/baseline.log` 与 `pr0-scope/reports/`。PR 1 修复该错误并承载新增的用户引号和过滤回归；累计版本的两个方法在旧模式和安全模式均已通过。这与 PR 0 单独通过是不同的结论。

## 原有失败及未完成的外部验证

FileSystem 的共同失败为 `FileTreeDummyTest.testEmptyDir`、`testEmptyDirPrefix`、`testNestedPrefix`；共同错误为具体基类 `AbstractRemoteServiceTest.testInsertAndClear` 缺构造函数参数解析器。ServerTest 通过。Asia/Shanghai 时区下 CSV 的原 5 项日期断言通过。没有删改这些原有断言；CI 的 Jupiter 对照任务保存双方完整结果并拒绝新增差异，不将已知失败伪装成全通过。

PR 0 的远程原始大矩阵已触发。ZooKeeper 3.7.2 的 dlcdn 下载返回 404，部分后续任务因此无法启动 IGinX；补丁 PR 将固定版本改用已验证 HTTP 200 的 Apache 归档。PR 0 的格式、许可证、CodeQL 和三平台单元任务通过，不代表原完整矩阵全绿。原 FileSystem test-list、IoTDB12 + FileSystem ClusterIT、重启矩阵及所有目标平台仍以各 PR 的 Actions 记录为准。

PR 1–8 首轮 CI 暴露的 Ubuntu 26.04 / Python 3.12.6 不兼容以及独立构建遗漏内置 Pemja 依赖模块均已修正：安全验收固定 Ubuntu 24.04，并显式构建 `dependency`。后续远程任务仍有 Maven Central HTTP 429 下载失败和排队项；这不等于测试断言已通过，需在远程 CI 完成后复核。

[发布后的 CI 批次](https://github.com/IGinX-THU/IGinX/actions/runs/45937082803) 中，Linux/Windows 网关与元数据验收组、Windows 集群验收组、原 ClusterIT、基线反例和 FileSystem Jupiter 对照已通过。该批次元数据 Jupiter 任务在依赖准备时遭遇 Maven Central 429，尚未执行测试；其它失败促成上述修复。此处记录具体修订的结果，不代表后续 PR 最新提交已全绿。

本地未执行 Linux POSIX 0600/0644 和 `.sh` 分支，这些已编写并纳入 Linux CI。已执行 Windows `.bat` 的无效配置拒绝、恢复和迁移错误退出码。补丁前反例是有记录的选定反例集，不是所有负向子组合的穷尽重放。

## 复现

Java 8、Maven、Python（pemjax 0.2.4、pandas、numpy、python-dateutil）准备后：

```sh
python .github/scripts/test/run_security_acceptance.py entry
python .github/scripts/test/run_security_acceptance.py gateways
python .github/scripts/test/run_security_acceptance.py metadata
python .github/scripts/test/run_security_acceptance.py cluster
IGINX_TEST_ETCD=/path/to/etcd python .github/scripts/test/run_security_acceptance.py etcd
python .github/scripts/test/run_security_baseline.py
python .github/scripts/test/run_security_baseline.py --jupiter
```

可通过 `IGINX_TEST_PYTHON` 指定 Python；etcd 固定为 3.5.13。执行器检查实际 XML 中的方法、失败、错误和跳过，缺依赖直接失败。原公共 `rpc.thrift` 未改动；仅内部 FileSystem Thrift 添加可选凭据字段。安全模式迁移、etcd 字节前缀授权和回退见 [安全模式指南](SecurityMode.md)。

## PR 695–702 的 CI 故障修复（2026-11-27）

该轮检查针对实际失败日志，不把下载失败、断言失败和整项作业超时混为同一原因。

- PR 695 的 Controller 作业达到 100 分钟限制后进入取消阶段，总计约 105 分钟；原任务详细日志不可取得，无法证明具体卡在哪个方法。并发 SQL 辅助类曾忽略工作线程异常并无限等待计数器；现在传播异常、统一限时并回收线程。PR 1 同时补齐已取消物理任务的结果通知，内部后台任务不依赖已关闭的提交者会话。
- 多个 PR 的原 `TransformIT.commitMixedPythonJobsByYamlWithRegisterTest` 因后台 SQL 注册函数被新入口鉴权拒绝而失败。注册/删除函数现在显式接收服务端内部上下文；外部 RPC（包括 sessionId=0）仍须通过管理员鉴权。新增提交者断开后的真实后台注册回归，并保留普通用户和伪造会话的拒绝断言。
- Windows IGT-26 的撤销结果不稳定。用户缓存现在按 ZooKeeper zxid / etcd revision 合并本地写入、初始快照和 watch 事件，保留删除版本，拒绝过期事件；同名同密码账户重建也会使旧会话失效。REST 在读取结果后重新验证会话及 RPC 状态，避免取消查询返回 HTTP 200。测试用独立节点夹具阻塞真实存储任务，每轮确认任务已进入后撤销账户；100 轮均保留 5 秒上限、删除后的首次新请求立即返回 401、后续请求持续 401、会话和工作线程回收断言。
- CI 项目版本直接从 POM 读取，避免为了一个版本号下载 Maven help 插件。仅依赖解析失败允许有上限的重试；断言失败不重试。无 GPU 的 Linux/Windows runner 预装同版本 CPU PyTorch，配合 pip 缓存，避免下载无用 CUDA 组件。Java/Python Pemjax 同步主分支 0.2.4。
- 每个回归类最多 15 分钟，Controller 最多 45 分钟，ClusterIT 最多 20 分钟；超时先保存 JVM 线程栈，再终止进程树。单类失败后继续执行其余原测试，最终保留失败退出码；报告、服务日志和超时诊断始终上传。

修复后的本地 Windows / Java 8 验证：核心会话和事件顺序单元测试 8/8；代码注册及 REST 验收 6/6（包含 100 轮真实阻塞撤销）；并发 SQL 辅助类 2/2；ZooKeeper 和 etcd 跨节点撤销各 3/3；原 TransformIT 完整类 27/27 执行项通过（保留原有 2 个忽略项），含原失败方法；原 SQLSessionIT 62/62；Python 超时执行器 4/4（含独立进程组的孙进程清理）。累计版本共 111 个 Java 方法和 4 个 Python 测试通过。另外仅含 PR 1 源码的独立运行时，入口与代码注册 8/8 通过。仓库原有 SQL/Transform 回归断言未修改。

证据保存在修复工作区 `target/ci-fixes/`：`focused-round2.log`、`sql-executor.log`、`zk-regression.log`、`etcd-regression.log`、`transform-regression.log`、`transform-all-regression.log`、`rest-immediate.log`、`sql-regression.log`、`pr1-regression.log` 及对应 XML。以上是明确的本地验证范围；各 PR 最新远程 CI 的完整结果仍须独立核对，历史 FileSystem Jupiter 的四项既有问题仍按原始基线对照。

PR 1–8 各累计版本的完整测试模块 reactor 编译均通过（`repair-stage-compilation.json`，具体提交及日志保存在本轮 CI 修复记录中）。最后保留即时 401 的 REST 断言变更在 PR 4–8 使用相同测试类，另行编译并完成 3/3 验收和 100 轮阻塞撤销。Java 格式检查及许可证检查均通过。

## PR 698–702 后续失败分析与修复（2026-11-27）

本轮依据上述最终提交实际产生的 24 个失败检查逐项分类，原始日志和验收产物保存于修复工作区 `target/ci-fixes-round2/`。同一提交因堆叠分支更新触发了重复运行，以下同时检查了两次运行，不能用其中一次成功覆盖另一次失败。

- FileSystem 原回归在多次运行中通过，最后的 `actions/upload-artifact` 却报 `Maximum call stack size exceeded`，例如 [PR 698 的作业](https://github.com/IGinX-THU/IGinX/actions/runs/45977055108/job/140431297435)。工作流原先以仓库级递归通配符搜索报告，遍历了测试数据库和 Python 环境。改为按 POM 模块定位报告、按指定目录收集日志，再上传一个 ZIP；保留报告、节点日志、原基线对照、超时线程栈和文件清单。诊断目录移至 `.ci-diagnostics/`，避免 `mvn clean` 删除正在写入的证据。
- PR 698、701、702 的部分构建或验收在下载 Maven 插件、JUnit 依赖时持续返回 HTTP 429，未运行测试。原先的两次重试仍访问同一地址。现在仅 GitHub CI 的 Central 429 在有上限的重试中切换至 [Google 公布的 Central 镜像](https://docs.cloud.google.com/docs/buildpacks/java#google_cloud_hosted_maven_central_mirror)，保留总时限，不重试断言失败；本地 Maven 配置不变。新增工作流并发分组，取消同一 PR 被新事件替代的重复运行。
- PR 699 Windows、PR 701 Linux 的 IGT-43 均因 `/iginx/info/node…` 的 `NoNodeException` 启动失败，见 [PR 701 元数据作业](https://github.com/IGinX-THU/IGinX/actions/runs/45977058625/job/140473300247)。ZooKeeper 会话过期删除临时节点不受应用层互斥锁约束，`getChildren` 与 `getData` 之间仍可能发生成员离开。成员快照读取现在只忽略已离开的节点，保留其它节点；权限、连接等其它异常继续抛出。新增真实 ZooKeeper 的确定性离开竞态和拒绝未授权读取两项回归，从 PR 5 起执行。
- PR 701 的 etcd IGT-30 报新增 `TThreadPoolServer WorkerProcess-5` 未回收；产物中的线程栈显示它在 `ThreadPoolExecutor.getTask` 空闲等待，总线程数从 26 降至 24。默认 20 个 Thrift 核心线程按需创建且不因空闲退出，原基线只看到其中 5 个。测量前同时持有足够的 RPC 连接完成核心池预热，再关闭这些测试会话；20 轮、180 个节点样本、5 秒撤销上限、线程总数不增长及新增工作线程回收断言均保留。
- PR 702 格式检查指出验证文档中多余的空行，已移除。Windows 检出换行与 Linux 检查的差异单独核对，避免局部过滤检查漏掉文档。
- PR 702 的一次 UDFIT 在 `SELECT no_mod(1.98)` 后达到 15 分钟限制；同提交另一次及其它 PR 的该类通过。该次线程栈因归档失败未留存，不能据此断言是业务死锁或已定位其原因。继续保留原 UDF 回归和超时失败，并在本轮完整重跑中核对；归档修复确保再次发生时可取得诊断。

本轮本地复验：真实 ZooKeeper 成员读取回归 2/2；ZooKeeper 滚动迁移与撤销 4/4；etcd 撤销 3/3；CI 进程控制、Central 429 回退、clean 后证据保存及归档测试 7/7。撤销时限与原验收断言未放宽。记录见 `membership-test.log`、`zk-regression.log`、`etcd-regression.log`、`python-tests.log`。最新远程矩阵的结果需按本轮新提交另行核对。

## PR 697 / 699 的归档与取消状态复核（2026-11-27）

- [PR 697 的失败作业](https://github.com/IGinX-THU/IGinX/actions/runs/45977059148/job/140488604328) 中，22 个原回归类均返回 0，最后上传产物时出现栈溢出。本分支尚未包含后继 PR 的归档修复；本轮补齐限定目录的 ZIP 归档、clean 后保留诊断及对应脚本回归。
- [PR 699 的红色汇总检查](https://github.com/IGinX-THU/IGinX/actions/runs/45997093386/job/140499106888) 来自被替代的重复运行，四个前置任务均为 `cancelled`。同提交的有效测试和 lint 运行已经通过。汇总的 `always()` 改为 `!cancelled()`，取消时不再启动汇总，真实失败仍由原检查阻断。本轮首次推送后，重复运行的汇总已正确归为取消，未产生失败。
- 重跑的 [PR 702 集群作业](https://github.com/IGinX-THU/IGinX/actions/runs/46006413323/job/140530664075) 暴露了共享实现的另一处竞态：会话在 RPC 入口检查后、执行器再次检查前失效，执行器提前返回 `Result(ACCESS_DENY)`，未设置 SQL 类型。节点日志明确记录 `ExecuteSqlResp` 的必填 `type` 为空，Thrift 序列化异常关闭连接；客户端因此收到传输异常，而非拒绝状态。`Result` 现在默认使用 `SqlType.Unknown`，同样覆盖解析前过载拒绝及两种 SQL 响应，已知 SQL 类型继续保留，公共 Thrift 协议不变。
- 同轮 [PR 699 的 Windows 元数据作业](https://github.com/IGinX-THU/IGinX/actions/runs/46006414786/job/140530728113) 在回退的最后一个节点持续查询失败。节点日志显示，存储连接表枚举与读取之间，旧成员的 `/iginx/connection-storage/node…` 临时记录消失，导致整个连接表刷新中断；该节点没有可用路由，随后报空操作符错误。存储连接表和节点连接表现共用快照读取，仅忽略已经离开的记录，保留所有存活记录，继续传播权限等其它异常。新增两条路径各自的离开竞态和未授权读取回归；修复前两个离开用例稳定复现 `NoNodeException`。
- 后续 [PR 702 Linux 入口作业](https://github.com/IGinX-THU/IGinX/actions/runs/46010095073/job/140543190006) 在 IGT-16 节点启动时报 `BindException`，未进入业务断言。夹具用 `ServerSocket(0)` 选端口后释放，在子 JVM 启动前可能被系统临时端口分配抢占。夹具改为从 CI 临时端口范围之外的 10000–29999 选择可绑定端口，同一 JVM 不重复分配；新增可绑定性、唯一性及存活监听器不受后续分配影响的两项回归。
- 同轮 [PR 697 Linux 集群作业](https://github.com/IGinX-THU/IGinX/actions/runs/46010097308/job/140543213390) 没有运行测试：首次下载 AntRun 插件失败，后两次只重复本地缓存的失败。依赖重试补上 [`-U`](https://maven.apache.org/ref/3.9.8/maven-embedder/cli.html)，强制检查缺失发布版本；保留原重试次数、总时限、真实失败退出码和断言不重试规则，新增失败缓存刷新回归。
- [PR 700 格式作业](https://github.com/IGinX-THU/IGinX/actions/runs/46010094668/job/140543111309) 的插件前缀查询遇到 Central 429，但下载错误以 WARNING 记录，最终是 `NoPluginFoundForPrefixException`，未被旧下载分类器识别。补齐该类错误的有界重试；必须同时存在下载失败证据，单纯写错插件前缀仍立即失败。

新增 `ResultResponseTest` 通过 Binary、Compact 两种编码实际序列化并读回两种响应。修复前访问拒绝和过载拒绝两项确定性复现同样的缺字段错误，正常类型对照通过。该类纳入各适用分支的入口验收，拒绝响应回归映射到 IGT-30；原三节点撤销的 20 轮、180 样本及 5 秒时限继续保留。原始日志与本轮复验记录位于 `target/ci-fixes-round3/`，最新 CI 结果记录在各 PR 说明中。
