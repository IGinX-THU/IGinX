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

Java 8、Maven、Python（pemjax 0.2.3、pandas、numpy、python-dateutil）准备后：

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
