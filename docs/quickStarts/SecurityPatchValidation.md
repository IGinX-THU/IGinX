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

## 原有失败及未完成的外部验证

FileSystem 的共同失败为 `FileTreeDummyTest.testEmptyDir`、`testEmptyDirPrefix`、`testNestedPrefix`；共同错误为具体基类 `AbstractRemoteServiceTest.testInsertAndClear` 缺构造函数参数解析器。ServerTest 通过。Asia/Shanghai 时区下 CSV 的原 5 项日期断言通过。没有删改这些原有断言；CI 的 Jupiter 对照任务保存双方完整结果并拒绝新增差异，不将已知失败伪装成全通过。

PR 0 的远程原始大矩阵已触发。ZooKeeper 3.7.2 的 dlcdn 下载返回 404，部分后续任务因此无法启动 IGinX；补丁 PR 将固定版本改用已验证 HTTP 200 的 Apache 归档。PR 0 的格式、许可证、CodeQL 和三平台单元任务通过，不代表原完整矩阵全绿。原 FileSystem test-list、IoTDB12 + FileSystem ClusterIT、重启矩阵及所有目标平台仍以各 PR 的 Actions 记录为准。

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
