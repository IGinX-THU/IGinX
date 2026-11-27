# IGinX 安全模式与 Indagent 接入

此扩展对应安全补丁设计的 IG0–IG9，公共 `rpc.thrift` 保持兼容。IG0–IG8 由安全配置文件显式启用；IG9 的副本初始化修复始终生效。Indagent 的总控、调度、命名空间租约与自定义放置策略仍由 Indagent 实现。

本指南描述 PR 1–8 全部集成后的行为。当前累计阶段见 [security-patch-stage.json](security-patch-stage.json)：入口检查从 PR 1、撤销从 PR 2、保留前缀从 PR 3、REST/MQTT 从 PR 4、ZooKeeper 从 PR 5、etcd 从 PR 6、内部 HMAC 从 PR 7、副本修复从 PR 8 生效。PR 1 即识别全部配置键。

## 启用配置

将 `conf/security.properties.template` 复制为 `config.properties` 同目录的 `security.properties`，填写所需配置组。配置路径遵循 `IGINX_CONF` 和 `IGINX_HOME`。没有此文件时保持旧的认证行为；空文件启用会话、SQL、REST/MQTT 权限检查。配置只在启动时读取，不能通过 SHOW/SET CONFIG 读取或修改其中的密钥。

|              配置               |                         约束                          |
|-------------------------------|-----------------------------------------------------|
| `zookeeper.user/password/acl` | 三项同时提供；acl 为 `open` 或 `creator`                     |
| `etcd.user/password`          | 两项同时提供；使用独立的非 root 账户                               |
| `internal.secret/auth`        | 两项同时提供；auth 为 `optional` 或 `required`；同一集群共享 secret |
| `reserved.prefixes`           | 逗号分隔的完整路径前缀，如 `indagent,bigfile,sys.blob`           |
| `reserved.accounts`           | 允许访问保留前缀的账户名模式，仅 `*` 表示通配；设置前缀时必须提供                 |

三种密码/密钥均至少 20 个字符。未知键、缺失的配对项、非法枚举和空值会使启动失败。Unix 文件不得对组或其他用户可读，建议 `chmod 600 security.properties`；Windows 需通过 ACL 限制服务账户以外的读取权限，程序输出设置提示。模板只含注释，部署时应取消所需配置组的注释。

## 账户与请求

首次初始化后，通过用户管理接口修改 root 密码；以后以元数据中的密码为准，修改配置中的 password 不会重置已有 root。为 Indagent 建立符合允许名单的数据账户并授予 Read/Write；凭据轮换可新建 v2 账户、切换调用方、再删除 v1。root 本身也受保留前缀限制，除非明确列入允许名单。

|               操作                |     所需权限     |
|---------------------------------|--------------|
| 查询、聚合、列信息、导出、用户列表、函数列表和作业状态     | Read         |
| 插入、删除、清空、CSV 导入/上传              | Write        |
| INSERT FROM SELECT              | Read 与 Write |
| 用户管理、配置、会话列表、规则、函数注册/删除、作业提交/取消 | Admin        |
| 存储引擎管理、集群信息、COMPACT             | Cluster      |

所有 SQL 入口根据解析后的语句检查权限。sessionId 必须来自有效登录；登录失败仍返回失败状态和 0，调用方必须检查状态。结果分页和关闭语句绑定原会话。debugInfo 在安全模式下禁用。修改密码、权限或删除账户会撤销该账户的已有会话，其他账户不受影响；跨节点依赖元数据 watch 通知。

保留路径采用完整段边界匹配，`indagent2.x` 不属于 `indagent`。显式访问和混合写入在执行前拒绝；通配读、聚合、列分页和删除只处理可见列。CSV 表头与 VALUE2META 产生的运行时路径再次检查。内部 Transform 上下文不需要外部会话，但仍不能访问保留路径。物理压缩、迁移及已接受写入的副本传播继续正常运行。

REST 所有路径要求 HTTP Basic 认证，登录失败为 401，无操作权限为 403；每个请求使用真实调用账户，并在请求结束清理会话。MQTT 使用真实连接账户，安全模式禁止订阅；发布者须有 Write 权限且满足路径限制，变更凭据后必须重连。FileSystem 内部 RPC 的四个方法校验绑定方法名和完整参数的 HMAC，`optional` 用于滚动迁移，`required` 拒绝缺失或不匹配的凭据。

## ZooKeeper 两阶段迁移

迁移脚本位于发行包的 `sbin/zk_acl_migrate.sh` 或 `.bat`。脚本读取当前配置，口令不放在命令行。迁移前备份元数据。

全新部署即使直接使用 `creator`，首次初始化后也应执行一次 apply/check，使 ZooKeeper 根节点 `/` 的 ACL 同样生效。

1. 各节点部署补丁，配置相同 ZooKeeper 账户、`zookeeper.acl=open` 和 `internal.auth=optional`，逐个重启并验证读写。
2. 执行 `zk_acl_migrate check`、`zk_acl_migrate apply`、`zk_acl_migrate check`。apply 只调整 IGinX 实际使用的元数据根，不遍历修改 `/other`、`/indagent` 或 `/zookeeper`；根 `/` 保留匿名 READ，仅 IGinX 元数据账户具有写/管理权限。持有活动锁的目录被跳过并返回非零，释放后重试。
3. 各节点改为 `zookeeper.acl=creator`、`internal.auth=required`，逐个重启。全部重启后再次 apply/check，覆盖滚动期间创建的节点。无改动时重复 apply 输出 0 changed。
4. 回退时先逐个改回 open/optional 并重启，再执行 `revert`；此时 check 应报告非零。最后逐个移除安全配置并重启。不要在 creator ACL 仍生效时先移除凭据。

锁父节点总计 25 个，使用 PERSISTENT。ZooKeeper 3.7.2 会在客户端 Stat 中把容器节点的 ephemeralOwner 隐藏为 0，不能据此判定节点类型。迁移工具保守地转换未经验证的锁目录，以原子事务重建空闲的锁目录子树，再写入绑定路径、创建 zxid 和元数据密钥的证明。证明写在 Curator 不使用的锁父节点数据中；不改变业务元数据。活跃锁子节点存在时不重建。测试同时检查服务端容器集合，避免仅检查客户端 Stat 的假阳性。

## etcd 切换与回退

`sbin/etcd_auth_migrate check|apply|revert` 使用控制台输入或 `IGINX_ETCD_ROOT_PASSWORD` 中的操作员 root 密码，创建专用账户及 `<用户名>_iginx_metadata` 角色。权限覆盖 IGinX 实际使用的字节前缀范围 `[root, prefixEnd(root))`，包含 `/id` 分配目录。现有 etcd 键包含 `/userroot`、`/storage000…`，不能只授权 `root/` 子树。其他应用不得复用这些 IGinX 前缀；测试确认 `/other`、`/indagent`、`/application-user` 不在授权范围。重复 apply 不撤销已正确的权限；共享角色或已有无关角色会拒绝修改。

**已实测的版本限制：jetcd 0.5.11 + etcd 3.5.13 需要停机窗口。** etcd 未启用认证时，带用户名/密码的 jetcd 客户端报 `authentication is not enabled`，不能按“先配凭据滚动重启，再开启认证”的顺序上线。采用以下顺序：

1. 在认证关闭期间先准备 root 和专用账户，执行 apply；迁移命令仅在服务端明确报告认证未开启时使用无认证的操作员连接。check 此时仍应报告认证未启用。
2. 停止 IGinX 节点，部署安全配置；操作员使用 etcd 管理工具开启全局认证，再启动所有已配置的节点，验证读写、watch 和 check。
3. 回退也安排停机窗口：停止节点，操作员明确关闭全局认证，执行 revert 并移除配置中的 etcd 凭据，再启动节点。若其他应用依赖全局认证，应保留认证及 IGinX 账户，不能仅删除其权限。

迁移命令从不自动开启/关闭全局认证。IGinX 服务端从不自动退回匿名访问。revert 只撤回本工具管理的角色，不删除其他用户/角色或数据。

## 副本初始化

`replicaNum` 表示主副本以外的副本数，新集群记录 `max(0, replicaNum)`，不再按第一个节点当时可见的引擎数量永久降低配置。已有元数据中的副本数保持不变。引擎不足时告警；首次分片或重分片要求足够的可写引擎，否则明确返回 `not enough storage engines for replication`。应先启动所需引擎再写入。

## 回归测试

使用 Java 8 和仓库的 JUnit 4 runner：

```sh
mvn -P-format -pl dataSource/filesystem -am test -Dtest=SecurityConfigTest,SessionManagerTest,StatementAuthTableTest,UploadRegistryTest,NaivePolicyReplicaTest,ZkAclTest,InternalAuthTest -DfailIfNoTests=false
mvn -P-format -pl dataSource/filesystem -am test -Dtest=SecurityIntegrationIT,SecurityStartupIT -DfailIfNoTests=false
mvn -P-format -pl dataSource/filesystem -am test -Dtest=ClusterSecurityIT -DfailIfNoTests=false
IGINX_TEST_ETCD=/path/to/etcd mvn -P-format -pl core -am test -Dtest=EtcdAuthIT -DfailIfNoTests=false
```

集成测试使用临时目录和本机端口，需允许回环网络连接。`SecurityIntegrationIT` 单独运行，避免配置单例被其他测试提前初始化。CI 的 `security-test.yml` 在 Linux/Windows 上运行隔离测试、在 Linux 上验证 etcd，并在安全模式下运行现有 FileSystem 回归清单。每步使用 `assert_test_reports.py` 检查执行数量及失败数，防止漏跑。原来的 UserPermissionIT 清单项已修正为实际存在的 JUnit 4 UserManagementIT；LegacyBehaviorIT 只在非安全模式执行。

完整的设计验收还包括跨节点断网恢复、长查询并发撤销、在线扩容、完整 Python/UDF/Transform、两阶段三节点升级与回退。新增隔离测试不能替代这些部署验收；应结合现有集群、重启和完整回归工作流保存报告。
