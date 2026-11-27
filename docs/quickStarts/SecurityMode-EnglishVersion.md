# Security mode and Indagent integration

The extension implements the IG0–IG9 security boundaries without changing public `rpc.thrift`. IG0–IG8 are enabled by `security.properties`; the IG9 replica initialization correction also applies in legacy mode. Indagent orchestration and its custom placement policy remain application responsibilities.

## Configuration

Copy `conf/security.properties.template` beside the active `config.properties` as `security.properties`, uncomment complete groups, and replace the placeholders. Resolution honors `IGINX_CONF` and `IGINX_HOME`. No file preserves legacy behavior. An empty file enables session, SQL, REST and MQTT checks. Settings are immutable until restart and excluded from SHOW/SET CONFIG.

|          Properties           |                                Requirements                                 |
|-------------------------------|-----------------------------------------------------------------------------|
| `zookeeper.user/password/acl` | All three together; acl is open or creator                                  |
| `etcd.user/password`          | Both together; use a dedicated non-root account                             |
| `internal.secret/auth`        | Both together; auth is optional or required; share the secret across nodes  |
| `reserved.prefixes`           | Literal dot-delimited prefixes, e.g. indagent,bigfile,sys.blob              |
| `reserved.accounts`           | Account allowlist with `*` wildcards; required when prefixes are configured |

Passwords and internal secrets must contain at least 20 characters. Invalid keys, incomplete groups, empty values or invalid enums fail startup. Unix files must not be group/other readable (`chmod 600`); on Windows restrict access with OS ACLs. The template contains only comments.

## Authorization

Queries, aggregates, exports, columns, user/function lists and job status require Read. Inserts, deletes, clearing data and CSV upload/import require Write. INSERT FROM SELECT requires both. User/config/session/rule/function management and job submission/cancellation require Admin. Storage management, cluster information and COMPACT require Cluster.

Every SQL entry point checks its parsed statement. Sessions must be issued by successful authentication; failed login retains the existing failure-status/sessionId=0 response. Pagination and closeStatement require the originating session. debugInfo is disabled. Password/permission changes and account deletion revoke existing sessions; remote nodes receive metadata watch notifications.

Explicit reserved paths are denied before execution, including mixed inserts and dynamically generated CSV/VALUE2META paths. General wildcard reads, aggregation, pagination and deletion exclude reserved columns. Root is restricted unless allowlisted. Create a dedicated `indagent_data_*` Read/Write account and rotate by creating a replacement account, moving callers, then deleting the old account. Change the initial root password through user management; configuration does not reset an existing root password.

REST requires Basic authentication on every route, uses the caller identity across asynchronous work and closes request sessions. Authentication failures return 401; denied operations return 403. MQTT authenticates each connection, requires Write for publication and prohibits subscriptions. Revoked connections must reconnect. FileSystem RPC authenticates all four internal methods using a constant-time HMAC comparison bound to the method and arguments; optional mode supports rollout, required mode rejects unsigned or invalid requests. Internal Transform contexts bypass external session checks but retain reserved-path restrictions.

## ZooKeeper rollout

Use `sbin/zk_acl_migrate.sh` or `.bat` with check/apply/revert. Back up metadata first.

New deployments starting directly with creator mode must also run apply/check after initialization to secure the ZooKeeper root `/`.

1. Deploy patched nodes with common credentials, `zookeeper.acl=open` and `internal.auth=optional`. Restart each node and verify reads/writes.
2. Run check/apply/check. Only IGinX metadata roots are changed. `/other`, `/indagent` and `/zookeeper` remain untouched. Root `/` keeps anonymous READ; only the metadata account gets write/admin rights. Active lock directories are skipped with a nonzero result; retry after release.
3. Switch nodes to creator/required and restart sequentially. Apply/check again to cover nodes created during rollout. An unchanged apply reports zero changes.
4. To revert, first restart every node with open/optional, run revert, then remove security configuration and restart. Check reports nonzero after reverting. Do not remove credentials while creator ACLs are still active.

All 25 lock parent/intermediate nodes are persistent. ZooKeeper 3.7.2 masks container ownership to zero in client Stat, so that field cannot prove persistence. Migration atomically replaces idle, unverified lock subtrees and records an HMAC proof bound to each path and creation zxid in the otherwise-unused Curator parent payload. It refuses replacement when an active lock child exists. The embedded-server tests inspect the actual container set as well as ACLs.

## etcd maintenance window

Use `sbin/etcd_auth_migrate check|apply|revert`. Supply the operator root password through an interactive prompt or `IGINX_ETCD_ROOT_PASSWORD`. The dedicated `<user>_iginx_metadata` role grants the byte-prefix ranges `[root, prefixEnd(root))` used by IGinX, including `/id`. Existing keys include `/userroot` and `/storage000…`; granting only slash-delimited descendants is insufficient. Other applications must not reuse IGinX prefixes. Tests verify that `/other`, `/indagent`, and `/application-user` remain outside the grant. Repeated apply leaves correct permissions intact. Shared roles and unrelated account roles are rejected.

**Verified limitation: jetcd 0.5.11 with etcd 3.5.13 requires downtime for authentication rollout.** Configured credentials fail while global auth is disabled. Provision accounts first, stop IGinX, install credentials, explicitly enable etcd auth as the operator, and restart configured nodes. Then verify data access, watches and check. Only the operator provisioning command can use an anonymous connection after an explicit auth-not-enabled response; the server never falls back.

For rollback, stop nodes, explicitly disable global auth where compatible with other applications, revoke the managed role and remove IGinX credentials before restarting. Otherwise retain authentication and the required account. Migration never toggles global authentication and never removes unrelated data, users or roles.

## Replicas and testing

New metadata stores retain `max(0, replicaNum)` even if the first node currently sees fewer engines. Existing persisted counts are unchanged. Insufficient engines produce a warning and fragment allocation fails explicitly with `not enough storage engines for replication`; start enough writable engines before writes.

Run the Java 8/JUnit 4 commands in [the Chinese guide](SecurityMode.md#回归测试). Tests use temporary directories and loopback services; run SecurityIntegrationIT in its own fork. `security-test.yml` covers Linux/Windows isolated tests, etcd migration, and the existing FileSystem suite in security mode. `assert_test_reports.py` fails missing, skipped-only or failed reports. LegacyBehaviorIT pins the unsecured behavior and skips in security mode. Existing REST tests accept IGINX_TEST_REST_USER/PASSWORD, and the Python debug-info test skips only in security mode.

Full deployment acceptance additionally requires cross-node outage/recovery, concurrent cancellation, expansion, Python/UDF/Transform, and three-node rolling migration/rollback scenarios from the design. Keep those reports alongside cluster/restart/full-suite CI results; isolated tests do not replace them.
