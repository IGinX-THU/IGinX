package cn.edu.tsinghua.iginx.metadata.storage.zk;

import static cn.edu.tsinghua.iginx.metadata.storage.constant.Constant.*;
import static cn.edu.tsinghua.iginx.metadata.utils.ColumnsIntervalUtils.fromString;
import static cn.edu.tsinghua.iginx.metadata.utils.IdUtils.generateId;
import static cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus.*;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.cache.IMetaCache;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.metadata.exception.MetaStorageException;
import cn.edu.tsinghua.iginx.metadata.hook.*;
import cn.edu.tsinghua.iginx.metadata.storage.IMetaStorage;
import cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus;
import cn.edu.tsinghua.iginx.utils.JsonUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperMetaStorage implements IMetaStorage {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperMetaStorage.class);

  private static final String IGINX_NODE = "/iginx/node";

  private static final String STORAGE_ENGINE_NODE = "/storage/node";

  private static final String STORAGE_UNIT_NODE = "/unit/unit";

  private static final String IGINX_LOCK_NODE = "/lock/iginx";

  private static final String SCHEMA_MAPPING_LOCK_NODE = "/lock/schema";

  private static final String POLICY_NODE_PREFIX = "/policy";

  private static final String POLICY_LEADER = "/policy/leader";

  private static final String POLICY_VERSION = "/policy/version";

  private static final String POLICY_LOCK_NODE = "/lock/policy";

  private boolean isMaster = false;

  private static ZooKeeperMetaStorage INSTANCE = null;

  private final CuratorFramework client;
  private final Lock storageUnitMutexLock = new ReentrantLock();
  private final InterProcessMutex storageUnitMutex;
  private final Lock fragmentMutexLock = new ReentrantLock();
  private final InterProcessMutex fragmentMutex;
  private final Lock fragmentRequestsCounterMutexLock = new ReentrantLock();
  private final InterProcessMutex fragmentRequestsCounterMutex;
  private final Lock fragmentHeatCounterMutexLock = new ReentrantLock();
  private final InterProcessMutex fragmentHeatCounterMutex;
  private final Lock timeseriesHeatCounterMutexLock = new ReentrantLock();
  private final InterProcessMutex timeseriesHeatCounterMutex;
  private final Lock reshardStatusMutexLock = new ReentrantLock();
  private final InterProcessMutex reshardStatusMutex;
  private final Lock reshardCounterMutexLock = new ReentrantLock();
  private final InterProcessMutex reshardCounterMutex;
  private final Lock maxActiveEndTimeStatisticsMutexLock = new ReentrantLock();
  private final InterProcessMutex maxActiveEndTimeStatisticsMutex;

  protected TreeCache schemaMappingsCache;
  protected TreeCache iginxCache;
  protected TreeCache storageEngineCache;
  protected TreeCache storageUnitCache;
  protected TreeCache fragmentCache;
  protected TreeCache reshardStatusCache;
  protected TreeCache reshardCounterCache;
  protected TreeCache maxActiveEndTimeStatisticsCache;

  private SchemaMappingChangeHook schemaMappingChangeHook = null;
  private IginxChangeHook iginxChangeHook = null;
  private StorageChangeHook storageChangeHook = null;
  private StorageUnitChangeHook storageUnitChangeHook = null;
  private FragmentChangeHook fragmentChangeHook = null;
  private UserChangeHook userChangeHook = null;
  private TimeSeriesChangeHook timeSeriesChangeHook = null;
  private VersionChangeHook versionChangeHook = null;
  private TransformChangeHook transformChangeHook = null;
  private ReshardStatusChangeHook reshardStatusChangeHook = null;
  private ReshardCounterChangeHook reshardCounterChangeHook = null;
  private MaxActiveEndKeyStatisticsChangeHook maxActiveEndKeyStatisticsChangeHook = null;

  protected TreeCache userCache;

  protected TreeCache policyCache;

  protected TreeCache timeseriesCache;

  protected TreeCache versionCache;

  private TreeCache transformCache;

  public ZooKeeperMetaStorage() {
    client =
        CuratorFrameworkFactory.builder()
            .connectString(
                ConfigDescriptor.getInstance().getConfig().getZookeeperConnectionString())
            .connectionTimeoutMs(15000)
            .retryPolicy(new RetryForever(1000))
            .build();
    client.start();

    fragmentMutex = new InterProcessMutex(client, FRAGMENT_LOCK_NODE);
    storageUnitMutex = new InterProcessMutex(client, STORAGE_UNIT_LOCK_NODE);
    reshardStatusMutex = new InterProcessMutex(client, RESHARD_STATUS_LOCK_NODE);
    reshardCounterMutex = new InterProcessMutex(client, RESHARD_COUNTER_LOCK_NODE);
    maxActiveEndTimeStatisticsMutex =
        new InterProcessMutex(client, ACTIVE_END_TIME_COUNTER_LOCK_NODE);
    fragmentRequestsCounterMutex = new InterProcessMutex(client, LATENCY_COUNTER_LOCK_NODE);
    fragmentHeatCounterMutex = new InterProcessMutex(client, FRAGMENT_HEAT_COUNTER_LOCK_NODE);
    timeseriesHeatCounterMutex = new InterProcessMutex(client, TIMESERIES_HEAT_COUNTER_LOCK_NODE);
  }

  public static ZooKeeperMetaStorage getInstance() {
    if (INSTANCE == null) {
      synchronized (ZooKeeperMetaStorage.class) {
        if (INSTANCE == null) {
          INSTANCE = new ZooKeeperMetaStorage();
        }
      }
    }
    return INSTANCE;
  }

  @Override
  public Map<String, Map<String, Integer>> loadSchemaMapping() throws MetaStorageException {
    InterProcessMutex mutex = new InterProcessMutex(client, SCHEMA_MAPPING_LOCK_NODE);
    try {
      mutex.acquire();
      Map<String, Map<String, Integer>> schemaMappings = new HashMap<>();
      if (client.checkExists().forPath(SCHEMA_MAPPING_PREFIX) == null) {
        // 当前还没有数据，创建父节点，然后不需要解析数据
        client.create().withMode(CreateMode.PERSISTENT).forPath(SCHEMA_MAPPING_PREFIX);
      } else {
        List<String> schemas = this.client.getChildren().forPath(SCHEMA_MAPPING_PREFIX);
        for (String schema : schemas) {
          Map<String, Integer> schemaMapping =
              JsonUtils.parseMap(
                  new String(this.client.getData().forPath(SCHEMA_MAPPING_PREFIX + "/" + schema)),
                  String.class,
                  Integer.class);
          schemaMappings.put(schema, schemaMapping);
        }
      }
      registerSchemaMappingListener();
      return schemaMappings;
    } catch (Exception e) {
      throw new MetaStorageException("get error when load schema mapping", e);
    } finally {
      try {
        mutex.release();
      } catch (Exception e) {
        throw new MetaStorageException(
            "get error when release interprocess lock for " + SCHEMA_MAPPING_LOCK_NODE, e);
      }
    }
  }

  private void registerSchemaMappingListener() throws Exception {
    this.schemaMappingsCache = new TreeCache(client, SCHEMA_MAPPING_PREFIX);
    TreeCacheListener listener =
        (curatorFramework, event) -> {
          if (schemaMappingChangeHook == null) {
            return;
          }
          if (event.getData() == null
              || event.getData().getPath() == null
              || event.getData().getPath().equals(SCHEMA_MAPPING_PREFIX)) {
            return; // 创建根节点，不必理会
          }
          byte[] data;
          Map<String, Integer> schemaMapping = null;
          String schema = event.getData().getPath().substring(SCHEMA_MAPPING_PREFIX.length());
          switch (event.getType()) {
            case NODE_ADDED:
            case NODE_UPDATED:
              data = event.getData().getData();
              schemaMapping = JsonUtils.parseMap(new String(data), String.class, Integer.class);
              break;
            case NODE_REMOVED:
            default:
              break;
          }
          schemaMappingChangeHook.onChange(schema, schemaMapping);
        };
    this.schemaMappingsCache.getListenable().addListener(listener);
    this.schemaMappingsCache.start();
  }

  @Override
  public void registerSchemaMappingChangeHook(SchemaMappingChangeHook hook) {
    this.schemaMappingChangeHook = hook;
  }

  @Override
  public void updateSchemaMapping(String schema, Map<String, Integer> schemaMapping)
      throws MetaStorageException {
    InterProcessMutex mutex = new InterProcessMutex(this.client, SCHEMA_MAPPING_LOCK_NODE);
    try {
      mutex.acquire();
      if (this.client.checkExists().forPath(SCHEMA_MAPPING_PREFIX + "/" + schema) == null) {
        if (schemaMapping == null) { // 待删除的数据不存在
          return;
        }
        // 创建新数据
        this.client
            .create()
            .forPath(SCHEMA_MAPPING_PREFIX + "/" + schema, JsonUtils.toJson(schemaMapping));
      } else {
        if (schemaMapping == null) { // 待删除的数据存在
          this.client.delete().forPath(SCHEMA_MAPPING_PREFIX + "/" + schema);
        }
        // 更新数据
        this.client
            .setData()
            .forPath(SCHEMA_MAPPING_PREFIX + "/" + schema, JsonUtils.toJson(schemaMapping));
      }
    } catch (Exception e) {
      throw new MetaStorageException("get error when update schemaMapping", e);
    } finally {
      try {
        mutex.release();
      } catch (Exception e) {
        throw new MetaStorageException(
            "get error when release interprocess lock for " + SCHEMA_MAPPING_LOCK_NODE, e);
      }
    }
  }

  @Override
  public Map<Long, IginxMeta> loadIginx() throws MetaStorageException {
    InterProcessMutex mutex = new InterProcessMutex(client, IGINX_LOCK_NODE);
    try {
      mutex.acquire();
      Map<Long, IginxMeta> iginxMetaMap = new HashMap<>();
      if (client.checkExists().forPath(IGINX_NODE_PREFIX) == null) {
        // 当前还没有数据，创建父节点，然后不需要解析数据
        client.create().withMode(CreateMode.PERSISTENT).forPath(IGINX_NODE_PREFIX);
      } else {
        List<String> children = client.getChildren().forPath(IGINX_NODE_PREFIX);
        for (String childName : children) {
          byte[] data = client.getData().forPath(IGINX_NODE_PREFIX + "/" + childName);
          IginxMeta iginxMeta = JsonUtils.fromJson(data, IginxMeta.class);
          if (iginxMeta == null) {
            LOGGER.error("resolve data from {}/{} error", IGINX_NODE_PREFIX, childName);
            continue;
          }
          iginxMetaMap.putIfAbsent(iginxMeta.getId(), iginxMeta);
        }
      }
      registerIginxListener();
      return iginxMetaMap;
    } catch (Exception e) {
      throw new MetaStorageException("get error when load iginx", e);
    } finally {
      try {
        mutex.release();
      } catch (Exception e) {
        throw new MetaStorageException(
            "get error when release interprocess lock for " + SCHEMA_MAPPING_LOCK_NODE, e);
      }
    }
  }

  @Override
  public long registerIginx(IginxMeta iginx) throws MetaStorageException {
    InterProcessMutex mutex = new InterProcessMutex(client, IGINX_LOCK_NODE);
    try {
      mutex.acquire();
      String nodeName =
          this.client
              .create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
              .forPath(IGINX_NODE, "".getBytes(StandardCharsets.UTF_8));
      long id = Long.parseLong(nodeName.substring(IGINX_NODE.length()));
      IginxMeta iginxMeta =
          new IginxMeta(id, iginx.getIp(), iginx.getPort(), iginx.getExtraParams());
      this.client.setData().forPath(nodeName, JsonUtils.toJson(iginxMeta));
      return id;
    } catch (Exception e) {
      throw new MetaStorageException("get error when load iginx", e);
    } finally {
      try {
        mutex.release();
      } catch (Exception e) {
        throw new MetaStorageException(
            "get error when release interprocess lock for " + IGINX_LOCK_NODE, e);
      }
    }
  }

  private void registerIginxListener() throws Exception {
    this.iginxCache = new TreeCache(this.client, IGINX_NODE_PREFIX);
    TreeCacheListener listener =
        (curatorFramework, event) -> {
          if (iginxChangeHook == null) {
            return;
          }
          byte[] data;
          IginxMeta iginxMeta;
          switch (event.getType()) {
            case NODE_UPDATED:
              data = event.getData().getData();
              iginxMeta = JsonUtils.fromJson(data, IginxMeta.class);
              if (iginxMeta != null) {
                LOGGER.info(
                    "new iginx comes to cluster: id = "
                        + iginxMeta.getId()
                        + " ,ip = "
                        + iginxMeta.getIp()
                        + " , port = "
                        + iginxMeta.getPort());
                iginxChangeHook.onChange(iginxMeta.getId(), iginxMeta);
              } else {
                LOGGER.error("resolve iginx meta from zookeeper error");
              }
              break;
            case NODE_REMOVED:
              data = event.getData().getData();
              String path = event.getData().getPath();
              LOGGER.info("node {} is removed", path);
              if (path.equals(IGINX_NODE_PREFIX)) {
                // 根节点被删除
                LOGGER.info("all iginx leave from cluster, iginx shutdown.");
                System.exit(1);
                break;
              }
              iginxMeta = JsonUtils.fromJson(data, IginxMeta.class);
              if (iginxMeta != null) {
                LOGGER.info(
                    "iginx leave from cluster: id = "
                        + iginxMeta.getId()
                        + " ,ip = "
                        + iginxMeta.getIp()
                        + " , port = "
                        + iginxMeta.getPort());
                iginxChangeHook.onChange(iginxMeta.getId(), null);
              } else {
                LOGGER.error("resolve iginx meta from zookeeper error");
              }
              break;
            default:
              break;
          }
        };
    this.iginxCache.getListenable().addListener(listener);
    this.iginxCache.start();
  }

  @Override
  public void registerIginxChangeHook(IginxChangeHook hook) {
    this.iginxChangeHook = hook;
  }

  @Override
  public Map<Long, StorageEngineMeta> loadStorageEngine(List<StorageEngineMeta> storageEngines)
      throws MetaStorageException {
    InterProcessMutex mutex = new InterProcessMutex(this.client, STORAGE_ENGINE_LOCK_NODE);
    try {
      mutex.acquire();
      if (this.client.checkExists().forPath(STORAGE_ENGINE_NODE_PREFIX)
          == null) { // 节点不存在，说明还没有别的 iginx 节点写入过元信息
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(STORAGE_ENGINE_NODE_PREFIX);
        for (StorageEngineMeta storageEngineMeta : storageEngines) {
          String nodeName =
              this.client
                  .create()
                  .creatingParentsIfNeeded()
                  .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                  .forPath(STORAGE_ENGINE_NODE, "".getBytes(StandardCharsets.UTF_8));
          long id = Long.parseLong(nodeName.substring(STORAGE_ENGINE_NODE.length()));
          storageEngineMeta.setId(id);
          this.client.setData().forPath(nodeName, JsonUtils.toJson(storageEngineMeta));
        }
      }
      // 注册监听器
      registerStorageEngineListener();
      // 加载数据
      Map<Long, StorageEngineMeta> storageEngineMetaMap = new HashMap<>();
      List<String> children = this.client.getChildren().forPath(STORAGE_ENGINE_NODE_PREFIX);
      for (String childName : children) {
        byte[] data = this.client.getData().forPath(STORAGE_ENGINE_NODE_PREFIX + "/" + childName);
        StorageEngineMeta storageEngineMeta = JsonUtils.fromJson(data, StorageEngineMeta.class);
        if (storageEngineMeta == null) {
          LOGGER.error(
              "resolve data from " + STORAGE_ENGINE_NODE_PREFIX + "/" + childName + " error");
          continue;
        }
        storageEngineMetaMap.putIfAbsent(storageEngineMeta.getId(), storageEngineMeta);
      }

      registerMaxActiveEndTimeStatisticsListener();
      registerReshardStatusListener();
      registerReshardCounterListener();
      return storageEngineMetaMap;
    } catch (Exception e) {
      throw new MetaStorageException("get error when load schema mapping", e);
    } finally {
      try {
        mutex.release();
      } catch (Exception e) {
        throw new MetaStorageException(
            "get error when release interprocess lock for " + STORAGE_ENGINE_LOCK_NODE, e);
      }
    }
  }

  @Override
  public long addStorageEngine(StorageEngineMeta storageEngine) throws MetaStorageException {
    InterProcessMutex mutex = new InterProcessMutex(this.client, STORAGE_ENGINE_LOCK_NODE);
    try {
      mutex.acquire();
      String nodeName =
          this.client
              .create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
              .forPath(STORAGE_ENGINE_NODE, "".getBytes(StandardCharsets.UTF_8));
      long id = Long.parseLong(nodeName.substring(STORAGE_ENGINE_NODE.length()));
      storageEngine.setId(id);
      this.client.setData().forPath(nodeName, JsonUtils.toJson(storageEngine));
      return id;
    } catch (Exception e) {
      throw new MetaStorageException("get error when add storage engine", e);
    } finally {
      try {
        mutex.release();
      } catch (Exception e) {
        throw new MetaStorageException(
            "get error when release interprocess lock for " + SCHEMA_MAPPING_LOCK_NODE, e);
      }
    }
  }

  @Override
  public void removeDummyStorageEngine(long storageEngineId) throws MetaStorageException {
    InterProcessMutex mutex = new InterProcessMutex(this.client, STORAGE_ENGINE_LOCK_NODE);
    try {
      mutex.acquire();
      String nodeName = generateId(STORAGE_ENGINE_NODE, storageEngineId);
      if (this.client.checkExists().forPath(nodeName) != null) {
        this.client.delete().forPath(nodeName);
      }
    } catch (Exception e) {
      throw new MetaStorageException("get error when removing dummy storage engine", e);
    } finally {
      try {
        mutex.release();
      } catch (Exception e) {
        throw new MetaStorageException(
            "get error when release interprocess lock for " + STORAGE_ENGINE_LOCK_NODE, e);
      }
    }
  }

  private void registerStorageEngineListener() throws Exception {
    this.storageEngineCache = new TreeCache(this.client, STORAGE_ENGINE_NODE_PREFIX);
    TreeCacheListener listener =
        (curatorFramework, event) -> {
          if (storageChangeHook == null) {
            return;
          }
          byte[] data;
          StorageEngineMeta storageEngineMeta;
          switch (event.getType()) {
            case NODE_ADDED:
            case NODE_UPDATED:
              if (event.getData().getPath().equals(STORAGE_ENGINE_NODE_PREFIX)) {
                break;
              }
              data = event.getData().getData();
              LOGGER.info("storage engine meta updated {}", event.getData().getPath());
              LOGGER.info("storage engine: {}", new String(data));
              storageEngineMeta = JsonUtils.fromJson(data, StorageEngineMeta.class);
              if (storageEngineMeta != null) {
                LOGGER.info(
                    "new storage engine comes to cluster: id = "
                        + storageEngineMeta.getId()
                        + " ,ip = "
                        + storageEngineMeta.getIp()
                        + " , port = "
                        + storageEngineMeta.getPort());
                storageChangeHook.onChange(storageEngineMeta.getId(), storageEngineMeta);
              } else {
                LOGGER.error("resolve storage engine from zookeeper error");
              }
              break;
            case NODE_REMOVED:
              data = event.getData().getData();
              String path = event.getData().getPath();
              LOGGER.info("node {} is removed", path);
              if (path.equals(IGINX_NODE_PREFIX)) {
                // 根节点被删除
                LOGGER.info("all iginx leave from cluster, iginx exits");
                System.exit(2);
                break;
              }
              storageEngineMeta = JsonUtils.fromJson(data, StorageEngineMeta.class);
              if (storageEngineMeta != null) {
                LOGGER.info(
                    "storage engine leave from cluster: id = "
                        + storageEngineMeta.getId()
                        + " ,ip = "
                        + storageEngineMeta.getIp()
                        + " , port = "
                        + storageEngineMeta.getPort());
                storageChangeHook.onChange(storageEngineMeta.getId(), null);
              } else {
                LOGGER.error("resolve storage engine from zookeeper error");
              }
              break;
            default:
              break;
          }
        };
    this.storageEngineCache.getListenable().addListener(listener);
    this.storageEngineCache.start();
  }

  @Override
  public void registerStorageChangeHook(StorageChangeHook hook) {
    this.storageChangeHook = hook;
  }

  @Override
  public Map<String, StorageUnitMeta> loadStorageUnit() throws MetaStorageException {
    try {
      Map<String, StorageUnitMeta> storageUnitMetaMap = new HashMap<>();
      if (this.client.checkExists().forPath(STORAGE_UNIT_NODE_PREFIX) == null) {
        // 当前还没有数据，创建父节点，然后不需要解析数据
        this.client.create().withMode(CreateMode.PERSISTENT).forPath(STORAGE_UNIT_NODE_PREFIX);
      } else {
        List<String> storageUnitIds = this.client.getChildren().forPath(STORAGE_UNIT_NODE_PREFIX);
        storageUnitIds.sort(String::compareTo);
        for (String storageUnitId : storageUnitIds) {
          LOGGER.info("load storage unit: {}", storageUnitId);
          byte[] data =
              this.client.getData().forPath(STORAGE_UNIT_NODE_PREFIX + "/" + storageUnitId);
          StorageUnitMeta storageUnitMeta = JsonUtils.fromJson(data, StorageUnitMeta.class);
          if (!storageUnitMeta.isMaster()) { // 需要加入到主节点的子节点列表中
            StorageUnitMeta masterStorageUnitMeta =
                storageUnitMetaMap.get(storageUnitMeta.getMasterId());
            if (masterStorageUnitMeta == null) { // 子节点先于主节点加入系统中，不应该发生，报错
              LOGGER.error(
                  "unexpected storage unit "
                      + new String(data)
                      + ", because it does not has a master storage unit");
            } else {
              masterStorageUnitMeta.addReplica(storageUnitMeta);
            }
          }
          storageUnitMetaMap.put(storageUnitMeta.getId(), storageUnitMeta);
        }
      }
      registerStorageUnitListener();
      return storageUnitMetaMap;
    } catch (Exception e) {
      throw new MetaStorageException("get error when load storage unit", e);
    }
  }

  @Override
  public void lockStorageUnit() throws MetaStorageException {
    try {
      storageUnitMutexLock.lock();
      storageUnitMutex.acquire();
    } catch (Exception e) {
      storageUnitMutexLock.unlock();
      throw new MetaStorageException("acquire storage unit mutex error: ", e);
    }
  }

  @Override
  public String addStorageUnit() throws MetaStorageException { // 只在有锁的情况下调用，内部不需要加锁
    try {
      String nodeName =
          this.client
              .create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
              .forPath(STORAGE_UNIT_NODE, "".getBytes(StandardCharsets.UTF_8));
      return nodeName.substring(STORAGE_UNIT_NODE_PREFIX.length() + 1);
    } catch (Exception e) {
      throw new MetaStorageException("add storage unit error: ", e);
    }
  }

  @Override
  public void updateStorageUnit(StorageUnitMeta storageUnitMeta)
      throws MetaStorageException { // 只在有锁的情况下调用，内部不需要加锁
    try {
      this.client
          .setData()
          .forPath(
              STORAGE_UNIT_NODE_PREFIX + "/" + storageUnitMeta.getId(),
              JsonUtils.toJson(storageUnitMeta));
    } catch (Exception e) {
      throw new MetaStorageException("add storage unit error: ", e);
    }
  }

  @Override
  public void releaseStorageUnit() throws MetaStorageException {
    try {
      storageUnitMutex.release();
    } catch (Exception e) {
      throw new MetaStorageException("release storage mutex error: ", e);
    } finally {
      storageUnitMutexLock.unlock();
    }
  }

  private void registerStorageUnitListener() throws Exception {
    this.storageUnitCache = new TreeCache(this.client, STORAGE_UNIT_NODE_PREFIX);
    TreeCacheListener listener =
        (curatorFramework, event) -> {
          if (storageUnitChangeHook == null) {
            return;
          }
          switch (event.getType()) {
            case NODE_ADDED:
            case NODE_UPDATED:
              byte[] data = event.getData().getData();
              if (event.getData().getPath().equals(STORAGE_UNIT_NODE_PREFIX)) {
                break;
              }
              StorageUnitMeta storageUnitMeta = JsonUtils.fromJson(data, StorageUnitMeta.class);
              if (storageUnitMeta != null) {
                LOGGER.info("new storage unit comes to cluster: id = {}", storageUnitMeta.getId());
                storageUnitChangeHook.onChange(storageUnitMeta.getId(), storageUnitMeta);
              }
              break;
            default:
              // TODO: case label. should we do nothing for other events?
              break;
          }
        };
    this.storageUnitCache.getListenable().addListener(listener);
    this.storageUnitCache.start();
  }

  @Override
  public void registerStorageUnitChangeHook(StorageUnitChangeHook hook) {
    this.storageUnitChangeHook = hook;
  }

  @Override
  public List<FragmentMeta> getFragmentListByColumnNameAndKeyInterval(
      String columnName, KeyInterval keyInterval) {
    try {
      List<String> columnsIntervalNames = this.client.getChildren().forPath(FRAGMENT_NODE_PREFIX);
      for (String columnsIntervalName : columnsIntervalNames) {
        ColumnsInterval columnsInterval = fromString(columnsIntervalName);
        if (columnsInterval.isContain(columnName)) {
          List<FragmentMeta> fragments = new ArrayList<>();
          List<String> keyIntervalNames =
              this.client.getChildren().forPath(FRAGMENT_NODE_PREFIX + "/" + columnsIntervalName);
          for (String keyIntervalName : keyIntervalNames) {
            if (Long.parseLong(keyIntervalName) >= keyInterval.getEndKey()) {
              break;
            }
            FragmentMeta fragmentMeta =
                JsonUtils.fromJson(
                    this.client
                        .getData()
                        .forPath(
                            FRAGMENT_NODE_PREFIX
                                + "/"
                                + columnsIntervalName
                                + "/"
                                + keyIntervalName),
                    FragmentMeta.class);
            if (fragmentMeta.getKeyInterval().getEndKey() > keyInterval.getStartKey()) {
              fragments.add(fragmentMeta);
            }
          }
          return fragments;
        }
      }
    } catch (Exception e) {
      LOGGER.error("get error when query fragment by columnName and keyInterval");
    }
    return new ArrayList<>();
  }

  @Override
  public Map<ColumnsInterval, List<FragmentMeta>> getFragmentMapByColumnsIntervalAndKeyInterval(
      ColumnsInterval columnsInterval, KeyInterval keyInterval) {
    try {
      List<String> columnsIntervalNames = this.client.getChildren().forPath(FRAGMENT_NODE_PREFIX);
      Map<ColumnsInterval, List<FragmentMeta>> fragmentMap = new HashMap<>();
      for (String columnsIntervalName : columnsIntervalNames) {
        ColumnsInterval fragmentColumnsInterval = fromString(columnsIntervalName);
        if (fragmentColumnsInterval.isIntersect(columnsInterval)) {
          List<FragmentMeta> fragments = new ArrayList<>();
          List<String> keyIntervalNames =
              this.client.getChildren().forPath(FRAGMENT_NODE_PREFIX + "/" + columnsIntervalName);
          for (String keyIntervalName : keyIntervalNames) {
            if (Long.parseLong(keyIntervalName) >= keyInterval.getEndKey()) {
              break;
            }
            FragmentMeta fragmentMeta =
                JsonUtils.fromJson(
                    this.client
                        .getData()
                        .forPath(
                            FRAGMENT_NODE_PREFIX
                                + "/"
                                + columnsIntervalName
                                + "/"
                                + keyIntervalName),
                    FragmentMeta.class);
            if (fragmentMeta.getKeyInterval().getEndKey() > keyInterval.getStartKey()) {
              fragments.add(fragmentMeta);
            }
          }
          fragmentMap.put(fragmentColumnsInterval, fragments);
        }
      }
      return fragmentMap;
    } catch (Exception e) {
      LOGGER.error("get error when query fragment by columnsInterval and keyInterval");
    }
    return new HashMap<>();
  }

  @Override
  public Map<ColumnsInterval, List<FragmentMeta>> loadFragment() throws MetaStorageException {
    try {
      Map<ColumnsInterval, List<FragmentMeta>> fragmentListMap = new HashMap<>();
      if (this.client.checkExists().forPath(FRAGMENT_NODE_PREFIX) == null) {
        // 当前还没有数据，创建父节点，然后不需要解析数据
        this.client.create().withMode(CreateMode.PERSISTENT).forPath(FRAGMENT_NODE_PREFIX);
      } else {
        List<String> columnsIntervalNames = this.client.getChildren().forPath(FRAGMENT_NODE_PREFIX);
        for (String columnsIntervalName : columnsIntervalNames) {
          ColumnsInterval columnsInterval = fromString(columnsIntervalName);
          List<FragmentMeta> fragmentMetaList = new ArrayList<>();
          List<String> keyIntervalNames =
              this.client.getChildren().forPath(FRAGMENT_NODE_PREFIX + "/" + columnsIntervalName);
          for (String keyIntervalName : keyIntervalNames) {
            FragmentMeta fragmentMeta =
                JsonUtils.fromJson(
                    this.client
                        .getData()
                        .forPath(
                            FRAGMENT_NODE_PREFIX
                                + "/"
                                + columnsIntervalName
                                + "/"
                                + keyIntervalName),
                    FragmentMeta.class);
            fragmentMetaList.add(fragmentMeta);
          }
          fragmentListMap.put(columnsInterval, fragmentMetaList);
        }
      }
      registerFragmentListener();
      return fragmentListMap;
    } catch (Exception e) {
      throw new MetaStorageException("get error when update fragment", e);
    }
  }

  private void registerFragmentListener() throws Exception {
    this.fragmentCache = new TreeCache(this.client, FRAGMENT_NODE_PREFIX);
    TreeCacheListener listener =
        (curatorFramework, event) -> {
          if (fragmentChangeHook == null) {
            return;
          }
          byte[] data;
          FragmentMeta fragmentMeta;
          switch (event.getType()) {
            case NODE_UPDATED:
              data = event.getData().getData();
              fragmentMeta = JsonUtils.fromJson(data, FragmentMeta.class);
              if (fragmentMeta != null) {
                fragmentChangeHook.onChange(false, fragmentMeta);
              } else {
                LOGGER.error("resolve fragment from zookeeper error");
              }
              break;
            case NODE_ADDED:
              String path = event.getData().getPath();
              String[] pathParts = path.split("/");
              if (pathParts.length == 4) {
                fragmentMeta = JsonUtils.fromJson(event.getData().getData(), FragmentMeta.class);
                if (fragmentMeta != null) {
                  fragmentChangeHook.onChange(true, fragmentMeta);
                } else {
                  LOGGER.error("resolve fragment from zookeeper error");
                }
              }
              break;
            default:
              break;
          }
        };
    this.fragmentCache.getListenable().addListener(listener);
    this.fragmentCache.start();
  }

  @Override
  public void lockFragment() throws MetaStorageException {
    try {
      fragmentMutexLock.lock();
      fragmentMutex.acquire();
    } catch (Exception e) {
      fragmentMutexLock.unlock();
      throw new MetaStorageException("acquire fragment mutex error: ", e);
    }
  }

  @Override
  public void updateFragment(FragmentMeta fragmentMeta)
      throws MetaStorageException { // 只在有锁的情况下调用，内部不需要加锁
    try {
      this.client
          .setData()
          .forPath(
              FRAGMENT_NODE_PREFIX
                  + "/"
                  + fragmentMeta.getColumnsInterval().toString()
                  + "/"
                  + fragmentMeta.getKeyInterval().toString(),
              JsonUtils.toJson(fragmentMeta));
    } catch (Exception e) {
      throw new MetaStorageException("get error when update fragment", e);
    }
  }

  @Override
  public void removeFragment(FragmentMeta fragmentMeta)
      throws MetaStorageException { // 只在有锁的情况下调用，内部不需要加锁
    try {
      this.client
          .delete()
          .forPath(
              FRAGMENT_NODE_PREFIX
                  + "/"
                  + fragmentMeta.getColumnsInterval().toString()
                  + "/"
                  + fragmentMeta.getKeyInterval().toString());
      // 没有子节点，删除父节点
      if (this.client
          .getChildren()
          .forPath(FRAGMENT_NODE_PREFIX + "/" + fragmentMeta.getColumnsInterval())
          .isEmpty()) {
        this.client
            .delete()
            .forPath(FRAGMENT_NODE_PREFIX + "/" + fragmentMeta.getColumnsInterval());
      }
      // 删除不需要的统计数据
      String requestWritePath =
          STATISTICS_FRAGMENT_REQUESTS_PREFIX_WRITE
              + "/"
              + fragmentMeta.getColumnsInterval().toString()
              + "/"
              + fragmentMeta.getKeyInterval().toString();
      if (this.client.checkExists().forPath(requestWritePath) != null) {
        this.client.delete().forPath(requestWritePath);
      }
      String requestReadPath =
          STATISTICS_FRAGMENT_REQUESTS_PREFIX_READ
              + "/"
              + fragmentMeta.getColumnsInterval().toString()
              + "/"
              + fragmentMeta.getKeyInterval().toString();
      if (this.client.checkExists().forPath(requestReadPath) != null) {
        this.client.delete().forPath(requestReadPath);
      }
      String pointsPath =
          STATISTICS_FRAGMENT_POINTS_PREFIX
              + "/"
              + fragmentMeta.getColumnsInterval().toString()
              + "/"
              + fragmentMeta.getKeyInterval().toString();
      if (this.client.checkExists().forPath(pointsPath) != null) {
        this.client.delete().forPath(pointsPath);
      }
    } catch (Exception e) {
      throw new MetaStorageException("get error when remove fragment", e);
    }
  }

  @Override
  public void addFragment(FragmentMeta fragmentMeta)
      throws MetaStorageException { // 只在有锁的情况下调用，内部不需要加锁
    try {
      this.client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(
              FRAGMENT_NODE_PREFIX
                  + "/"
                  + fragmentMeta.getColumnsInterval().toString()
                  + "/"
                  + fragmentMeta.getKeyInterval().toString(),
              JsonUtils.toJson(fragmentMeta));
    } catch (Exception e) {
      throw new MetaStorageException("get error when add fragment", e);
    }
  }

  @Override
  public void updateFragmentByColumnsInterval(
      ColumnsInterval columnsInterval, FragmentMeta fragmentMeta) throws MetaStorageException {
    try {
      this.client
          .delete()
          .forPath(
              FRAGMENT_NODE_PREFIX
                  + "/"
                  + columnsInterval.toString()
                  + "/"
                  + fragmentMeta.getKeyInterval().toString());
      List<String> keyIntervalNames =
          this.client
              .getChildren()
              .forPath(FRAGMENT_NODE_PREFIX + "/" + columnsInterval.toString());
      if (keyIntervalNames.isEmpty()) {
        this.client.delete().forPath(FRAGMENT_NODE_PREFIX + "/" + columnsInterval.toString());
      }
      this.client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(
              FRAGMENT_NODE_PREFIX
                  + "/"
                  + fragmentMeta.getColumnsInterval().toString()
                  + "/"
                  + fragmentMeta.getKeyInterval().toString(),
              JsonUtils.toJson(fragmentMeta));
    } catch (Exception e) {
      throw new MetaStorageException("get error when update fragment", e);
    }
  }

  @Override
  public void releaseFragment() throws MetaStorageException {
    try {
      fragmentMutex.release();
    } catch (Exception e) {
      throw new MetaStorageException("release fragment mutex error: ", e);
    } finally {
      fragmentMutexLock.unlock();
    }
  }

  @Override
  public void registerFragmentChangeHook(FragmentChangeHook hook) {
    this.fragmentChangeHook = hook;
  }

  private void registerUserListener() throws Exception {
    this.userCache = new TreeCache(this.client, USER_NODE_PREFIX);
    TreeCacheListener listener =
        (curatorFramework, event) -> {
          if (userChangeHook == null) {
            return;
          }
          UserMeta userMeta;
          switch (event.getType()) {
            case NODE_ADDED:
            case NODE_UPDATED:
              if (event.getData().getPath().equals(USER_NODE_PREFIX)) {
                return; // 前缀事件，非含数据的节点的变化，不需要处理
              }
              userMeta = JsonUtils.fromJson(event.getData().getData(), UserMeta.class);
              if (userMeta != null) {
                userChangeHook.onChange(userMeta.getUsername(), userMeta);
              } else {
                LOGGER.error("resolve user from zookeeper error");
              }
              break;
            case NODE_REMOVED:
              String path = event.getData().getPath();
              String[] pathParts = path.split("/");
              String username = pathParts[pathParts.length - 1];
              userChangeHook.onChange(username, null);
              break;
            default:
              break;
          }
        };
    this.userCache.getListenable().addListener(listener);
    this.userCache.start();
  }

  @Override
  public List<UserMeta> loadUser(UserMeta userMeta) throws MetaStorageException {
    InterProcessMutex mutex = new InterProcessMutex(this.client, USER_LOCK_NODE);
    try {
      mutex.acquire();
      if (this.client.checkExists().forPath(USER_NODE_PREFIX) == null) { // 节点不存在，说明系统中第一个用户还没有被创建
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(USER_NODE_PREFIX + "/" + userMeta.getUsername(), JsonUtils.toJson(userMeta));
      }
      List<UserMeta> users = new ArrayList<>();
      List<String> usernames = this.client.getChildren().forPath(USER_NODE_PREFIX);
      for (String username : usernames) {
        byte[] data = this.client.getData().forPath(USER_NODE_PREFIX + "/" + username);
        UserMeta user = JsonUtils.fromJson(data, UserMeta.class);
        if (user == null) {
          LOGGER.error("resolve data from {}/{} error", USER_NODE_PREFIX, username);
          continue;
        }
        users.add(user);
      }
      registerUserListener();
      return users;
    } catch (Exception e) {
      throw new MetaStorageException("get error when load user", e);
    } finally {
      try {
        mutex.release();
      } catch (Exception e) {
        throw new MetaStorageException(
            "get error when release interprocess lock for " + USER_LOCK_NODE, e);
      }
    }
  }

  @Override
  public void registerUserChangeHook(UserChangeHook hook) {
    this.userChangeHook = hook;
  }

  @Override
  public void addUser(UserMeta userMeta) throws MetaStorageException {
    InterProcessMutex mutex = new InterProcessMutex(this.client, USER_LOCK_NODE);
    try {
      mutex.acquire();
      this.client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(USER_NODE_PREFIX + "/" + userMeta.getUsername(), JsonUtils.toJson(userMeta));
    } catch (Exception e) {
      throw new MetaStorageException("get error when add user", e);
    } finally {
      try {
        mutex.release();
      } catch (Exception e) {
        throw new MetaStorageException(
            "get error when release interprocess lock for " + USER_LOCK_NODE, e);
      }
    }
  }

  @Override
  public void updateUser(UserMeta userMeta) throws MetaStorageException {
    InterProcessMutex mutex = new InterProcessMutex(this.client, USER_LOCK_NODE);
    try {
      mutex.acquire();
      this.client
          .setData()
          .forPath(USER_NODE_PREFIX + "/" + userMeta.getUsername(), JsonUtils.toJson(userMeta));
    } catch (Exception e) {
      throw new MetaStorageException("get error when update user", e);
    } finally {
      try {
        mutex.release();
      } catch (Exception e) {
        throw new MetaStorageException(
            "get error when release interprocess lock for " + USER_LOCK_NODE, e);
      }
    }
  }

  @Override
  public void removeUser(String username) throws MetaStorageException {
    InterProcessMutex mutex = new InterProcessMutex(this.client, USER_LOCK_NODE);
    try {
      mutex.acquire();
      this.client.delete().forPath(USER_NODE_PREFIX + "/" + username);
    } catch (Exception e) {
      throw new MetaStorageException("get error when remove user", e);
    } finally {
      try {
        mutex.release();
      } catch (Exception e) {
        throw new MetaStorageException(
            "get error when release interprocess lock for " + USER_LOCK_NODE, e);
      }
    }
  }

  @Override
  public void registerTimeseriesChangeHook(TimeSeriesChangeHook hook) {
    this.timeSeriesChangeHook = hook;
  }

  @Override
  public void registerVersionChangeHook(VersionChangeHook hook) {
    this.versionChangeHook = hook;
  }

  @Override
  public boolean election() {
    if (isMaster) {
      return true;
    }
    try {
      this.client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.EPHEMERAL)
          .forPath(POLICY_LEADER);
      LOGGER.info("成功");
      isMaster = true;
    } catch (KeeperException.NodeExistsException e) {
      LOGGER.info("失败");
      isMaster = false;
    } finally {
      // TODO: this will cause exception lost! Can we catch all exception instead?
      return isMaster;
    }
  }

  @Override
  public void updateTimeseriesData(Map<String, Double> timeseriesData, long iginxid, long version)
      throws Exception {
    InterProcessMutex mutex = new InterProcessMutex(this.client, POLICY_LOCK_NODE);
    try {
      mutex.acquire();
      if (this.client.checkExists().forPath(TIMESERIES_NODE_PREFIX + "/" + iginxid) == null) {
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(TIMESERIES_NODE_PREFIX + "/" + iginxid, tobytes(timeseriesData));
      } else {
        this.client
            .setData()
            .forPath(TIMESERIES_NODE_PREFIX + "/" + iginxid, tobytes(timeseriesData));
      }
      this.client
          .setData()
          .forPath(POLICY_NODE_PREFIX + "/" + iginxid, String.valueOf(version).getBytes());
    } catch (Exception e) {
      throw new MetaStorageException("get error when update timeseries", e);
    } finally {
      try {
        mutex.release();
      } catch (Exception e) {
        throw new MetaStorageException(
            "get error when release interprocess lock for " + POLICY_LOCK_NODE, e);
      }
    }
  }

  @Override
  public Map<String, Double> getColumnsData() {
    Map<String, Double> ret = new HashMap<>();
    try {
      Set<Integer> idSet =
          loadIginx().keySet().stream().map(Long::intValue).collect(Collectors.toSet());
      List<String> children = client.getChildren().forPath(TIMESERIES_NODE_PREFIX);
      for (String child : children) {
        byte[] data = this.client.getData().forPath(TIMESERIES_NODE_PREFIX + "/" + child);
        if (!idSet.contains(Integer.parseInt(child))) {
          continue;
        }
        Map<String, Double> tmp = toMap(data);
        if (tmp == null) {
          LOGGER.error("resolve data from {}/{} error", TIMESERIES_NODE_PREFIX, child);
          continue;
        }
        tmp.forEach(
            (timeseries, value) -> {
              double now = 0.0;
              if (ret.containsKey(timeseries)) {
                now = ret.get(timeseries);
              }
              ret.put(timeseries, now + value);
            });
      }
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
    }
    return ret;
  }

  private byte[] tobytes(Map<String, Double> prefix) {
    StringBuilder ret = new StringBuilder();
    for (Map.Entry<String, Double> entry : prefix.entrySet()) {
      ret.append(entry.getKey());
      ret.append("~");
      ret.append(entry.getValue());
      ret.append("^");
    }
    if (ret.length() != 0 && ret.charAt(ret.length() - 1) == '^') {
      ret.deleteCharAt(ret.length() - 1);
    }
    LOGGER.info(ret.toString());
    return ret.toString().getBytes();
  }

  private Map<String, Double> toMap(byte[] prefix) {
    LOGGER.info(new String(prefix));
    Map<String, Double> ret = new HashMap<>();
    String str = new String(prefix);
    String[] tmp = str.split("\\^");
    for (String entry : Arrays.asList(tmp)) {
      String[] tmp2 = entry.split("~");
      try {
        ret.put(tmp2[0], Double.parseDouble(tmp2[1]));
      } catch (Exception e) {
        LOGGER.error(entry);
      }
    }
    return ret;
  }

  @Override
  public void registerPolicy(long iginxId, int num) throws Exception {
    InterProcessMutex mutex = new InterProcessMutex(this.client, POLICY_LOCK_NODE);
    try {
      mutex.acquire();
      if (client.checkExists().forPath(POLICY_NODE_PREFIX) == null) {
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(POLICY_NODE_PREFIX);
      }

      if (client.checkExists().forPath(TIMESERIES_NODE_PREFIX) == null) {
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(TIMESERIES_NODE_PREFIX);
      }
      if (client.checkExists().forPath(POLICY_VERSION) == null) {
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(POLICY_VERSION);

        this.client.setData().forPath(POLICY_VERSION, ("0" + "\t" + num).getBytes());
      }

      this.client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(POLICY_NODE_PREFIX + "/" + iginxId);

      this.client.setData().forPath(POLICY_NODE_PREFIX + "/" + iginxId, "0".getBytes());

      this.client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(TIMESERIES_NODE_PREFIX + "/" + iginxId);

      this.policyCache = new TreeCache(this.client, POLICY_LEADER);
      TreeCacheListener listener =
          (curatorFramework, event) -> {
            switch (event.getType()) {
              case NODE_REMOVED:
                election();
                break;
              default:
                break;
            }
          };
      this.policyCache.getListenable().addListener(listener);
      this.policyCache.start();

      this.timeseriesCache = new TreeCache(this.client, POLICY_NODE_PREFIX);
      TreeCacheListener listener2 =
          (curatorFramework, event) -> {
            switch (event.getType()) {
              case NODE_ADDED:
              case NODE_UPDATED:
                String path = event.getData().getPath();
                String[] pathParts = path.split("/");
                int version = -1;
                int node = -1;
                if (pathParts.length == 2 && isNumeric(pathParts[1])) {
                  version = Integer.parseInt(new String(event.getData().getData()));
                  node = Integer.parseInt(pathParts[1]);
                }
                if (version != -1 && node != -1) {
                  timeSeriesChangeHook.onChange(node, version);
                }
                break;
              default:
                break;
            }
          };
      this.timeseriesCache.getListenable().addListener(listener2);
      this.timeseriesCache.start();

      this.versionCache = new TreeCache(this.client, POLICY_VERSION);
      TreeCacheListener listener3 =
          (curatorFramework, event) -> {
            switch (event.getType()) {
              case NODE_UPDATED:
                String[] str = new String(event.getData().getData()).split("\t");
                int version = Integer.parseInt(str[0]);
                int newNum = Integer.parseInt(str[1]);
                if (version > 0) {
                  versionChangeHook.onChange(version, newNum);
                } else {
                  LOGGER.error("resolve prefix from zookeeper error");
                }
                break;
              default:
                break;
            }
          };
      this.versionCache.getListenable().addListener(listener3);
      this.versionCache.start();

    } catch (Exception e) {
      throw new MetaStorageException("get error when init policy", e);
    } finally {
      try {
        mutex.release();
      } catch (Exception e) {
        throw new MetaStorageException(
            "get error when release interprocess lock for " + POLICY_LOCK_NODE, e);
      }
    }
  }

  @Override
  public int updateVersion() {
    int version = -1;
    try {
      version =
          Integer.parseInt(new String(client.getData().forPath(POLICY_VERSION)).split("\t")[0]);
      this.client.setData().forPath(POLICY_VERSION, ((version + 1) + "\t" + "0").getBytes());
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
    }
    return version + 1;
  }

  @Override
  public void registerTransformChangeHook(TransformChangeHook hook) {
    this.transformChangeHook = hook;
  }

  @Override
  public List<TransformTaskMeta> loadTransformTask() throws MetaStorageException {
    InterProcessMutex mutex = new InterProcessMutex(this.client, TRANSFORM_LOCK_NODE);
    try {
      mutex.acquire();
      List<TransformTaskMeta> tasks = new ArrayList<>();
      if (this.client.checkExists().forPath(TRANSFORM_NODE_PREFIX) == null) {
        // 当前还没有数据，创建父节点，然后不需要解析数据
        client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(TRANSFORM_NODE_PREFIX);
      } else {
        List<String> classNames = this.client.getChildren().forPath(TRANSFORM_NODE_PREFIX);
        for (String className : classNames) {
          byte[] data = this.client.getData().forPath(TRANSFORM_NODE_PREFIX + "/" + className);
          TransformTaskMeta task = JsonUtils.fromJson(data, TransformTaskMeta.class);
          if (task == null) {
            LOGGER.error("resolve data from {}/{} error", TRANSFORM_NODE_PREFIX, className);
            continue;
          }
          tasks.add(task);
        }
      }
      registerTransformListener();
      return tasks;
    } catch (Exception e) {
      throw new MetaStorageException("get error when load transform tasks", e);
    } finally {
      try {
        mutex.release();
      } catch (Exception e) {
        throw new MetaStorageException(
            "get error when release interprocess lock for " + TRANSFORM_LOCK_NODE, e);
      }
    }
  }

  private void registerTransformListener() throws Exception {
    this.transformCache = new TreeCache(this.client, TRANSFORM_NODE_PREFIX);
    TreeCacheListener listener =
        (curatorFramework, event) -> {
          if (transformChangeHook == null) {
            return;
          }
          TransformTaskMeta taskMeta;
          switch (event.getType()) {
            case NODE_ADDED:
            case NODE_UPDATED:
              if (event.getData() == null
                  || event.getData().getPath() == null
                  || event.getData().getPath().equals(TRANSFORM_NODE_PREFIX)) {
                return; // 前缀事件，非含数据的节点的变化，不需要处理
              }
              taskMeta = JsonUtils.fromJson(event.getData().getData(), TransformTaskMeta.class);
              if (taskMeta != null) {
                transformChangeHook.onChange(taskMeta.getName(), taskMeta);
              } else {
                LOGGER.error("resolve transform task from zookeeper error");
              }
              break;
            case NODE_REMOVED:
              String path = event.getData().getPath();
              String[] pathParts = path.split("/");
              String className = pathParts[pathParts.length - 1];
              transformChangeHook.onChange(className, null);
              break;
            default:
              break;
          }
        };
    this.transformCache.getListenable().addListener(listener);
    this.transformCache.start();
  }

  @Override
  public void addTransformTask(TransformTaskMeta transformTask) throws MetaStorageException {
    InterProcessMutex mutex = new InterProcessMutex(this.client, TRANSFORM_LOCK_NODE);
    try {
      mutex.acquire();
      this.client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(
              TRANSFORM_NODE_PREFIX + "/" + transformTask.getName(),
              JsonUtils.toJson(transformTask));
    } catch (Exception e) {
      throw new MetaStorageException("get error when add transform task", e);
    } finally {
      try {
        mutex.release();
      } catch (Exception e) {
        throw new MetaStorageException(
            "get error when release interprocess lock for " + TRANSFORM_LOCK_NODE, e);
      }
    }
  }

  @Override
  public void updateTransformTask(TransformTaskMeta transformTask) throws MetaStorageException {
    InterProcessMutex mutex = new InterProcessMutex(this.client, TRANSFORM_LOCK_NODE);
    try {
      mutex.acquire();
      this.client
          .setData()
          .forPath(
              TRANSFORM_NODE_PREFIX + "/" + transformTask.getName(),
              JsonUtils.toJson(transformTask));
    } catch (Exception e) {
      throw new MetaStorageException("get error when update transform task", e);
    } finally {
      try {
        mutex.release();
      } catch (Exception e) {
        throw new MetaStorageException(
            "get error when release interprocess lock for " + TRANSFORM_LOCK_NODE, e);
      }
    }
  }

  @Override
  public void dropTransformTask(String name) throws MetaStorageException {
    InterProcessMutex mutex = new InterProcessMutex(this.client, TRANSFORM_LOCK_NODE);
    try {
      mutex.acquire();
      this.client.delete().forPath(TRANSFORM_NODE_PREFIX + "/" + name);
    } catch (Exception e) {
      throw new MetaStorageException("get error when drop transform task", e);
    } finally {
      try {
        mutex.release();
      } catch (Exception e) {
        throw new MetaStorageException(
            "get error when release interprocess lock for " + TRANSFORM_LOCK_NODE, e);
      }
    }
  }

  @Override
  public void updateTimeseriesLoad(Map<String, Long> timeseriesLoadMap) throws Exception {
    for (Entry<String, Long> timeseriesLoadEntry : timeseriesLoadMap.entrySet()) {
      String path = STATISTICS_TIMESERIES_HEAT_PREFIX + "/" + timeseriesLoadEntry.getKey();
      if (this.client.checkExists().forPath(path) == null) {
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(path, JsonUtils.toJson(timeseriesLoadEntry.getValue()));
      }
      byte[] data = this.client.getData().forPath(path);
      long heat = JsonUtils.fromJson(data, Long.class);
      this.client.setData().forPath(path, JsonUtils.toJson(heat + timeseriesLoadEntry.getValue()));
    }
  }

  @Override
  public Map<String, Long> loadTimeseriesHeat() throws Exception {
    Map<String, Long> timeseriesHeatMap = new HashMap<>();
    List<String> children = client.getChildren().forPath(STATISTICS_TIMESERIES_HEAT_PREFIX);
    for (String child : children) {
      byte[] data = this.client.getData().forPath(STATISTICS_TIMESERIES_HEAT_PREFIX + "/" + child);
      long heat = JsonUtils.fromJson(data, Long.class);
      timeseriesHeatMap.put(child, heat);
    }
    return timeseriesHeatMap;
  }

  @Override
  public void removeTimeseriesHeat() throws MetaStorageException {
    try {
      if (this.client.checkExists().forPath(STATISTICS_TIMESERIES_HEAT_PREFIX) != null) {
        this.client.delete().deletingChildrenIfNeeded().forPath(STATISTICS_TIMESERIES_HEAT_PREFIX);
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when removing timeseries heat: ", e);
    }
  }

  @Override
  public void lockTimeseriesHeatCounter() throws MetaStorageException {
    try {
      timeseriesHeatCounterMutexLock.lock();
      timeseriesHeatCounterMutex.acquire();
    } catch (Exception e) {
      timeseriesHeatCounterMutexLock.unlock();
      throw new MetaStorageException(
          "encounter error when acquiring timeseries heat counter mutex: ", e);
    }
  }

  @Override
  public void incrementTimeseriesHeatCounter() throws MetaStorageException {
    try {
      if (this.client.checkExists().forPath(STATISTICS_TIMESERIES_HEAT_COUNTER_PREFIX) == null) {
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(STATISTICS_TIMESERIES_HEAT_COUNTER_PREFIX, JsonUtils.toJson(1));
      } else {
        int counter =
            JsonUtils.fromJson(
                this.client.getData().forPath(STATISTICS_TIMESERIES_HEAT_COUNTER_PREFIX),
                Integer.class);
        this.client
            .setData()
            .forPath(STATISTICS_TIMESERIES_HEAT_COUNTER_PREFIX, JsonUtils.toJson(counter + 1));
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when updating timeseries heat counter: ", e);
    }
  }

  @Override
  public void resetTimeseriesHeatCounter() throws MetaStorageException {
    try {
      if (this.client.checkExists().forPath(STATISTICS_TIMESERIES_HEAT_COUNTER_PREFIX) == null) {
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(STATISTICS_TIMESERIES_HEAT_COUNTER_PREFIX, JsonUtils.toJson(0));
      } else {
        this.client
            .setData()
            .forPath(STATISTICS_TIMESERIES_HEAT_COUNTER_PREFIX, JsonUtils.toJson(0));
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when resetting timeseries heat counter: ", e);
    }
  }

  @Override
  public void releaseTimeseriesHeatCounter() throws MetaStorageException {
    try {
      timeseriesHeatCounterMutex.release();
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when releasing timeseries heat mutex: ", e);
    } finally {
      timeseriesHeatCounterMutexLock.unlock();
    }
  }

  @Override
  public int getTimeseriesHeatCounter() throws MetaStorageException {
    try {
      if (this.client.checkExists().forPath(STATISTICS_TIMESERIES_HEAT_COUNTER_PREFIX) == null) {
        return 0;
      } else {
        return JsonUtils.fromJson(
            this.client.getData().forPath(STATISTICS_TIMESERIES_HEAT_COUNTER_PREFIX),
            Integer.class);
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when get timeseries heat counter: ", e);
    }
  }

  @Override
  public void updateFragmentRequests(
      Map<FragmentMeta, Long> writeRequestsMap, Map<FragmentMeta, Long> readRequestsMap)
      throws Exception {
    for (Entry<FragmentMeta, Long> writeRequestsEntry : writeRequestsMap.entrySet()) {
      if (writeRequestsEntry.getValue() > 0) {
        String requestsPath =
            STATISTICS_FRAGMENT_REQUESTS_PREFIX_WRITE
                + "/"
                + writeRequestsEntry.getKey().getColumnsInterval().toString()
                + "/"
                + writeRequestsEntry.getKey().getKeyInterval().toString();
        String pointsPath =
            STATISTICS_FRAGMENT_POINTS_PREFIX
                + "/"
                + writeRequestsEntry.getKey().getColumnsInterval().toString()
                + "/"
                + writeRequestsEntry.getKey().getKeyInterval().toString();
        if (this.client.checkExists().forPath(requestsPath) == null) {
          this.client
              .create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.PERSISTENT)
              .forPath(requestsPath, JsonUtils.toJson(writeRequestsEntry.getValue()));
        }
        byte[] data = this.client.getData().forPath(requestsPath);
        long requests = JsonUtils.fromJson(data, Long.class);
        this.client
            .setData()
            .forPath(requestsPath, JsonUtils.toJson(requests + writeRequestsEntry.getValue()));

        if (this.client.checkExists().forPath(pointsPath) == null) {
          this.client
              .create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.PERSISTENT)
              .forPath(pointsPath, JsonUtils.toJson(writeRequestsEntry.getValue()));
        }
        data = this.client.getData().forPath(pointsPath);
        long points = JsonUtils.fromJson(data, Long.class);
        this.client
            .setData()
            .forPath(pointsPath, JsonUtils.toJson(points + writeRequestsEntry.getValue()));
      }
    }
    for (Entry<FragmentMeta, Long> readRequestsEntry : readRequestsMap.entrySet()) {
      String path =
          STATISTICS_FRAGMENT_REQUESTS_PREFIX_READ
              + "/"
              + readRequestsEntry.getKey().getColumnsInterval().toString()
              + "/"
              + readRequestsEntry.getKey().getKeyInterval().toString();
      if (this.client.checkExists().forPath(path) == null) {
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(path, JsonUtils.toJson(readRequestsEntry.getValue()));
      }
      byte[] data = this.client.getData().forPath(path);
      long requests = JsonUtils.fromJson(data, Long.class);
      this.client
          .setData()
          .forPath(path, JsonUtils.toJson(requests + readRequestsEntry.getValue()));
    }
  }

  @Override
  public Map<FragmentMeta, Long> loadFragmentPoints(IMetaCache cache) throws Exception {
    Map<FragmentMeta, Long> writePointsMap = new HashMap<>();
    if (this.client.checkExists().forPath(STATISTICS_FRAGMENT_POINTS_PREFIX) != null) {
      List<String> children = client.getChildren().forPath(STATISTICS_FRAGMENT_POINTS_PREFIX);
      for (String child : children) {
        ColumnsInterval columnsInterval = fromString(child);
        List<FragmentMeta> fragmentMetas =
            cache.getFragmentMapByExactColumnsInterval(columnsInterval);

        List<String> keyIntervals =
            client.getChildren().forPath(STATISTICS_FRAGMENT_POINTS_PREFIX + "/" + child);

        for (String keyInterval : keyIntervals) {
          long startTime = Long.parseLong(keyInterval);
          for (FragmentMeta fragmentMeta : fragmentMetas) {
            if (fragmentMeta.getKeyInterval().getStartKey() == startTime) {
              byte[] data =
                  this.client
                      .getData()
                      .forPath(STATISTICS_FRAGMENT_POINTS_PREFIX + "/" + child + "/" + keyInterval);
              long points = JsonUtils.fromJson(data, Long.class);
              writePointsMap.put(fragmentMeta, points);
            }
          }
        }
      }
    }
    return writePointsMap;
  }

  @Override
  public void deleteFragmentPoints(ColumnsInterval columnsInterval, KeyInterval keyInterval)
      throws Exception {
    String path =
        STATISTICS_FRAGMENT_POINTS_PREFIX
            + "/"
            + columnsInterval.toString()
            + "/"
            + keyInterval.toString();
    if (this.client.checkExists().forPath(path) != null) {
      this.client.delete().forPath(path);
    }
  }

  @Override
  public void updateFragmentPoints(FragmentMeta fragmentMeta, long points) throws Exception {
    String path =
        STATISTICS_FRAGMENT_POINTS_PREFIX
            + "/"
            + fragmentMeta.getColumnsInterval().toString()
            + "/"
            + fragmentMeta.getKeyInterval().toString();
    if (this.client.checkExists().forPath(path) == null) {
      this.client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(path, JsonUtils.toJson(points));
    } else {
      this.client.setData().forPath(path, JsonUtils.toJson(points));
    }
  }

  @Override
  public void removeFragmentRequests() throws MetaStorageException {
    try {
      if (this.client.checkExists().forPath(STATISTICS_FRAGMENT_REQUESTS_PREFIX_WRITE) != null) {
        this.client
            .delete()
            .deletingChildrenIfNeeded()
            .forPath(STATISTICS_FRAGMENT_REQUESTS_PREFIX_WRITE);
      }
      if (this.client.checkExists().forPath(STATISTICS_FRAGMENT_REQUESTS_PREFIX_READ) != null) {
        this.client
            .delete()
            .deletingChildrenIfNeeded()
            .forPath(STATISTICS_FRAGMENT_REQUESTS_PREFIX_READ);
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when removing fragment requests: ", e);
    }
  }

  @Override
  public void lockFragmentRequestsCounter() throws MetaStorageException {
    try {
      fragmentRequestsCounterMutexLock.lock();
      fragmentRequestsCounterMutex.acquire();
    } catch (Exception e) {
      fragmentRequestsCounterMutexLock.unlock();
      throw new MetaStorageException(
          "encounter error when acquiring fragment requests counter mutex: ", e);
    }
  }

  @Override
  public void incrementFragmentRequestsCounter() throws MetaStorageException {
    try {
      if (this.client.checkExists().forPath(STATISTICS_FRAGMENT_REQUESTS_COUNTER_PREFIX) == null) {
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(STATISTICS_FRAGMENT_REQUESTS_COUNTER_PREFIX, JsonUtils.toJson(1));
      } else {
        int counter =
            JsonUtils.fromJson(
                this.client.getData().forPath(STATISTICS_FRAGMENT_REQUESTS_COUNTER_PREFIX),
                Integer.class);
        this.client
            .setData()
            .forPath(STATISTICS_FRAGMENT_REQUESTS_COUNTER_PREFIX, JsonUtils.toJson(counter + 1));
      }
    } catch (Exception e) {
      throw new MetaStorageException(
          "encounter error when updating fragment requests counter: ", e);
    }
  }

  @Override
  public void resetFragmentRequestsCounter() throws MetaStorageException {
    try {
      if (this.client.checkExists().forPath(STATISTICS_FRAGMENT_REQUESTS_COUNTER_PREFIX) == null) {
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(STATISTICS_FRAGMENT_REQUESTS_COUNTER_PREFIX, JsonUtils.toJson(0));
      } else {
        this.client
            .setData()
            .forPath(STATISTICS_FRAGMENT_REQUESTS_COUNTER_PREFIX, JsonUtils.toJson(0));
      }
    } catch (Exception e) {
      throw new MetaStorageException(
          "encounter error when resetting fragment requests counter: ", e);
    }
  }

  @Override
  public void releaseFragmentRequestsCounter() throws MetaStorageException {
    try {
      fragmentRequestsCounterMutex.release();
    } catch (Exception e) {
      throw new MetaStorageException(
          "encounter error when releasing fragment requests counter mutex: ", e);
    } finally {
      fragmentRequestsCounterMutexLock.unlock();
    }
  }

  @Override
  public int getFragmentRequestsCounter() throws MetaStorageException {
    try {
      if (this.client.checkExists().forPath(STATISTICS_FRAGMENT_REQUESTS_COUNTER_PREFIX) == null) {
        return 0;
      } else {
        return JsonUtils.fromJson(
            this.client.getData().forPath(STATISTICS_FRAGMENT_REQUESTS_COUNTER_PREFIX),
            Integer.class);
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when get fragment requests counter: ", e);
    }
  }

  @Override
  public void updateFragmentHeat(
      Map<FragmentMeta, Long> writeHotspotMap, Map<FragmentMeta, Long> readHotspotMap)
      throws Exception {
    for (Entry<FragmentMeta, Long> writeHotspotEntry : writeHotspotMap.entrySet()) {
      String path =
          STATISTICS_FRAGMENT_HEAT_PREFIX_WRITE
              + "/"
              + writeHotspotEntry.getKey().getColumnsInterval().toString()
              + "/"
              + writeHotspotEntry.getKey().getKeyInterval().toString();
      if (this.client.checkExists().forPath(path) == null) {
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(path, JsonUtils.toJson(writeHotspotEntry.getValue()));
      } else {
        byte[] data = this.client.getData().forPath(path);
        long heat = JsonUtils.fromJson(data, Long.class);
        this.client.setData().forPath(path, JsonUtils.toJson(heat + writeHotspotEntry.getValue()));
      }
    }
    for (Entry<FragmentMeta, Long> readHotspotEntry : readHotspotMap.entrySet()) {
      String path =
          STATISTICS_FRAGMENT_HEAT_PREFIX_READ
              + "/"
              + readHotspotEntry.getKey().getColumnsInterval().toString()
              + "/"
              + readHotspotEntry.getKey().getKeyInterval().toString();
      if (this.client.checkExists().forPath(path) == null) {
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(path, JsonUtils.toJson(readHotspotEntry.getValue()));
      } else {
        byte[] data = this.client.getData().forPath(path);
        long heat = JsonUtils.fromJson(data, Long.class);
        this.client.setData().forPath(path, JsonUtils.toJson(heat + readHotspotEntry.getValue()));
      }
    }
  }

  @Override
  public Pair<Map<FragmentMeta, Long>, Map<FragmentMeta, Long>> loadFragmentHeat(IMetaCache cache)
      throws Exception {
    Map<FragmentMeta, Long> writeHotspotMap = new HashMap<>();
    if (this.client.checkExists().forPath(STATISTICS_FRAGMENT_HEAT_PREFIX_WRITE) != null) {
      List<String> children = client.getChildren().forPath(STATISTICS_FRAGMENT_HEAT_PREFIX_WRITE);
      for (String child : children) {
        ColumnsInterval columnsInterval = fromString(child);
        Map<ColumnsInterval, List<FragmentMeta>> fragmentMapOfTimeSeriesInterval =
            cache.getFragmentMapByColumnsInterval(columnsInterval);
        List<FragmentMeta> fragmentMetas = fragmentMapOfTimeSeriesInterval.get(columnsInterval);

        if (fragmentMetas != null) {
          List<String> keyIntervals =
              client.getChildren().forPath(STATISTICS_FRAGMENT_HEAT_PREFIX_WRITE + "/" + child);
          for (String keyInterval : keyIntervals) {
            long startTime = Long.parseLong(keyInterval);
            for (FragmentMeta fragmentMeta : fragmentMetas) {
              if (fragmentMeta.getKeyInterval().getStartKey() == startTime) {
                byte[] data =
                    this.client
                        .getData()
                        .forPath(
                            STATISTICS_FRAGMENT_HEAT_PREFIX_WRITE
                                + "/"
                                + child
                                + "/"
                                + keyInterval);
                long heat = JsonUtils.fromJson(data, Long.class);
                writeHotspotMap.put(fragmentMeta, heat);
              }
            }
          }
        }
      }
    }

    Map<FragmentMeta, Long> readHotspotMap = new HashMap<>();
    if (this.client.checkExists().forPath(STATISTICS_FRAGMENT_HEAT_PREFIX_READ) != null) {
      List<String> children = client.getChildren().forPath(STATISTICS_FRAGMENT_HEAT_PREFIX_READ);
      for (String child : children) {
        ColumnsInterval columnsInterval = fromString(child);
        Map<ColumnsInterval, List<FragmentMeta>> fragmentMapOfTimeSeriesInterval =
            cache.getFragmentMapByColumnsInterval(columnsInterval);
        List<FragmentMeta> fragmentMetas = fragmentMapOfTimeSeriesInterval.get(columnsInterval);

        if (fragmentMetas != null) {
          List<String> keyIntervals =
              client.getChildren().forPath(STATISTICS_FRAGMENT_HEAT_PREFIX_READ + "/" + child);
          for (String keyInterval : keyIntervals) {
            long startTime = Long.parseLong(keyInterval);
            for (FragmentMeta fragmentMeta : fragmentMetas) {
              if (fragmentMeta.getKeyInterval().getStartKey() == startTime) {
                byte[] data =
                    this.client
                        .getData()
                        .forPath(
                            STATISTICS_FRAGMENT_HEAT_PREFIX_READ + "/" + child + "/" + keyInterval);
                long heat = JsonUtils.fromJson(data, Long.class);
                readHotspotMap.put(fragmentMeta, heat);
              }
            }
          }
        }
      }
    }
    return new Pair<>(writeHotspotMap, readHotspotMap);
  }

  @Override
  public void removeFragmentHeat() throws MetaStorageException {
    try {
      if (this.client.checkExists().forPath(STATISTICS_FRAGMENT_HEAT_PREFIX_WRITE) != null) {
        this.client
            .delete()
            .deletingChildrenIfNeeded()
            .forPath(STATISTICS_FRAGMENT_HEAT_PREFIX_WRITE);
      }
      if (this.client.checkExists().forPath(STATISTICS_FRAGMENT_HEAT_PREFIX_READ) != null) {
        this.client
            .delete()
            .deletingChildrenIfNeeded()
            .forPath(STATISTICS_FRAGMENT_HEAT_PREFIX_READ);
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when removing fragment heat: ", e);
    }
  }

  @Override
  public void lockFragmentHeatCounter() throws MetaStorageException {
    try {
      fragmentHeatCounterMutexLock.lock();
      fragmentHeatCounterMutex.acquire();
    } catch (Exception e) {
      fragmentHeatCounterMutexLock.unlock();
      throw new MetaStorageException(
          "encounter error when acquiring fragment heat counter mutex: ", e);
    }
  }

  @Override
  public void incrementFragmentHeatCounter() throws MetaStorageException {
    try {
      if (this.client.checkExists().forPath(STATISTICS_FRAGMENT_HEAT_COUNTER_PREFIX) == null) {
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(STATISTICS_FRAGMENT_HEAT_COUNTER_PREFIX, JsonUtils.toJson(1));
      } else {
        int counter =
            JsonUtils.fromJson(
                this.client.getData().forPath(STATISTICS_FRAGMENT_HEAT_COUNTER_PREFIX),
                Integer.class);
        this.client
            .setData()
            .forPath(STATISTICS_FRAGMENT_HEAT_COUNTER_PREFIX, JsonUtils.toJson(counter + 1));
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when updating fragment heat counter: ", e);
    }
  }

  @Override
  public void resetFragmentHeatCounter() throws MetaStorageException {
    try {
      if (this.client.checkExists().forPath(STATISTICS_FRAGMENT_HEAT_COUNTER_PREFIX) == null) {
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(STATISTICS_FRAGMENT_HEAT_COUNTER_PREFIX, JsonUtils.toJson(0));
      } else {
        this.client.setData().forPath(STATISTICS_FRAGMENT_HEAT_COUNTER_PREFIX, JsonUtils.toJson(0));
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when resetting fragment heat counter: ", e);
    }
  }

  @Override
  public void releaseFragmentHeatCounter() throws MetaStorageException {
    try {
      fragmentHeatCounterMutex.release();
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when releasing latency counter mutex: ", e);
    } finally {
      fragmentHeatCounterMutexLock.unlock();
    }
  }

  @Override
  public int getFragmentHeatCounter() throws MetaStorageException {
    try {
      if (this.client.checkExists().forPath(STATISTICS_FRAGMENT_HEAT_COUNTER_PREFIX) == null) {
        return 0;
      } else {
        return JsonUtils.fromJson(
            this.client.getData().forPath(STATISTICS_FRAGMENT_HEAT_COUNTER_PREFIX), Integer.class);
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when get fragment heat counter: ", e);
    }
  }

  @Override
  public boolean proposeToReshard() throws MetaStorageException {
    try {
      ReshardStatus status;
      if (this.client.checkExists().forPath(RESHARD_STATUS_NODE_PREFIX) == null) {
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(RESHARD_STATUS_NODE_PREFIX, JsonUtils.toJson(EXECUTING));
        return true;
      } else {
        status =
            JsonUtils.fromJson(
                this.client.getData().forPath(RESHARD_STATUS_NODE_PREFIX), ReshardStatus.class);
        if (status.equals(NON_RESHARDING) || status.equals(JUDGING)) {
          this.client.setData().forPath(RESHARD_STATUS_NODE_PREFIX, JsonUtils.toJson(EXECUTING));
          return true;
        }
        return false;
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when proposing to reshard: ", e);
    }
  }

  @Override
  public void lockReshardStatus() throws MetaStorageException {
    try {
      reshardStatusMutexLock.lock();
      reshardStatusMutex.acquire();
    } catch (Exception e) {
      reshardStatusMutexLock.unlock();
      throw new MetaStorageException("encounter error when acquiring reshard status mutex: ", e);
    }
  }

  @Override
  public void updateReshardStatus(ReshardStatus status) throws MetaStorageException {
    try {
      if (this.client.checkExists().forPath(RESHARD_STATUS_NODE_PREFIX) == null) {
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(RESHARD_STATUS_NODE_PREFIX, JsonUtils.toJson(status));
      } else {
        this.client.setData().forPath(RESHARD_STATUS_NODE_PREFIX, JsonUtils.toJson(status));
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when updating reshard status: ", e);
    }
  }

  @Override
  public void releaseReshardStatus() throws MetaStorageException {
    try {
      reshardStatusMutex.release();
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when releasing reshard status mutex: ", e);
    } finally {
      reshardStatusMutexLock.unlock();
    }
  }

  @Override
  public void removeReshardStatus() throws MetaStorageException {
    try {
      if (this.client.checkExists().forPath(RESHARD_STATUS_NODE_PREFIX) != null) {
        this.client.delete().forPath(RESHARD_STATUS_NODE_PREFIX);
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when removing reshard status: ", e);
    }
  }

  @Override
  public void registerReshardStatusHook(ReshardStatusChangeHook hook) {
    this.reshardStatusChangeHook = hook;
  }

  private void registerReshardStatusListener() throws Exception {
    this.reshardStatusCache = new TreeCache(this.client, RESHARD_STATUS_NODE_PREFIX);
    TreeCacheListener listener =
        (curatorFramework, event) -> {
          byte[] data;
          ReshardStatus status;
          switch (event.getType()) {
            case NODE_ADDED:
            case NODE_UPDATED:
              data = event.getData().getData();
              status = JsonUtils.fromJson(data, ReshardStatus.class);
              LOGGER.error("status = {}", status);
              reshardStatusChangeHook.onChange(status);
              break;
            default:
              break;
          }
        };
    this.reshardStatusCache.getListenable().addListener(listener);
    this.reshardStatusCache.start();
  }

  @Override
  public void lockReshardCounter() throws MetaStorageException {
    try {
      reshardCounterMutexLock.lock();
      reshardCounterMutex.acquire();
    } catch (Exception e) {
      reshardCounterMutexLock.unlock();
      throw new MetaStorageException("encounter error when acquiring reshard counter mutex: ", e);
    }
  }

  @Override
  public void incrementReshardCounter() throws MetaStorageException {
    try {
      if (this.client.checkExists().forPath(RESHARD_COUNTER_NODE_PREFIX) == null) {
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(RESHARD_COUNTER_NODE_PREFIX, JsonUtils.toJson(1));
      } else {
        int counter =
            JsonUtils.fromJson(
                this.client.getData().forPath(RESHARD_COUNTER_NODE_PREFIX), Integer.class);
        this.client.setData().forPath(RESHARD_COUNTER_NODE_PREFIX, JsonUtils.toJson(counter + 1));
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when updating reshard counter: ", e);
    }
  }

  @Override
  public void resetReshardCounter() throws MetaStorageException {
    try {
      if (this.client.checkExists().forPath(RESHARD_COUNTER_NODE_PREFIX) == null) {
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(RESHARD_COUNTER_NODE_PREFIX, JsonUtils.toJson(0));
      } else {
        this.client.setData().forPath(RESHARD_COUNTER_NODE_PREFIX, JsonUtils.toJson(0));
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when resetting reshard counter: ", e);
    }
  }

  @Override
  public void releaseReshardCounter() throws MetaStorageException {
    try {
      reshardCounterMutex.release();
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when releasing reshard counter mutex: ", e);
    } finally {
      reshardCounterMutexLock.unlock();
    }
  }

  @Override
  public void removeReshardCounter() throws MetaStorageException {
    try {
      if (this.client.checkExists().forPath(RESHARD_COUNTER_NODE_PREFIX) != null) {
        this.client.delete().forPath(RESHARD_COUNTER_NODE_PREFIX);
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when removing reshard counter: ", e);
    }
  }

  @Override
  public void registerReshardCounterChangeHook(ReshardCounterChangeHook hook) {
    this.reshardCounterChangeHook = hook;
  }

  private void registerMaxActiveEndTimeStatisticsListener() throws Exception {
    this.maxActiveEndTimeStatisticsCache =
        new TreeCache(this.client, MAX_ACTIVE_END_TIME_STATISTICS_NODE_PREFIX);
    TreeCacheListener listener =
        (curatorFramework, event) -> {
          if (maxActiveEndKeyStatisticsChangeHook == null) {
            return;
          }
          String path;
          byte[] data;
          long endTime;
          switch (event.getType()) {
            case NODE_ADDED:
            case NODE_UPDATED:
              path = event.getData().getPath();
              data = event.getData().getData();
              String[] pathParts = path.split("/");
              if (pathParts.length == 7) {
                endTime = JsonUtils.fromJson(data, Long.class);
                maxActiveEndKeyStatisticsChangeHook.onChange(endTime);
              }
              break;
            default:
              break;
          }
        };
    this.maxActiveEndTimeStatisticsCache.getListenable().addListener(listener);
    this.maxActiveEndTimeStatisticsCache.start();
  }

  @Override
  public void lockMaxActiveEndKeyStatistics() throws MetaStorageException {
    try {
      maxActiveEndTimeStatisticsMutexLock.lock();
      maxActiveEndTimeStatisticsMutex.acquire();
    } catch (Exception e) {
      maxActiveEndTimeStatisticsMutexLock.unlock();
      throw new MetaStorageException(
          "encounter error when acquiring mac active end time statistics mutex: ", e);
    }
  }

  @Override
  public void addOrUpdateMaxActiveEndKeyStatistics(long endKey) throws MetaStorageException {
    try {
      if (this.client.checkExists().forPath(MAX_ACTIVE_END_TIME_STATISTICS_NODE) == null) {
        this.client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(MAX_ACTIVE_END_TIME_STATISTICS_NODE, JsonUtils.toJson(endKey));
      } else {
        this.client
            .setData()
            .forPath(MAX_ACTIVE_END_TIME_STATISTICS_NODE, JsonUtils.toJson(endKey));
      }
    } catch (Exception e) {
      throw new MetaStorageException(
          "encounter error when adding or updating max active end time statistics: ", e);
    }
  }

  @Override
  public long getMaxActiveEndKeyStatistics() throws MetaStorageException {
    try {
      if (this.client.checkExists().forPath(MAX_ACTIVE_END_TIME_STATISTICS_NODE) != null) {
        return JsonUtils.fromJson(
            this.client.getData().forPath(MAX_ACTIVE_END_TIME_STATISTICS_NODE), Long.class);
      }
    } catch (Exception e) {
      throw new MetaStorageException(
          "encounter error when adding or updating max active end time statistics: ", e);
    }
    return -1;
  }

  @Override
  public void releaseMaxActiveEndKeyStatistics() throws MetaStorageException {
    try {
      maxActiveEndTimeStatisticsMutex.release();
    } catch (Exception e) {
      throw new MetaStorageException(
          "encounter error when releasing max active end time statistics mutex: ", e);
    } finally {
      maxActiveEndTimeStatisticsMutexLock.unlock();
    }
  }

  @Override
  public void registerMaxActiveEndKeyStatisticsChangeHook(MaxActiveEndKeyStatisticsChangeHook hook)
      throws MetaStorageException {
    this.maxActiveEndKeyStatisticsChangeHook = hook;
  }

  private void registerReshardCounterListener() throws Exception {
    this.reshardCounterCache = new TreeCache(this.client, RESHARD_COUNTER_NODE_PREFIX);
    TreeCacheListener listener =
        (curatorFramework, event) -> {
          byte[] data;
          int counter;
          switch (event.getType()) {
            case NODE_ADDED:
            case NODE_UPDATED:
              data = event.getData().getData();
              counter = JsonUtils.fromJson(data, Integer.class);
              reshardCounterChangeHook.onChange(counter);
              break;
            default:
              break;
          }
        };
    this.reshardCounterCache.getListenable().addListener(listener);
    this.reshardCounterCache.start();
  }

  public static boolean isNumeric(String str) {
    String bigStr;
    try {
      bigStr = new BigDecimal(str).toString();
    } catch (Exception e) {
      return false; // 异常 说明包含非数字。
    }
    return true;
  }
}
