package cn.edu.tsinghua.iginx;

import static cn.edu.tsinghua.iginx.metadata.utils.IdUtils.generateDummyStorageUnitId;
import static cn.edu.tsinghua.iginx.metadata.utils.StorageEngineUtils.*;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getLongArrayFromByteBuffer;
import static cn.edu.tsinghua.iginx.utils.HostUtils.isLocalHost;
import static cn.edu.tsinghua.iginx.utils.HostUtils.isValidHost;
import static cn.edu.tsinghua.iginx.utils.StringUtils.isEqual;

import cn.edu.tsinghua.iginx.auth.FilePermissionManager;
import cn.edu.tsinghua.iginx.auth.SessionManager;
import cn.edu.tsinghua.iginx.auth.UserManager;
import cn.edu.tsinghua.iginx.auth.entity.FileAccessType;
import cn.edu.tsinghua.iginx.auth.utils.FilePermissionRuleNameFilters;
import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.engine.ContextBuilder;
import cn.edu.tsinghua.iginx.engine.StatementExecutor;
import cn.edu.tsinghua.iginx.engine.logical.optimizer.IRuleCollection;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.StorageManager;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import cn.edu.tsinghua.iginx.exception.StatusCode;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.resource.QueryResourceManager;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.transform.exec.TransformJobManager;
import cn.edu.tsinghua.iginx.utils.*;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IginxWorker implements IService.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(IginxWorker.class);

  private static final IginxWorker instance = new IginxWorker();

  private final IMetaManager metaManager = DefaultMetaManager.getInstance();

  private final UserManager userManager = UserManager.getInstance();

  private final SessionManager sessionManager = SessionManager.getInstance();

  private final QueryResourceManager queryManager = QueryResourceManager.getInstance();

  private final ContextBuilder contextBuilder = ContextBuilder.getInstance();

  private final StatementExecutor executor = StatementExecutor.getInstance();

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private IginxWorker() {
    // if there are new local parquets/file systems in conf, add them to cluster.
    if (!addLocalStorageEngineMetas()) {
      LOGGER.error("there are no valid storage engines!");
      System.exit(-1);
    }
  }

  private boolean addLocalStorageEngineMetas() {
    List<StorageEngineMeta> localMetas = new ArrayList<>();
    boolean hasOtherMetas = false;
    Status status = new Status(RpcUtils.SUCCESS.code);
    for (StorageEngineMeta metaFromConf : metaManager.getStorageEngineListFromConf()) {
      if (!isEmbeddedStorageEngine(metaFromConf.getStorageEngine())) {
        hasOtherMetas = true;
        continue;
      }
      Map<String, String> extraParams = metaFromConf.getExtraParams();
      if (!checkEmbeddedStorageExtraParams(metaFromConf.getStorageEngine(), extraParams)) {
        LOGGER.error(
            "missing params or providing invalid ones for {} in config file.", metaFromConf);
        status.addToSubStatus(RpcUtils.FAILURE);
        continue;
      }
      metaFromConf.setExtraParams(extraParams);
      boolean hasAdded = false;
      for (StorageEngineMeta meta : metaManager.getStorageEngineList()) {
        if (isDuplicated(metaFromConf, meta)) {
          hasAdded = true;
          break;
        }
      }
      if (hasAdded) {
        hasOtherMetas = true;
        continue;
      }
      if (isLocal(metaFromConf)) {
        localMetas.add(metaFromConf);
      }
    }
    if (!localMetas.isEmpty()) {
      addStorageEngineMetas(localMetas, status);
    } else if (!hasOtherMetas) {
      return false;
    }
    return true;
  }

  public static IginxWorker getInstance() {
    return instance;
  }

  @Override
  public OpenSessionResp openSession(OpenSessionReq req) {
    String username = req.getUsername();
    String password = req.getPassword();
    if (!userManager.checkUser(username, password)) {
      OpenSessionResp resp = new OpenSessionResp(RpcUtils.WRONG_USERNAME_OR_PASSWORD);
      resp.setSessionId(0L);
      return resp;
    }
    long sessionId = sessionManager.openSession(username);
    OpenSessionResp resp = new OpenSessionResp(RpcUtils.SUCCESS);
    resp.setSessionId(sessionId);
    return resp;
  }

  @Override
  public Status closeSession(CloseSessionReq req) {
    sessionManager.closeSession(req.getSessionId());
    return RpcUtils.SUCCESS;
  }

  @Override
  public Status deleteColumns(DeleteColumnsReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Write)) {
      return RpcUtils.ACCESS_DENY;
    }
    RequestContext ctx = contextBuilder.build(req);
    executor.execute(ctx);
    return ctx.getResult().getStatus();
  }

  @Override
  public Status insertColumnRecords(InsertColumnRecordsReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Write)) {
      return RpcUtils.ACCESS_DENY;
    }
    if (!StringUtils.allHasMoreThanOneSubPath(req.getPaths())) {
      LOGGER.error("Insert paths must have more than one sub paths.");
      return RpcUtils.FAILURE;
    }
    RequestContext ctx = contextBuilder.build(req);
    executor.execute(ctx);
    return ctx.getResult().getStatus();
  }

  @Override
  public Status insertNonAlignedColumnRecords(InsertNonAlignedColumnRecordsReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Write)) {
      return RpcUtils.ACCESS_DENY;
    }
    if (!StringUtils.allHasMoreThanOneSubPath(req.getPaths())) {
      LOGGER.error("Insert paths must have more than one sub paths.");
      return RpcUtils.FAILURE;
    }
    RequestContext ctx = contextBuilder.build(req);
    executor.execute(ctx);
    return ctx.getResult().getStatus();
  }

  @Override
  public Status insertRowRecords(InsertRowRecordsReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Write)) {
      return RpcUtils.ACCESS_DENY;
    }
    if (!StringUtils.allHasMoreThanOneSubPath(req.getPaths())) {
      LOGGER.error("Insert paths must have more than one sub paths.");
      return RpcUtils.FAILURE;
    }
    RequestContext ctx = contextBuilder.build(req);
    executor.execute(ctx);
    return ctx.getResult().getStatus();
  }

  @Override
  public Status insertNonAlignedRowRecords(InsertNonAlignedRowRecordsReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Write)) {
      return RpcUtils.ACCESS_DENY;
    }
    if (!StringUtils.allHasMoreThanOneSubPath(req.getPaths())) {
      LOGGER.error("Insert paths must have more than one sub paths.");
      return RpcUtils.FAILURE;
    }
    RequestContext ctx = contextBuilder.build(req);
    executor.execute(ctx);
    return ctx.getResult().getStatus();
  }

  @Override
  public Status deleteDataInColumns(DeleteDataInColumnsReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Write)) {
      return RpcUtils.ACCESS_DENY;
    }
    RequestContext ctx = contextBuilder.build(req);
    executor.execute(ctx);
    return ctx.getResult().getStatus();
  }

  @Override
  public QueryDataResp queryData(QueryDataReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Read)) {
      return new QueryDataResp(RpcUtils.ACCESS_DENY);
    }
    RequestContext ctx = contextBuilder.build(req);
    executor.execute(ctx);
    return ctx.getResult().getQueryDataResp();
  }

  @Override
  public Status removeHistoryDataSource(RemoveHistoryDataSourceReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Cluster)) {
      return RpcUtils.ACCESS_DENY;
    }
    List<RemovedStorageEngineInfo> removedStorageEngineInfoList =
        req.getRemovedStorageEngineInfoList();
    Status status = new Status(RpcUtils.SUCCESS.code);

    for (RemovedStorageEngineInfo info : removedStorageEngineInfoList) {
      StorageEngineMeta storageEngineMeta = null;
      String infoIp = info.getIp(),
          infoSchemaPrefix = info.getSchemaPrefix(),
          infoDataPrefix = info.getDataPrefix();
      int infoPort = info.getPort();
      for (StorageEngineMeta meta : metaManager.getStorageEngineList()) {
        String metaIp = meta.getIp(),
            metaSchemaPrefix = meta.getSchemaPrefix(),
            metaDataPrefix = meta.getDataPrefix();
        int metaPort = meta.getPort();
        if (!infoIp.equals(metaIp)) {
          continue;
        }
        if (infoPort != metaPort) {
          continue;
        }
        if (!isEqual(infoSchemaPrefix, metaSchemaPrefix)) {
          continue;
        }
        if (!isEqual(infoDataPrefix, metaDataPrefix)) {
          continue;
        }
        storageEngineMeta = meta;
        break;
      }
      if (storageEngineMeta == null
          || storageEngineMeta.getDummyFragment() == null
          || storageEngineMeta.getDummyStorageUnit() == null) {
        LOGGER.error("dummy storage engine {} does not exist.", info);
        status.addToSubStatus(RpcUtils.FAILURE);
        continue;
      }
      if (!storageEngineMeta.isHasData()) {
        LOGGER.error("dummy storage engine {} has no data.", info);
        status.addToSubStatus(RpcUtils.FAILURE);
        continue;
      }
      if (!storageEngineMeta.isReadOnly()) {
        LOGGER.error("dummy storage engine {} is not read-only.", info);
        status.addToSubStatus(RpcUtils.FAILURE);
        continue;
      }
      // 更新 zk 以及缓存中的元数据信息
      if (!metaManager.removeDummyStorageEngine(storageEngineMeta.getId())) {
        LOGGER.error("unexpected error during removing dummy storage engine {}.", info);
        status.addToSubStatus(RpcUtils.FAILURE);
      }
    }

    if (status.isSetSubStatus()) {
      if (status.subStatus.size() == removedStorageEngineInfoList.size()) {
        status.setCode(RpcUtils.FAILURE.code); // 所有请求均失败
        status.setMessage("remove history data source failed");
      } else {
        status.setCode(RpcUtils.PARTIAL_SUCCESS.code); // 部分请求失败
        status.setMessage("remove history data source succeeded partially");
      }
    }

    return status;
  }

  @Override
  public Status addStorageEngines(AddStorageEnginesReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Cluster)) {
      return RpcUtils.ACCESS_DENY;
    }
    List<StorageEngine> storageEngines = req.getStorageEngines();
    List<StorageEngineMeta> storageEngineMetas = new ArrayList<>();
    Status status = new Status(RpcUtils.SUCCESS.code);

    for (StorageEngine storageEngine : storageEngines) {
      String ip = storageEngine.getIp();
      int port = storageEngine.getPort();
      StorageEngineType type = storageEngine.getType();
      Map<String, String> extraParams = storageEngine.getExtraParams();
      boolean hasData = Boolean.parseBoolean(extraParams.getOrDefault(Constants.HAS_DATA, "false"));
      String dataPrefix = null;
      if (hasData && extraParams.containsKey(Constants.DATA_PREFIX)) {
        dataPrefix = extraParams.get(Constants.DATA_PREFIX);
      }
      boolean readOnly =
          Boolean.parseBoolean(extraParams.getOrDefault(Constants.IS_READ_ONLY, "false"));

      if (!isValidHost(ip)) { // IP 不合法
        LOGGER.error("ip {} is invalid.", ip);
        status.addToSubStatus(RpcUtils.FAILURE);
        continue;
      }
      if (isEmbeddedStorageEngine(type) && !isLocalHost(ip)) {
        status.setCode(StatusCode.REDIRECT.getStatusCode());
        status.setMessage(ip + ":" + extraParams.get("iginx_port"));
        LOGGER.warn("redirect to {}:{}.", ip, extraParams.get("iginx_port"));
        return status;
      }
      if (!hasData & readOnly) { // 无意义的存储引擎：不带数据且只读
        LOGGER.error("normal storage engine {} should not be read-only.", storageEngine);
        status.addToSubStatus(RpcUtils.FAILURE);
        continue;
      }
      if (!checkEmbeddedStorageExtraParams(type, extraParams)) {
        LOGGER.error(
            "missing params or providing invalid ones for {} in statement.", storageEngine);
        status.addToSubStatus(RpcUtils.FAILURE);
        continue;
      }
      String schemaPrefix = extraParams.get(Constants.SCHEMA_PREFIX);
      StorageEngineMeta meta =
          new StorageEngineMeta(
              -1,
              ip,
              port,
              hasData,
              dataPrefix,
              schemaPrefix,
              readOnly,
              extraParams,
              type,
              metaManager.getIginxId());
      storageEngineMetas.add(meta);
    }

    if (status.isSetSubStatus()) {
      if (status.subStatus.size() == storageEngines.size()) {
        status.setCode(RpcUtils.FAILURE.code); // 所有请求均失败
        status.setMessage("add storage engines failed");
        return status;
      } else {
        status.setCode(RpcUtils.PARTIAL_SUCCESS.code); // 部分请求失败
      }
    }

    addStorageEngineMetas(storageEngineMetas, status, false);
    return status;
  }

  private void addStorageEngineMetas(List<StorageEngineMeta> storageEngineMetas, Status status) {
    addStorageEngineMetas(storageEngineMetas, status, true);
    if (status.code != RpcUtils.SUCCESS.code) {
      LOGGER.error("add local storage engines failed when initializing IginxWorker!");
    }
  }

  private void addStorageEngineMetas(
      List<StorageEngineMeta> storageEngineMetas, Status status, boolean hasChecked) {
    // 检测是否与已有的存储引擎冲突
    if (!hasChecked) {
      List<StorageEngineMeta> currentStorageEngines = metaManager.getStorageEngineList();
      List<StorageEngineMeta> duplicatedStorageEngines = new ArrayList<>();
      for (StorageEngineMeta storageEngine : storageEngineMetas) {
        for (StorageEngineMeta currentStorageEngine : currentStorageEngines) {
          if (isDuplicated(storageEngine, currentStorageEngine)) {
            duplicatedStorageEngines.add(storageEngine);
            LOGGER.error("repeatedly add storage engine {}.", storageEngine);
            status.addToSubStatus(RpcUtils.FAILURE);
            break;
          }
        }
      }
      if (!duplicatedStorageEngines.isEmpty()) {
        storageEngineMetas.removeAll(duplicatedStorageEngines);
        if (!storageEngineMetas.isEmpty()) {
          status.setCode(RpcUtils.PARTIAL_SUCCESS.code);
        } else {
          status.setCode(RpcUtils.FAILURE.code);
          status.setMessage("repeatedly add storage engine");
          return;
        }
      }
    }
    if (!storageEngineMetas.isEmpty()
        && storageEngineMetas.stream().anyMatch(e -> !e.isReadOnly())) {
      storageEngineMetas
          .get(storageEngineMetas.size() - 1)
          .setNeedReAllocate(true); // 如果这批节点不是只读的话，每一批最后一个是 true，表示需要进行扩容
    }
    for (StorageEngineMeta meta : storageEngineMetas) {
      if (meta.isHasData()) {
        String dataPrefix = meta.getDataPrefix();
        String schemaPrefix = meta.getSchemaPrefix();
        StorageUnitMeta dummyStorageUnit = new StorageUnitMeta(generateDummyStorageUnitId(0), -1);
        Pair<ColumnsInterval, KeyInterval> boundary =
            StorageManager.getBoundaryOfStorage(meta, dataPrefix);
        if (boundary == null) {
          status.setCode(RpcUtils.FAILURE.code);
          status.setMessage(
              String.format(
                  "Failed to process dummy storage engine %s. Please check params:%s;%s.",
                  meta.getStorageEngine(), meta, meta.getExtraParams()));
          return;
        }
        LOGGER.info("boundary for {}: {}", meta, boundary);
        FragmentMeta dummyFragment;

        if (dataPrefix == null) {
          boundary.k.setSchemaPrefix(schemaPrefix);
          dummyFragment = new FragmentMeta(boundary.k, boundary.v, dummyStorageUnit);
        } else {
          ColumnsInterval columnsInterval = new ColumnsInterval(dataPrefix);
          columnsInterval.setSchemaPrefix(schemaPrefix);
          dummyFragment = new FragmentMeta(columnsInterval, boundary.v, dummyStorageUnit);
        }
        dummyFragment.setDummyFragment(true);
        meta.setDummyStorageUnit(dummyStorageUnit);
        meta.setDummyFragment(dummyFragment);
      }
    }

    // init local parquet/file system before adding to meta
    // exclude remote parquet/file system
    List<StorageEngineMeta> localMetas = new ArrayList<>();
    List<StorageEngineMeta> otherMetas = new ArrayList<>();
    for (StorageEngineMeta meta : storageEngineMetas) {
      if (isEmbeddedStorageEngine(meta.getStorageEngine())) {
        if (!isLocal(meta)) {
          LOGGER.error("storage engine {} needs to be local.", meta);
          status.addToSubStatus(RpcUtils.FAILURE);
        } else {
          localMetas.add(meta);
        }
      } else {
        otherMetas.add(meta);
      }
    }

    // TODO: 下面两个循环疑似可以合并在一起
    StorageManager storageManager = PhysicalEngineImpl.getInstance().getStorageManager();
    for (StorageEngineMeta meta : otherMetas) {
      IStorage storage = StorageManager.initStorageInstance(meta);
      if (storage == null) {
        status.addToSubStatus(
            RpcUtils.status(
                StatusCode.STATEMENT_EXECUTION_ERROR,
                String.format("init storage engine %s failed", meta)));
        continue;
      }
      if (!metaManager.addStorageEngines(Collections.singletonList(meta))) {
        LOGGER.error("add storage engine {} failed.", meta);
        status.addToSubStatus(RpcUtils.FAILURE);
        continue;
      }
      storageManager.addStorage(meta, storage);
    }
    for (StorageEngineMeta meta : localMetas) {
      IStorage storage = StorageManager.initStorageInstance(meta);
      if (storage == null) {
        status.addToSubStatus(
            RpcUtils.status(
                StatusCode.STATEMENT_EXECUTION_ERROR,
                String.format("init storage engine %s failed", meta)));
        continue;
      }
      if (!metaManager.addStorageEngines(Collections.singletonList(meta))) {
        LOGGER.error("add storage engine {} failed.", meta);
        status.addToSubStatus(RpcUtils.FAILURE);
        continue;
      }
      storageManager.addStorage(meta, storage);
    }
    if (status.isSetSubStatus()) {
      status.setCode(RpcUtils.FAILURE.code);
      status.setMessage("add storage engines failed");
    }
  }

  private boolean isDuplicated(StorageEngineMeta engine1, StorageEngineMeta engine2) {
    if (!engine1.getStorageEngine().equals(engine2.getStorageEngine())) {
      return false;
    }
    if (engine1.getPort() != engine2.getPort()) {
      return false;
    }
    if (!Objects.equals(engine1.getDataPrefix(), engine2.getDataPrefix())) {
      return false;
    }
    if (!Objects.equals(engine1.getSchemaPrefix(), engine2.getSchemaPrefix())) {
      return false;
    }
    if (isLocalHost(engine1.getIp()) && isLocalHost(engine2.getIp())) { // 都是本机IP
      return true;
    }
    return engine1.getIp().equals(engine2.getIp());
  }

  @Override
  public AggregateQueryResp aggregateQuery(AggregateQueryReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Read)) {
      return new AggregateQueryResp(RpcUtils.ACCESS_DENY);
    }
    RequestContext ctx = contextBuilder.build(req);
    executor.execute(ctx);
    return ctx.getResult().getAggregateQueryResp();
  }

  @Override
  public DownsampleQueryResp downsampleQuery(DownsampleQueryReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Read)) {
      return new DownsampleQueryResp(RpcUtils.ACCESS_DENY);
    }
    RequestContext ctx = contextBuilder.build(req);
    executor.execute(ctx);
    return ctx.getResult().getDownSampleQueryResp();
  }

  @Override
  public ShowColumnsResp showColumns(ShowColumnsReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Read)) {
      return new ShowColumnsResp(RpcUtils.ACCESS_DENY);
    }
    RequestContext ctx = contextBuilder.build(req);
    executor.execute(ctx);
    return ctx.getResult().getShowColumnsResp();
  }

  @Override
  public GetReplicaNumResp getReplicaNum(GetReplicaNumReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Read)) {
      return new GetReplicaNumResp(RpcUtils.ACCESS_DENY);
    }
    GetReplicaNumResp resp = new GetReplicaNumResp(RpcUtils.SUCCESS);
    resp.setReplicaNum(ConfigDescriptor.getInstance().getConfig().getReplicaNum() + 1);
    return resp;
  }

  @Override
  public ExecuteSqlResp executeSql(ExecuteSqlReq req) {
    StatementExecutor executor = StatementExecutor.getInstance();
    RequestContext ctx = contextBuilder.build(req);
    executor.execute(ctx);
    return ctx.getResult().getExecuteSqlResp();
  }

  @Override
  public LastQueryResp lastQuery(LastQueryReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Read)) {
      return new LastQueryResp(RpcUtils.ACCESS_DENY);
    }

    RequestContext ctx = contextBuilder.build(req);
    executor.execute(ctx);
    return ctx.getResult().getLastQueryResp();
  }

  @Override
  public Status updateUser(UpdateUserReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Admin)) {
      return RpcUtils.ACCESS_DENY;
    }
    if (userManager.updateUser(req.username, req.password, req.auths)) {
      return RpcUtils.SUCCESS;
    }
    return RpcUtils.FAILURE;
  }

  @Override
  public Status addUser(AddUserReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Admin)) {
      return RpcUtils.ACCESS_DENY;
    }
    if (userManager.addUser(req.username, req.password, req.auths)) {
      return RpcUtils.SUCCESS;
    }
    return RpcUtils.FAILURE;
  }

  @Override
  public Status deleteUser(DeleteUserReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Admin)) {
      return RpcUtils.ACCESS_DENY;
    }
    if (userManager.deleteUser(req.username)) {
      return RpcUtils.SUCCESS;
    }
    return RpcUtils.FAILURE;
  }

  @Override
  public GetUserResp getUser(GetUserReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Read)) {
      return new GetUserResp(RpcUtils.ACCESS_DENY);
    }
    GetUserResp resp = new GetUserResp(RpcUtils.SUCCESS);
    List<UserMeta> users;
    if (req.usernames == null) {
      users = userManager.getUsers();
    } else {
      users = userManager.getUsers(req.getUsernames());
    }
    List<String> usernames = users.stream().map(UserMeta::getUsername).collect(Collectors.toList());
    List<UserType> userTypes =
        users.stream().map(UserMeta::getUserType).collect(Collectors.toList());
    List<Set<AuthType>> auths = users.stream().map(UserMeta::getAuths).collect(Collectors.toList());
    resp.setUsernames(usernames);
    resp.setUserTypes(userTypes);
    resp.setAuths(auths);
    return resp;
  }

  @Override
  public GetClusterInfoResp getClusterInfo(GetClusterInfoReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Cluster)) {
      return new GetClusterInfoResp(RpcUtils.ACCESS_DENY);
    }

    GetClusterInfoResp resp = new GetClusterInfoResp();

    // IginX 信息
    List<IginxInfo> iginxInfos = new ArrayList<>();
    // if starts in Docker, host_iginx_port will be given as env, representing host port to access
    // IGinX service
    String iginxPort = System.getenv("host_iginx_port");
    for (IginxMeta iginxMeta : metaManager.getIginxList()) {
      int thisIginxPort = iginxPort != null ? Integer.parseInt(iginxPort) : iginxMeta.getPort();
      iginxInfos.add(new IginxInfo(iginxMeta.getId(), iginxMeta.getIp(), thisIginxPort));
    }
    iginxInfos.sort(Comparator.comparingLong(IginxInfo::getId));
    resp.setIginxInfos(iginxInfos);

    // 数据库信息
    List<StorageEngineInfo> storageEngineInfos = new ArrayList<>();
    for (StorageEngineMeta storageEngineMeta : metaManager.getStorageEngineList()) {
      StorageEngineInfo info =
          new StorageEngineInfo(
              storageEngineMeta.getId(),
              storageEngineMeta.getIp().equals("host.docker.internal")
                  ? System.getenv("ip")
                  : storageEngineMeta.getIp(),
              storageEngineMeta.getPort(),
              storageEngineMeta.getStorageEngine());
      info.setSchemaPrefix(
          storageEngineMeta.getSchemaPrefix() == null
              ? "null"
              : storageEngineMeta.getSchemaPrefix());
      info.setDataPrefix(
          storageEngineMeta.getDataPrefix() == null ? "null" : storageEngineMeta.getDataPrefix());
      storageEngineInfos.add(info);
    }
    storageEngineInfos.sort(Comparator.comparingLong(StorageEngineInfo::getId));
    resp.setStorageEngineInfos(storageEngineInfos);

    Config config = ConfigDescriptor.getInstance().getConfig();
    List<MetaStorageInfo> metaStorageInfos = null;
    LocalMetaStorageInfo localMetaStorageInfo = null;

    switch (config.getMetaStorage()) {
      case Constants.ETCD_META:
        metaStorageInfos = new ArrayList<>();
        String[] endPoints = config.getEtcdEndpoints().split(",");
        for (String endPoint : endPoints) {
          if (endPoint.startsWith("http://")) {
            endPoint = endPoint.substring(7);
          } else if (endPoint.startsWith("https://")) {
            endPoint = endPoint.substring(8);
          }
          String[] ipAndPort = endPoint.split(":", 2);
          MetaStorageInfo metaStorageInfo =
              new MetaStorageInfo(
                  ipAndPort[0].equals("host.docker.internal") ? System.getenv("ip") : ipAndPort[0],
                  Integer.parseInt(ipAndPort[1]),
                  Constants.ETCD_META);
          metaStorageInfos.add(metaStorageInfo);
        }
        break;
      case Constants.ZOOKEEPER_META:
        metaStorageInfos = new ArrayList<>();
        String[] zookeepers = config.getZookeeperConnectionString().split(",");
        for (String zookeeper : zookeepers) {
          String[] ipAndPort = zookeeper.split(":", 2);
          MetaStorageInfo metaStorageInfo =
              new MetaStorageInfo(
                  ipAndPort[0].equals("host.docker.internal") ? System.getenv("ip") : ipAndPort[0],
                  Integer.parseInt(ipAndPort[1]),
                  Constants.ZOOKEEPER_META);
          metaStorageInfos.add(metaStorageInfo);
        }
        break;
      default:
        LOGGER.error("unexpected meta storage: {}", config.getMetaStorage());
    }

    if (metaStorageInfos != null && !metaStorageInfos.isEmpty()) {
      resp.setMetaStorageInfos(metaStorageInfos);
    }
    resp.setLocalMetaStorageInfo(localMetaStorageInfo);
    resp.setStatus(RpcUtils.SUCCESS);
    return resp;
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) {
    StatementExecutor executor = StatementExecutor.getInstance();
    RequestContext ctx = contextBuilder.build(req);
    executor.execute(ctx);
    queryManager.registerQuery(ctx.getId(), ctx);
    return ctx.getResult().getExecuteStatementResp(req.getFetchSize());
  }

  @Override
  public FetchResultsResp fetchResults(FetchResultsReq req) {
    RequestContext context = queryManager.getQuery(req.queryId);
    if (context == null) {
      return new FetchResultsResp(RpcUtils.SUCCESS, false);
    }
    return context.getResult().fetch(req.getFetchSize());
  }

  @Override
  public LoadCSVResp loadCSV(LoadCSVReq req) {
    StatementExecutor executor = StatementExecutor.getInstance();
    RequestContext ctx = contextBuilder.build(req);
    ctx.setLoadCSVFileByteBuffer(req.csvFile);
    executor.execute(ctx);
    return ctx.getResult().getLoadCSVResp();
  }

  @Override
  public LoadUDFResp loadUDF(LoadUDFReq req) {
    StatementExecutor executor = StatementExecutor.getInstance();
    RequestContext ctx = contextBuilder.build(req);
    ctx.setUDFModuleByteBuffer(req.udfFile);
    ctx.setRemoteUDF(req.isRemote);
    executor.execute(ctx);
    return ctx.getResult().getLoadUDFResp();
  }

  @Override
  public Status closeStatement(CloseStatementReq req) {
    queryManager.releaseQuery(req.queryId);
    return RpcUtils.SUCCESS;
  }

  @Override
  public CommitTransformJobResp commitTransformJob(CommitTransformJobReq req) {
    TransformJobManager manager = TransformJobManager.getInstance();
    CommitTransformJobResp resp = new CommitTransformJobResp();
    try {
      long jobId = manager.commit(req);

      if (jobId < 0) {
        resp.setStatus(RpcUtils.FAILURE);
      } else {
        resp.setStatus(RpcUtils.SUCCESS);
        resp.setJobId(jobId);
      }
    } catch (SecurityException e) {
      resp.setStatus(RpcUtils.ACCESS_DENY);
    }
    return resp;
  }

  @Override
  public QueryTransformJobStatusResp queryTransformJobStatus(QueryTransformJobStatusReq req) {
    TransformJobManager manager = TransformJobManager.getInstance();
    JobState jobState = manager.queryJobState(req.getJobId());
    if (jobState != null) {
      return new QueryTransformJobStatusResp(RpcUtils.SUCCESS, jobState);
    } else {
      return new QueryTransformJobStatusResp(RpcUtils.FAILURE, JobState.JOB_UNKNOWN);
    }
  }

  @Override
  public ShowEligibleJobResp showEligibleJob(ShowEligibleJobReq req) {
    TransformJobManager manager = TransformJobManager.getInstance();
    List<Long> jobIdList = manager.showEligibleJob(req.getJobState());
    return new ShowEligibleJobResp(RpcUtils.SUCCESS, jobIdList);
  }

  @Override
  public Status cancelTransformJob(CancelTransformJobReq req) {
    TransformJobManager manager = TransformJobManager.getInstance();
    boolean success = manager.cancel(req.getJobId());
    return success ? RpcUtils.SUCCESS : RpcUtils.FAILURE;
  }

  @Override
  public Status registerTask(RegisterTaskReq req) {
    List<UDFClassPair> pairs = req.getUDFClassPairs();
    String filePath = req.getFilePath();
    String errorMsg;
    Status status;

    boolean singleType = req.getTypesSize() == 1;

    Predicate<String> ruleNameFilter = FilePermissionRuleNameFilters.transformerRulesWithDefault();

    FilePermissionManager.Checker sourceChecker =
        FilePermissionManager.getInstance()
            .getChecker(null, ruleNameFilter, FileAccessType.EXECUTE);

    Optional<Path> sourceCheckedPath = sourceChecker.normalize(filePath);

    if (!sourceCheckedPath.isPresent()) {
      errorMsg = String.format("Register file %s has no execute permission", filePath);
      LOGGER.error(errorMsg);
      return RpcUtils.FAILURE.setMessage(errorMsg);
    }

    File sourceFile = sourceCheckedPath.get().toFile();
    if (!sourceFile.exists()) {
      if (!req.isRemote) {
        errorMsg = String.format("Register file not exist in declared path, path=%s", filePath);
        LOGGER.error(errorMsg);
        return RpcUtils.FAILURE.setMessage(errorMsg);
      }
    } else if (!req.isRemote) {
      // python file
      if (sourceFile.isFile() && !sourceFile.getName().endsWith(".py")) {
        errorMsg = "Register file must be a python file.";
        LOGGER.error(errorMsg);
        return RpcUtils.FAILURE.setMessage(errorMsg);
      }

      // python module dir, class name must contains '.'
      if (sourceFile.isDirectory()) {
        String className;
        for (UDFClassPair p : pairs) {
          className = p.classPath;
          if (!className.contains(".")) {
            errorMsg =
                "Class name must refer to a class in module if you are registering a python module directory. e.g.'module_name.file_name.class_name'.\n"
                    + className
                    + " is an invalid class name.";
            LOGGER.error(errorMsg);
            return RpcUtils.FAILURE.setMessage(errorMsg);
          }
        }
      }
    } else if (req.getModuleFile() == null || req.getModuleFile().length == 0) {
      errorMsg = "Read remote python module failed with no data.";
      LOGGER.error(errorMsg);
      return RpcUtils.FAILURE.setMessage(errorMsg);
    }

    List<TransformTaskMeta> transformTaskMetas = new ArrayList<>();
    for (UDFClassPair p : pairs) {
      TransformTaskMeta transformTaskMeta = metaManager.getTransformTask(p.name.trim());
      if (transformTaskMeta != null && transformTaskMeta.getIpSet().contains(config.getIp())) {
        errorMsg = String.format("Function %s already exist", transformTaskMeta);
        LOGGER.error(errorMsg);
        return RpcUtils.FAILURE.setMessage(errorMsg);
      }
      transformTaskMetas.add(transformTaskMeta);
    }

    String fileName = sourceFile.getName();
    String destPath =
        String.join(File.separator, config.getDefaultUDFDir(), "python_scripts", fileName);

    FilePermissionManager.Checker destChecker =
        FilePermissionManager.getInstance().getChecker(null, ruleNameFilter, FileAccessType.WRITE);

    Optional<Path> destCheckedPath = destChecker.normalize(destPath);
    if (!destCheckedPath.isPresent()) {
      errorMsg = String.format("Register file %s has no write permission", destPath);
      LOGGER.error(errorMsg);
      return RpcUtils.FAILURE.setMessage(errorMsg);
    }

    File destFile = destCheckedPath.get().toFile();

    if (destFile.exists()) {
      errorMsg = String.format("Register file(s) already exist, name=%s", fileName);
      LOGGER.error(errorMsg);
      return RpcUtils.FAILURE.setMessage(errorMsg);
    }

    try {
      if (req.isRemote) {
        status = loadRemoteUDFModule(req.moduleFile, destFile);
      } else {
        status = loadLocalUDFModule(sourceFile, destFile);
      }
      if (status.code != RpcUtils.SUCCESS.code) {
        return status;
      }
      if (sourceFile.isDirectory()) {
        // try to install module dependencies
        FunctionManager fm = FunctionManager.getInstance();
        fm.installReqsByPip(sourceFile.getName());
      }
    } catch (IOException e) {
      errorMsg =
          String.format(
              "Fail to %s register file(s), path=%s", req.isRemote ? "load" : "copy", destPath);
      LOGGER.error(errorMsg);
      return RpcUtils.FAILURE.setMessage(errorMsg);
    } catch (Exception e) {
      errorMsg =
          String.format(
              "Fail to install dependencies for %s. Please check if the requirements.txt in module is written correctly.",
              sourceFile.getName());
      LOGGER.error(errorMsg, e);
      LOGGER.debug("deleting {} due to failure in installing dependencies.", sourceFile.getPath());
      try {
        FileUtils.deleteFolder(destFile);
      } catch (IOException ee) {
        LOGGER.error("fail to delete udf module {}.", destFile.getPath(), ee);
      }
      return RpcUtils.FAILURE.setMessage(errorMsg);
    }

    UDFType type;
    TransformTaskMeta transformTaskMeta;
    for (int i = 0; i < transformTaskMetas.size(); i++) {
      type = singleType ? req.getTypes().get(0) : req.getTypes().get(i);
      transformTaskMeta = transformTaskMetas.get(i);
      if (transformTaskMeta != null) {
        transformTaskMeta.addIp(config.getIp());
        metaManager.updateTransformTask(transformTaskMeta);
      } else {
        metaManager.addTransformTask(
            new TransformTaskMeta(
                pairs.get(i).name,
                pairs.get(i).classPath,
                fileName,
                new HashSet<>(Collections.singletonList(config.getIp())),
                type));
      }
    }
    return RpcUtils.SUCCESS;
  }

  private Status loadLocalUDFModule(File sourceFile, File destFile) throws IOException {
    FileUtils.copyFileOrDir(sourceFile, destFile);
    return RpcUtils.SUCCESS;
  }

  private Status loadRemoteUDFModule(ByteBuffer moduleBuffer, File destFile) throws IOException {
    CompressionUtils.unzipFromByteBuffer(moduleBuffer, destFile.getParentFile());
    return RpcUtils.SUCCESS;
  }

  @Override
  public Status dropTask(DropTaskReq req) {
    String name = req.getName().trim();
    TransformTaskMeta transformTaskMeta = metaManager.getTransformTask(name);
    String errorMsg = "";
    if (transformTaskMeta == null) {
      errorMsg = "Function does not exist";
      LOGGER.error(errorMsg);
      return RpcUtils.FAILURE.setMessage(errorMsg);
    }

    TransformJobManager manager = TransformJobManager.getInstance();
    if (manager.isRegisterTaskRunning(name)) {
      errorMsg = String.format("Function %s is running.", name);
      LOGGER.error(errorMsg);
      return RpcUtils.FAILURE.setMessage(errorMsg);
    }

    if (!transformTaskMeta.getIpSet().contains(config.getIp())) {
      errorMsg = String.format("Function exists in node: %s", config.getIp());
      LOGGER.error(errorMsg);
      return RpcUtils.FAILURE.setMessage(errorMsg);
    }

    String filePath =
        config.getDefaultUDFDir()
            + File.separator
            + "python_scripts"
            + File.separator
            + transformTaskMeta.getFileName();

    Predicate<String> ruleNameFilter = FilePermissionRuleNameFilters.transformerRulesWithDefault();
    FilePermissionManager.Checker destChecker =
        FilePermissionManager.getInstance().getChecker(null, ruleNameFilter, FileAccessType.WRITE);
    Optional<Path> normalizedFile = destChecker.normalize(filePath);

    if (!normalizedFile.isPresent()) {
      errorMsg =
          String.format(
              "User has no write permission in target directory, task %s cannot be dropped.", name);
      LOGGER.error(errorMsg);
      return RpcUtils.FAILURE.setMessage(errorMsg);
    }

    File file = normalizedFile.get().toFile();

    if (!file.exists()) {
      metaManager.dropTransformTask(name);
      errorMsg = String.format("Register file not exist, path=%s", filePath);
      LOGGER.error(errorMsg);
      return RpcUtils.FAILURE.setMessage(errorMsg);
    }

    try {
      // if module/file only used by this task, delete its file(s)
      if (metaManager.getTransformTasksByModule(transformTaskMeta.getFileName()).size() == 1) {
        FileUtils.deleteFileOrDir(file);
      }
      metaManager.dropTransformTask(name);
      LOGGER.info("Register file has been dropped, path={}", filePath);
      return RpcUtils.SUCCESS;
    } catch (IOException e) {
      LOGGER.error("Fail to delete register file, path={}", filePath);
      return RpcUtils.FAILURE;
    }
  }

  @Override
  public GetRegisterTaskInfoResp getRegisterTaskInfo(GetRegisterTaskInfoReq req) {
    List<TransformTaskMeta> taskMetaList = metaManager.getTransformTasks();
    List<RegisterTaskInfo> taskInfoList = new ArrayList<>();
    for (TransformTaskMeta taskMeta : taskMetaList) {
      StringJoiner joiner = new StringJoiner(",");
      taskMeta.getIpSet().forEach(joiner::add);
      RegisterTaskInfo taskInfo =
          new RegisterTaskInfo(
              taskMeta.getName(),
              taskMeta.getClassName(),
              taskMeta.getFileName(),
              joiner.toString(),
              taskMeta.getType());
      taskInfoList.add(taskInfo);
    }
    GetRegisterTaskInfoResp resp = new GetRegisterTaskInfoResp(RpcUtils.SUCCESS);
    resp.setRegisterTaskInfoList(taskInfoList);
    return resp;
  }

  @Override
  public CurveMatchResp curveMatch(CurveMatchReq req) throws TException {
    QueryDataReq queryDataReq =
        new QueryDataReq(req.getSessionId(), req.getPaths(), req.getStartKey(), req.getEndKey());
    RequestContext ctx = contextBuilder.build(queryDataReq);
    executor.execute(ctx);
    QueryDataResp queryDataResp = ctx.getResult().getQueryDataResp();

    for (DataType type : queryDataResp.getDataTypeList()) {
      if (type.equals(DataType.BINARY) || type.equals(DataType.BOOLEAN)) {
        LOGGER.error("Unsupported data type: {}", type);
        return new CurveMatchResp(RpcUtils.FAILURE);
      }
    }

    List<Double> queryList =
        CurveMatchUtils.norm(
            CurveMatchUtils.calcShapePattern(req.getCurveQuery(), true, true, true, 0.1, 0.05));
    int maxWarpingWindow = (int) Math.ceil(queryList.size() / 4.0);
    List<Double> upper = CurveMatchUtils.getWindow(queryList, maxWarpingWindow, true);
    List<Double> lower = CurveMatchUtils.getWindow(queryList, maxWarpingWindow, false);

    List<String> paths = queryDataResp.getPaths();
    long[] queryTimestamps = getLongArrayFromByteBuffer(queryDataResp.getQueryDataSet().keys);
    List<List<Object>> values =
        ByteUtils.getValuesFromBufferAndBitmaps(
            queryDataResp.getDataTypeList(),
            queryDataResp.getQueryDataSet().getValuesList(),
            queryDataResp.getQueryDataSet().getBitmapList());

    double globalBestResult = Double.MAX_VALUE;
    long globalMatchedKey = 0L;
    String globalMatchedPath = "";

    for (int i = 0; i < paths.size(); i++) {
      List<Long> timestamps = new ArrayList<>();
      List<Double> value = new ArrayList<>();
      List<Integer> timestampsIndex = new ArrayList<>();
      int cnt = 0;
      for (int j = 0; j < queryTimestamps.length; j++) {
        if (values.get(j).get(i) != null) {
          timestamps.add(queryTimestamps[j]);
          value.add(ValueUtils.transformToDouble(values.get(j).get(i)));
          timestampsIndex.add(cnt);
          cnt++;
        }
      }
      List<Double> bestResultList = new CopyOnWriteArrayList<>();
      List<Long> matchedTimestampList = new CopyOnWriteArrayList<>();
      Collections.synchronizedList(timestampsIndex).stream()
          .parallel()
          .forEach(
              item -> {
                List<Double> fetchedValueList =
                    CurveMatchUtils.fetch(
                        timestamps, value, item, req.getCurveUnit(), req.getCurveQuerySize());
                if (fetchedValueList.size() == req.getCurveQuerySize()) {
                  List<Double> valueList =
                      CurveMatchUtils.calcShapePattern(
                          fetchedValueList, true, true, true, 0.1, 0.05);
                  double result =
                      CurveMatchUtils.calcDTW(
                          queryList, valueList, maxWarpingWindow, Double.MAX_VALUE, upper, lower);
                  bestResultList.add(result);
                  matchedTimestampList.add(timestamps.get(item));
                }
              });
      for (int j = 0; j < bestResultList.size(); j++) {
        if (bestResultList.get(j) < globalBestResult) {
          globalBestResult = bestResultList.get(j);
          globalMatchedKey = matchedTimestampList.get(j);
          globalMatchedPath = paths.get(i);
        }
      }
    }

    CurveMatchResp resp = new CurveMatchResp(RpcUtils.SUCCESS);
    resp.setMatchedKey(globalMatchedKey);
    resp.setMatchedPath(globalMatchedPath);
    return resp;
  }

  @Override
  public DebugInfoResp debugInfo(DebugInfoReq req) {
    byte[] payload = null;
    boolean parseFailure = false;
    switch (req.payloadType) {
      case GET_META:
        GetMetaReq getMetaReq;
        try {
          getMetaReq = JsonUtils.fromJson(req.getPayload(), GetMetaReq.class);
        } catch (RuntimeException e) {
          LOGGER.error("parse request failure: ", e);
          parseFailure = true;
          break;
        }
        payload = JsonUtils.toJson(getMeta(getMetaReq));
        break;
      default:
        Status status = new Status(RpcUtils.FAILURE.code);
        status.message = "unknown debug info type";
        return new DebugInfoResp(status);
    }
    if (parseFailure) {
      Status status = new Status(RpcUtils.FAILURE.code);
      status.message = "unknown payload for type " + req.payloadType;
      return new DebugInfoResp(status);
    }
    DebugInfoResp resp = new DebugInfoResp(RpcUtils.SUCCESS);
    resp.setPayload(payload);
    return resp;
  }

  public GetMetaResp getMeta(GetMetaReq req) {
    List<Storage> storages =
        metaManager.getStorageEngineList().stream()
            .map(e -> new Storage(e.getId(), e.getIp(), e.getPort(), e.getStorageEngine()))
            .collect(Collectors.toList());
    List<StorageUnit> units =
        metaManager.getStorageUnits().stream()
            .map(u -> new StorageUnit(u.getId(), u.getMasterId(), u.getStorageEngineId()))
            .collect(Collectors.toList());
    List<Fragment> fragments =
        metaManager.getFragments().stream()
            .map(
                f ->
                    new Fragment(
                        f.getMasterStorageUnitId(),
                        f.getKeyInterval().getStartKey(),
                        f.getKeyInterval().getEndKey(),
                        f.getColumnsInterval().getStartColumn(),
                        f.getColumnsInterval().getEndColumn()))
            .collect(Collectors.toList());
    return new GetMetaResp(fragments, storages, units);
  }

  @Override
  public ShowSessionIDResp showSessionID(ShowSessionIDReq req) {
    List<Long> sessionIDs = new ArrayList<>(SessionManager.getInstance().getSessionIds());
    return new ShowSessionIDResp(RpcUtils.SUCCESS, sessionIDs);
  }

  @Override
  public ShowRulesResp showRules(ShowRulesReq req) {
    try {
      IRuleCollection ruleCollection = getRuleCollection();
      return new ShowRulesResp(RpcUtils.SUCCESS, ruleCollection.getRulesInfo());
    } catch (ClassNotFoundException e) {
      LOGGER.error("show rules failed: ", e);
      return new ShowRulesResp(RpcUtils.FAILURE, null);
    }
  }

  @Override
  public Status setRules(SetRulesReq req) {
    Map<String, Boolean> rulesChange = req.getRulesChange();
    try {
      getRuleCollection().setRules(rulesChange);
      return RpcUtils.SUCCESS;
    } catch (ClassNotFoundException e) {
      LOGGER.error("set rules failed: ", e);
      return RpcUtils.FAILURE;
    }
  }

  private IRuleCollection getRuleCollection() throws ClassNotFoundException {
    // 获取接口的类加载器
    ClassLoader classLoader = IRuleCollection.class.getClassLoader();
    // 加载枚举类
    Class<?> enumClass =
        classLoader.loadClass("cn.edu.tsinghua.iginx.logical.optimizer.rules.RuleCollection");
    // 获取枚举实例
    Object enumInstance = enumClass.getEnumConstants()[0];

    // 强制转换为接口类型
    return (IRuleCollection) enumInstance;
  }
}
