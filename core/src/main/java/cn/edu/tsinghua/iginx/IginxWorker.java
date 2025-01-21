/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
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
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.exec.TransformJobManager;
import cn.edu.tsinghua.iginx.utils.*;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
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

  // to init scheduled jobs
  private final TransformJobManager transformJobManager = TransformJobManager.getInstance();

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private IginxWorker() {
    // if there are new local filesystem in conf, add them to cluster.
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
        if (metaFromConf.equals(meta)) {
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
  public Status removeStorageEngine(RemoveStorageEngineReq req) {
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
        partialFailAndLog(status, String.format("dummy storage engine %s does not exist.", info));
        continue;
      }
      if (!storageEngineMeta.isHasData()) {
        partialFailAndLog(status, String.format("dummy storage engine %s has no data.", info));
        continue;
      }
      if (!storageEngineMeta.isReadOnly()) {
        partialFailAndLog(status, String.format("dummy storage engine %s is not read-only.", info));
        continue;
      }
      // 更新 zk 以及缓存中的元数据信息
      if (!metaManager.removeDummyStorageEngine(storageEngineMeta.getId())) {
        partialFailAndLog(
            status,
            String.format("unexpected error during removing dummy storage engine %s.", info));
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
      // 仅add时，默认为true
      boolean hasData = Boolean.parseBoolean(extraParams.getOrDefault(Constants.HAS_DATA, "true"));
      String dataPrefix = null;
      if (hasData && extraParams.containsKey(Constants.DATA_PREFIX)) {
        dataPrefix = extraParams.get(Constants.DATA_PREFIX);
      }
      // 仅add时，默认为true
      boolean readOnly =
          Boolean.parseBoolean(extraParams.getOrDefault(Constants.IS_READ_ONLY, "true"));

      if (!isValidHost(ip)) { // IP 不合法
        partialFailAndLog(status, String.format("ip %s is invalid.", ip));
        continue;
      }
      if (isEmbeddedStorageEngine(type) && !isLocalHost(ip)) { // 非本地的文件系统引擎不可注册
        partialFailAndLog(
            status, String.format("redirect to %s:%s.", ip, extraParams.get("iginx_port")));
        continue;
      }
      if (!hasData & readOnly) { // 无意义的存储引擎：不带数据且只读
        partialFailAndLog(
            status,
            String.format("normal storage engine %s should not be read-only.", storageEngine));
        continue;
      }
      if (!checkEmbeddedStorageExtraParams(type, extraParams)) { // 参数不合法
        partialFailAndLog(
            status,
            String.format(
                "missing params or providing invalid ones for %s in statement.", storageEngine));
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
        status
            .setCode(RpcUtils.FAILURE.code)
            .setMessage("No valid engine can be registered. Detailed information:\n");
        appendFullMsg(status);
        return status;
      } else {
        status.setCode(RpcUtils.PARTIAL_SUCCESS.code); // 部分请求失败，message待后续处理时设置
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
      List<StorageEngineMeta> storageEnginesToBeRemoved = new ArrayList<>();
      for (StorageEngineMeta storageEngine : storageEngineMetas) {
        for (StorageEngineMeta currentStorageEngine : currentStorageEngines) {
          if (storageEngine.equals(currentStorageEngine)) {
            // 存在相同数据库
            storageEnginesToBeRemoved.add(storageEngine);
            partialFailAndLog(
                status, String.format("repeatedly add storage engine %s.", storageEngine));
            break;
          } else if (storageEngine.isSameAddress(currentStorageEngine)) {
            LOGGER.debug(
                "same address engine {} for new engine {}.", currentStorageEngine, storageEngine);
            // 已有相同IP、端口的数据库
            if (StorageManager.testEngineConnection(currentStorageEngine)) {
              LOGGER.debug("old engine can be connected");
              // 已有的数据库仍可连接
              if (currentStorageEngine.contains(storageEngine)) {
                // 已有数据库能够覆盖新注册的数据库，拒绝注册
                storageEnginesToBeRemoved.add(storageEngine);
                partialFailAndLog(
                    status,
                    String.format(
                        "engine:%s would not be registered: duplicate data coverage detected. Please check existing engines.",
                        storageEngine));
              }
            } else {
              LOGGER.debug("old engine cannot be connected");
              // 已有的数据库无法连接了，若是只读，直接删除
              if (currentStorageEngine.isReadOnly() && currentStorageEngine.isHasData()) {
                metaManager.removeDummyStorageEngine(currentStorageEngine.getId());
                LOGGER.warn(
                    "Existing dummy Storage engine {} cannot be connected and will be removed.",
                    currentStorageEngine);
              } else {
                // 并非只读，需要手动操作，拒绝注册同地址引擎
                storageEnginesToBeRemoved.add(storageEngine);
                partialFailAndLog(
                    status,
                    String.format(
                        "Existing Storage engine %s cannot be connected. New engine %s will not be registered.",
                        currentStorageEngine, storageEngine));
              }
            }
          }
        }
      }
      if (!storageEnginesToBeRemoved.isEmpty()) {
        storageEngineMetas.removeAll(storageEnginesToBeRemoved);
        if (storageEngineMetas.isEmpty()) {
          status
              .setCode(RpcUtils.FAILURE.code)
              .setMessage("No valid engine can be registered. Detailed information:\n");
          appendFullMsg(status);
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

    Iterator<StorageEngineMeta> iterator = storageEngineMetas.iterator();
    while (iterator.hasNext()) {
      // 首先去掉ip不合要求的本地引擎（本地引擎必须在同地址的IGinX节点注册）
      StorageEngineMeta meta = iterator.next();
      if (isEmbeddedStorageEngine(meta.getStorageEngine())) {
        if (!isLocal(meta)) {
          partialFailAndLog(status, String.format("storage engine %s needs to be local.", meta));
          iterator.remove();
          continue;
        }
      }
      // 然后设置dummy信息
      if (meta.isHasData()) {
        String dataPrefix = meta.getDataPrefix();
        String schemaPrefix = meta.getSchemaPrefix();
        StorageUnitMeta dummyStorageUnit = new StorageUnitMeta(generateDummyStorageUnitId(0), -1);
        Pair<ColumnsInterval, KeyInterval> boundary =
            StorageManager.getBoundaryOfStorage(meta, dataPrefix);
        if (boundary == null) {
          partialFailAndLog(
              status,
              String.format(
                  "Failed to read data in dummy storage engine %s. Please check params:%s;%s.",
                  meta.getStorageEngine(), meta, meta.getExtraParams()));
          iterator.remove();
          continue;
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

    iterator = storageEngineMetas.iterator();
    StorageManager storageManager = PhysicalEngineImpl.getInstance().getStorageManager();
    while (iterator.hasNext()) {
      StorageEngineMeta meta = iterator.next();
      // 为什么本地文件系统必须先init instance，再加入meta，storageManager：当数据源信息被加入meta，集群内其他节点都会立刻去尝试连接本地文件引擎的服务
      // 因此必须先init开启服务，然后在加入meta时获取唯一数据源id，再将id和引擎送入storageManager进行登记
      // 其他类型的引擎也需要先init初始化，以在修改元数据前确保引擎可用
      IStorage storage = StorageManager.initStorageInstance(meta);
      if (storage == null) {
        partialFailAndLog(status, String.format("init storage engine %s failed", meta));
        iterator.remove();
        continue;
      }
      if (!metaManager.addStorageEngines(Collections.singletonList(meta))) {
        partialFailAndLog(status, String.format("add storage engine %s failed.", meta));
        iterator.remove();
        continue;
      }
      storageManager.addStorage(meta, storage);
    }
    if (status.isSetSubStatus()) {
      if (storageEngineMetas.isEmpty()) {
        // 所有请求均失败
        status
            .setCode(RpcUtils.FAILURE.code)
            .setMessage("No valid engine can be registered. Detailed information:\n");
        appendFullMsg(status);
      } else {
        // 部分请求失败
        status
            .setCode(RpcUtils.PARTIAL_SUCCESS.code)
            .setMessage("Some of the engines cannot be registered. Detailed information:\n");
        appendFullMsg(status);
      }
    }
  }

  /** add failed sub status and log the message */
  private static void partialFailAndLog(Status status, String errMsg) {
    LOGGER.error(errMsg);
    status.addToSubStatus(
        new Status(StatusCode.STATEMENT_EXECUTION_ERROR.getStatusCode()).setMessage(errMsg));
  }

  /** append messages of sub status to the message of main status */
  private static void appendFullMsg(Status status) {
    StringBuilder msg = new StringBuilder(status.getMessage());
    for (Status subStatus : status.getSubStatus()) {
      msg.append("* ").append(subStatus.getMessage()).append("\n");
    }
    status.setMessage(String.valueOf(msg));
  }

  /** This function is only for read-only dummy, temporarily */
  @Override
  public Status alterStorageEngine(AlterStorageEngineReq req) {
    if (!sessionManager.checkSession(req.getSessionId(), AuthType.Cluster)) {
      return RpcUtils.ACCESS_DENY;
    }
    Status status = new Status(RpcUtils.SUCCESS.code);
    long targetId = req.getEngineId();
    Map<String, String> newParams = req.getNewParams();
    StorageEngineMeta targetMeta = metaManager.getStorageEngine(targetId);
    if (targetMeta == null) {
      status.setCode(RpcUtils.FAILURE.code);
      status.setMessage("No engine found with id:" + targetId);
      return status;
    }
    if (!targetMeta.isHasData() || !targetMeta.isReadOnly()) {
      status.setCode(RpcUtils.FAILURE.code);
      status.setMessage(
          "Only read-only & dummy engines' params can be altered. Engine with id("
              + targetId
              + ") cannot be altered.");
      return status;
    }

    // update meta info
    if (newParams.remove(Constants.IP) != null
        || newParams.remove(Constants.PORT) != null
        || newParams.remove(Constants.DATA_PREFIX) != null
        || newParams.remove(Constants.SCHEMA_PREFIX) != null) {
      status.setCode(RpcUtils.FAILURE.code);
      status.setMessage(
          "IP, port, type, data_prefix, schema_prefix cannot be altered. Removing and adding new engine is recommended.");
      return status;
    }
    targetMeta.updateExtraParams(newParams);

    // remove, then add
    if (!metaManager.removeDummyStorageEngine(targetId)) {
      LOGGER.error("unexpected error during removing dummy storage engine {}.", targetMeta);
      status.setCode(RpcUtils.FAILURE.code);
      status.setMessage("unexpected error occurred. Please check server log.");
      return status;
    }

    addStorageEngineMetas(Collections.singletonList(targetMeta), status, true);
    return status;
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
    if (req.isSetRemoteSession()) {
      ctx.setRemoteSession(req.isRemoteSession());
    }
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
    ctx.setLoadCSVFileName(req.csvFileName);
    executor.execute(ctx);
    return ctx.getResult().getLoadCSVResp();
  }

  @Override
  public LoadUDFResp loadUDF(LoadUDFReq req) {
    StatementExecutor executor = StatementExecutor.getInstance();
    RequestContext ctx = contextBuilder.build(req);
    ctx.setUDFModuleByteBuffer(req.udfFile);
    ctx.setRemoteSession(req.isRemote);
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
    Map<JobState, List<Long>> jobStateMap = manager.showEligibleJob(req.getJobState());
    return new ShowEligibleJobResp(RpcUtils.SUCCESS, jobStateMap);
  }

  @Override
  public Status cancelTransformJob(CancelTransformJobReq req) {
    TransformJobManager manager = TransformJobManager.getInstance();
    try {
      boolean success = manager.cancel(req.getJobId());
      return success ? RpcUtils.SUCCESS : RpcUtils.FAILURE;
    } catch (TransformException e) {
      return new Status(RpcUtils.FAILURE).setMessage(e.getMessage());
    }
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
      if (transformTaskMeta != null
          && transformTaskMeta.containsIpPort(config.getIp(), config.getPort())) {
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
        transformTaskMeta.addIpPort(config.getIp(), config.getPort());
        metaManager.updateTransformTask(transformTaskMeta);
      } else {
        LOGGER.debug(
            "Registering {} task: {} as {} in {}; iginx: {}:{}",
            type,
            pairs.get(i).classPath,
            pairs.get(i).name,
            fileName,
            config.getIp(),
            config.getPort());
        metaManager.addTransformTask(
            new TransformTaskMeta(
                pairs.get(i).name,
                pairs.get(i).classPath,
                fileName,
                config.getIp(),
                config.getPort(),
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

    if (!transformTaskMeta.containsIpPort(config.getIp(), config.getPort())) {
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
      FunctionManager.getInstance().removeFunction(name);
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
    List<IpPortPair> ipPortPairs;
    for (TransformTaskMeta taskMeta : taskMetaList) {
      ipPortPairs =
          taskMeta.getIpPortSet().stream()
              .map(p -> new IpPortPair(p.getK(), p.getV()))
              .collect(Collectors.toList());
      RegisterTaskInfo taskInfo =
          new RegisterTaskInfo(
              taskMeta.getName(),
              taskMeta.getClassName(),
              taskMeta.getFileName(),
              ipPortPairs,
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
    } catch (Exception e) {
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
    } catch (Exception e) {
      LOGGER.error("set rules failed: ", e);
      return RpcUtils.FAILURE;
    }
  }

  private IRuleCollection getRuleCollection()
      throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
    // 获取接口的类加载器
    ClassLoader classLoader = IRuleCollection.class.getClassLoader();
    // 加载枚举类
    Class<?> ruleCollectionClass =
        classLoader.loadClass("cn.edu.tsinghua.iginx.logical.optimizer.rules.RuleCollection");
    // get INSTANCE static field
    Object enumInstance = ruleCollectionClass.getField("INSTANCE").get(null);

    // 强制转换为接口类型
    return (IRuleCollection) enumInstance;
  }

  @Override
  public UploadFileResp uploadFileChunk(UploadFileReq req) {
    FileChunk chunk = req.getFileChunk();
    Status status = new Status();

    String filename = chunk.fileName;
    if (filename.contains("..") || filename.contains("/") || filename.contains("\\")) {
      status.setCode(RpcUtils.FAILURE.code);
      status.setMessage("Invalid filename");
      return new UploadFileResp(status);
    }

    String filepath = String.join(File.separator, System.getProperty("java.io.tmpdir"), filename);
    try {
      File file = new File(filepath);
      try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
        raf.seek(chunk.offset);
        raf.write(chunk.data.array());
        LOGGER.debug(
            "write {} bytes to file {} at offset {}",
            chunk.data.array().length,
            file,
            chunk.offset);
      }
      status.setCode(RpcUtils.SUCCESS.code);
      return new UploadFileResp(status);
    } catch (IOException e) {
      status.setCode(RpcUtils.FAILURE.code);
      status.setMessage("File chunk upload failed. Caused by: " + e.getMessage());
      return new UploadFileResp(status);
    }
  }
}
