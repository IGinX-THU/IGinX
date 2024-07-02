package cn.edu.tsinghua.iginx.metadata.entity;

import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class StorageEngineMeta {

  /** 数据库的 id */
  private long id;

  /** 数据库所在的 ip */
  private final String ip;

  /** 数据库开放的端口 */
  private final int port;

  private final boolean readOnly;

  private final String schemaPrefix;

  private final String dataPrefix;

  private final boolean hasData;

  private StorageUnitMeta dummyStorageUnit;

  private FragmentMeta dummyFragment;

  /** 数据库需要的其他参数信息，例如用户名、密码等 */
  private Map<String, String> extraParams;

  /** 数据库类型 */
  private final StorageEngineType storageEngine;

  /** 实例上管理的存储单元列表 */
  private transient List<StorageUnitMeta> storageUnitList = new ArrayList<>();

  private final long createdBy;

  private boolean needReAllocate;

  public StorageEngineMeta(
      long id,
      String ip,
      int port,
      Map<String, String> extraParams,
      StorageEngineType storageEngine,
      long createdBy) {
    this(id, ip, port, false, null, null, false, extraParams, storageEngine, createdBy);
  }

  public StorageEngineMeta(
      long id,
      String ip,
      int port,
      boolean hasData,
      String dataPrefix,
      boolean readOnly,
      Map<String, String> extraParams,
      StorageEngineType storageEngine,
      long createdBy) {
    this(
        id,
        ip,
        port,
        hasData,
        dataPrefix,
        null,
        readOnly,
        null,
        null,
        extraParams,
        storageEngine,
        createdBy,
        false);
  }

  public StorageEngineMeta(
      long id,
      String ip,
      int port,
      boolean hasData,
      String dataPrefix,
      String schemaPrefix,
      boolean readOnly,
      Map<String, String> extraParams,
      StorageEngineType storageEngine,
      long createdBy) {
    this(
        id,
        ip,
        port,
        hasData,
        dataPrefix,
        schemaPrefix,
        readOnly,
        null,
        null,
        extraParams,
        storageEngine,
        createdBy,
        false);
  }

  public StorageEngineMeta(
      long id,
      String ip,
      int port,
      boolean hasData,
      String dataPrefix,
      String schemaPrefix,
      boolean readOnly,
      StorageUnitMeta dummyStorageUnit,
      FragmentMeta dummyFragment,
      Map<String, String> extraParams,
      StorageEngineType storageEngine,
      long createdBy,
      boolean needReAllocate) {
    this.id = id;
    this.ip = ip;
    this.port = port;
    this.hasData = hasData;
    this.dataPrefix = dataPrefix;
    this.readOnly = readOnly;
    this.dummyStorageUnit = dummyStorageUnit;
    this.dummyFragment = dummyFragment;
    this.extraParams = extraParams;
    this.storageEngine = storageEngine;
    this.createdBy = createdBy;
    this.needReAllocate = needReAllocate;
    this.schemaPrefix = schemaPrefix;
  }

  public StorageEngineMeta(
      long id,
      String ip,
      int port,
      boolean hasData,
      String dataPrefix,
      String schemaPrefix,
      boolean readOnly,
      StorageUnitMeta dummyStorageUnit,
      FragmentMeta dummyFragment,
      Map<String, String> extraParams,
      StorageEngineType storageEngine,
      List<StorageUnitMeta> storageUnitList,
      long createdBy,
      boolean needReAllocate) {
    this.id = id;
    this.ip = ip;
    this.port = port;
    this.hasData = hasData;
    this.dataPrefix = dataPrefix;
    this.readOnly = readOnly;
    this.dummyStorageUnit = dummyStorageUnit;
    this.dummyFragment = dummyFragment;
    this.extraParams = extraParams;
    this.storageEngine = storageEngine;
    this.storageUnitList = storageUnitList;
    this.createdBy = createdBy;
    this.needReAllocate = needReAllocate;
    this.schemaPrefix = schemaPrefix;
  }

  public String getSchemaPrefix() {
    return schemaPrefix;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
    if (hasData) {
      dummyStorageUnit.setStorageEngineId(id);
    }
  }

  public String getIp() {
    return ip;
  }

  public int getPort() {
    return port;
  }

  public Map<String, String> getExtraParams() {
    return extraParams;
  }

  public void setExtraParams(Map<String, String> extraParams) {
    this.extraParams = extraParams;
  }

  public StorageEngineType getStorageEngine() {
    return storageEngine;
  }

  public List<StorageUnitMeta> getStorageUnitList() {
    if (storageUnitList == null) {
      storageUnitList = new ArrayList<>();
    }
    return storageUnitList;
  }

  public void removeStorageUnit(String id) {
    if (storageUnitList == null) {
      storageUnitList = new ArrayList<>();
    }
    storageUnitList.removeIf(e -> e.getId().equals(id));
  }

  public void addStorageUnit(StorageUnitMeta storageUnit) {
    if (storageUnitList == null) {
      storageUnitList = new ArrayList<>();
    }
    storageUnitList.add(storageUnit);
  }

  public boolean isHasData() {
    return hasData;
  }

  public String getDataPrefix() {
    return dataPrefix;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  public StorageUnitMeta getDummyStorageUnit() {
    return dummyStorageUnit;
  }

  public FragmentMeta getDummyFragment() {
    return dummyFragment;
  }

  public void setDummyStorageUnit(StorageUnitMeta dummyStorageUnit) {
    this.dummyStorageUnit = dummyStorageUnit;
  }

  public void setDummyFragment(FragmentMeta dummyFragment) {
    this.dummyFragment = dummyFragment;
  }

  public long getCreatedBy() {
    return createdBy;
  }

  public boolean isNeedReAllocate() {
    return needReAllocate;
  }

  public void setNeedReAllocate(boolean needReAllocate) {
    this.needReAllocate = needReAllocate;
  }

  @Override
  public String toString() {
    return "StorageEngineMeta {"
        + "ip='"
        + ip
        + '\''
        + ", port="
        + port
        + ", type='"
        + storageEngine.toString()
        + '\''
        + '}';
  }
}
