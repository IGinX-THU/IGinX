package cn.edu.tsinghua.iginx.metadata;

import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.metadata.exception.MetaStorageException;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.StorageUnitHook;
import cn.edu.tsinghua.iginx.policy.simple.ColumnCalDO;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;

public class MetaManagerMock implements IMetaManager {

  private final List<FragmentMeta> fragments = new ArrayList<>();

  private Map<ColumnsInterval, List<FragmentMeta>> getFragmentMapByColumnsIntervalMockMap =
      new HashMap<>();

  private static volatile MetaManagerMock INSTANCE;

  private MetaManagerMock() {}

  public static MetaManagerMock getInstance() {
    if (INSTANCE == null) {
      synchronized (MetaManagerMock.class) {
        if (INSTANCE == null) {
          INSTANCE = new MetaManagerMock();
        }
      }
    }
    return INSTANCE;
  }

  @Override
  public boolean addStorageEngines(List<StorageEngineMeta> storageEngineMetas) {
    return false;
  }

  @Override
  public boolean removeDummyStorageEngine(long storageEngineId) {
    return false;
  }

  @Override
  public List<StorageEngineMeta> getStorageEngineList() {
    return null;
  }

  @Override
  public List<StorageEngineMeta> getWritableStorageEngineList() {
    return null;
  }

  @Override
  public int getStorageEngineNum() {
    return 0;
  }

  @Override
  public StorageEngineMeta getStorageEngine(long id) {
    return null;
  }

  @Override
  public StorageUnitMeta getStorageUnit(String id) {
    return null;
  }

  @Override
  public Map<String, StorageUnitMeta> getStorageUnits(Set<String> ids) {
    return null;
  }

  @Override
  public List<StorageUnitMeta> getStorageUnits() {
    return null;
  }

  @Override
  public List<IginxMeta> getIginxList() {
    return null;
  }

  @Override
  public long getIginxId() {
    return 0;
  }

  @Override
  public List<FragmentMeta> getFragments() {
    return fragments;
  }

  @Override
  public List<FragmentMeta> getFragmentsByStorageUnit(String storageUnitId) {
    return null;
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorageUnit(String storageUnitId) {
    return null;
  }

  public void setGetFragmentMapByColumnsIntervalMockMap(
      Map<ColumnsInterval, List<FragmentMeta>> getFragmentMapByColumnsIntervalMockMap) {
    this.getFragmentMapByColumnsIntervalMockMap.clear();
    this.getFragmentMapByColumnsIntervalMockMap = getFragmentMapByColumnsIntervalMockMap;
  }

  @Override
  public Map<ColumnsInterval, List<FragmentMeta>> getFragmentMapByColumnsInterval(
      ColumnsInterval columnsInterval) {
    return getFragmentMapByColumnsInterval(columnsInterval, false);
  }

  @Override
  public Map<ColumnsInterval, List<FragmentMeta>> getFragmentMapByColumnsInterval(
      ColumnsInterval columnsInterval, boolean withDummyFragment) {
    Map<ColumnsInterval, List<FragmentMeta>> res = new HashMap<>();
    for (ColumnsInterval interval : getFragmentMapByColumnsIntervalMockMap.keySet()) {
      if (interval.isIntersect(columnsInterval)) {
        res.put(interval, getFragmentMapByColumnsIntervalMockMap.get(interval));
      }
    }

    return res;
  }

  @Override
  public boolean hasDummyFragment(ColumnsInterval columnsInterval) {
    return false;
  }

  @Override
  public boolean hasWritableStorageEngines() {
    return false;
  }

  @Override
  public Map<ColumnsInterval, FragmentMeta> getLatestFragmentMapByColumnsInterval(
      ColumnsInterval columnsInterval) {
    return null;
  }

  @Override
  public Map<ColumnsInterval, FragmentMeta> getLatestFragmentMap() {
    return null;
  }

  @Override
  public Map<ColumnsInterval, List<FragmentMeta>> getFragmentMapByColumnsIntervalAndKeyInterval(
      ColumnsInterval columnsInterval, KeyInterval keyInterval) {
    return null;
  }

  @Override
  public Map<ColumnsInterval, List<FragmentMeta>> getFragmentMapByColumnsIntervalAndKeyInterval(
      ColumnsInterval columnsInterval, KeyInterval keyInterval, boolean withDummyFragment) {
    return null;
  }

  @Override
  public List<FragmentMeta> getFragmentListByColumnName(String colName) {
    return null;
  }

  @Override
  public FragmentMeta getLatestFragmentByColumnName(String colName) {
    return null;
  }

  @Override
  public List<FragmentMeta> getFragmentListByColumnNameAndKeyInterval(
      String colName, KeyInterval keyInterval) {
    return null;
  }

  @Override
  public boolean createFragmentsAndStorageUnits(
      List<StorageUnitMeta> storageUnits, List<FragmentMeta> fragments) {
    return false;
  }

  @Override
  public FragmentMeta splitFragmentAndStorageUnit(
      StorageUnitMeta toAddStorageUnit, FragmentMeta toAddFragment, FragmentMeta fragment) {
    return null;
  }

  @Override
  public boolean hasFragment() {
    return false;
  }

  @Override
  public boolean createInitialFragmentsAndStorageUnits(
      List<StorageUnitMeta> storageUnits, List<FragmentMeta> initialFragments) {
    return false;
  }

  @Override
  public StorageUnitMeta generateNewStorageUnitMetaByFragment(
      FragmentMeta fragmentMeta, long targetStorageId) throws MetaStorageException {
    return null;
  }

  @Override
  public List<Long> selectStorageEngineIdList() {
    return null;
  }

  @Override
  public void registerStorageEngineChangeHook(StorageEngineChangeHook hook) {}

  @Override
  public void addOrUpdateSchemaMapping(String schema, Map<String, Integer> schemaMapping) {}

  @Override
  public void addOrUpdateSchemaMappingItem(String schema, String key, int value) {}

  @Override
  public Map<String, Integer> getSchemaMapping(String schema) {
    return null;
  }

  @Override
  public int getSchemaMappingItem(String schema, String key) {
    return 0;
  }

  @Override
  public boolean addUser(UserMeta user) {
    return false;
  }

  @Override
  public boolean updateUser(String username, String password, Set<AuthType> auths) {
    return false;
  }

  @Override
  public boolean removeUser(String username) {
    return false;
  }

  @Override
  public UserMeta getUser(String username) {
    return null;
  }

  @Override
  public List<UserMeta> getUsers() {
    return null;
  }

  @Override
  public List<UserMeta> getUsers(List<String> username) {
    return null;
  }

  @Override
  public void registerStorageUnitHook(StorageUnitHook hook) {}

  @Override
  public boolean election() {
    return false;
  }

  @Override
  public void saveColumnsData(InsertStatement statement) {}

  @Override
  public List<ColumnCalDO> getMaxValueFromColumns() {
    return null;
  }

  @Override
  public Map<String, Double> getColumnsData() {
    return null;
  }

  @Override
  public int updateVersion() {
    return 0;
  }

  @Override
  public Map<Integer, Integer> getColumnsVersionMap() {
    return null;
  }

  @Override
  public boolean addTransformTask(TransformTaskMeta transformTask) {
    return false;
  }

  @Override
  public boolean updateTransformTask(TransformTaskMeta transformTask) {
    return false;
  }

  @Override
  public boolean dropTransformTask(String name) {
    return false;
  }

  @Override
  public TransformTaskMeta getTransformTask(String name) {
    return null;
  }

  @Override
  public List<TransformTaskMeta> getTransformTasks() {
    return null;
  }

  @Override
  public List<TransformTaskMeta> getTransformTasksByModule(String moduleName) {
    return null;
  }

  @Override
  public void updateFragmentRequests(
      Map<FragmentMeta, Long> writeRequestsMap, Map<FragmentMeta, Long> readRequestsMap)
      throws Exception {}

  @Override
  public void updateFragmentHeat(
      Map<FragmentMeta, Long> writeHotspotMap, Map<FragmentMeta, Long> readHotspotMap)
      throws Exception {}

  @Override
  public Pair<Map<FragmentMeta, Long>, Map<FragmentMeta, Long>> loadFragmentHeat()
      throws Exception {
    return null;
  }

  @Override
  public void updateFragmentPoints(FragmentMeta fragmentMeta, long points) {}

  @Override
  public Map<FragmentMeta, Long> loadFragmentPoints() throws Exception {
    return null;
  }

  @Override
  public void clearMonitors() {}

  @Override
  public void removeFragment(FragmentMeta fragmentMeta) {
    fragments.remove(fragmentMeta);
  }

  @Override
  public void addFragment(FragmentMeta fragmentMeta) {
    fragments.add(fragmentMeta);
  }

  @Override
  public void endFragmentByColumnsInterval(FragmentMeta fragmentMeta, String endColumn) {}

  @Override
  public void updateFragmentByColumnsInterval(
      ColumnsInterval columnsInterval, FragmentMeta fragmentMeta) {}

  @Override
  public void updateMaxActiveEndKey(long endKey) {}

  @Override
  public long getMaxActiveEndKey() {
    return 0;
  }

  @Override
  public void submitMaxActiveEndKey() {}

  @Override
  public List<StorageEngineMeta> getStorageEngineListFromConf() {
    return null;
  }
}
