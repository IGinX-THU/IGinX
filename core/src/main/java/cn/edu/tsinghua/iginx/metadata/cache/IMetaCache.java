package cn.edu.tsinghua.iginx.metadata.cache;

import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.policy.simple.ColumnCalDO;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IMetaCache {

  boolean enableFragmentCacheControl();

  // 分片相关的缓存读写接口
  void initFragment(Map<ColumnsInterval, List<FragmentMeta>> fragmentListMap);

  void addFragment(FragmentMeta fragmentMeta);

  void updateFragment(FragmentMeta fragmentMeta);

  void updateFragmentByColumnsInterval(ColumnsInterval columnsInterval, FragmentMeta fragmentMeta);

  void deleteFragmentByColumnsInterval(ColumnsInterval columnsInterval, FragmentMeta fragmentMeta);

  List<FragmentMeta> getFragmentMapByExactColumnsInterval(ColumnsInterval columnsInterval);

  Map<ColumnsInterval, List<FragmentMeta>> getFragmentMapByColumnsInterval(
      ColumnsInterval columnsInterval);

  List<FragmentMeta> getDummyFragmentsByColumnsInterval(ColumnsInterval columnsInterval);

  Map<ColumnsInterval, FragmentMeta> getLatestFragmentMap();

  Map<ColumnsInterval, FragmentMeta> getLatestFragmentMapByColumnsInterval(
      ColumnsInterval columnsInterval);

  Map<ColumnsInterval, List<FragmentMeta>> getFragmentMapByColumnsIntervalAndKeyInterval(
      ColumnsInterval columnsInterval, KeyInterval keyInterval);

  List<FragmentMeta> getDummyFragmentsByColumnsIntervalAndKeyInterval(
      ColumnsInterval columnsInterval, KeyInterval keyInterval);

  List<FragmentMeta> getFragmentListByColumnName(String columnName);

  FragmentMeta getLatestFragmentByColumnName(String columnName);

  List<FragmentMeta> getFragmentListByColumnNameAndKeyInterval(
      String columnName, KeyInterval keyInterval);

  List<FragmentMeta> getFragmentListByStorageUnitId(String storageUnitId);

  boolean hasFragment();

  long getFragmentMinKey();

  // 数据单元相关的缓存读写接口
  boolean hasStorageUnit();

  void initStorageUnit(Map<String, StorageUnitMeta> storageUnits);

  StorageUnitMeta getStorageUnit(String id);

  Map<String, StorageUnitMeta> getStorageUnits(Set<String> ids);

  List<StorageUnitMeta> getStorageUnits();

  void addStorageUnit(StorageUnitMeta storageUnitMeta);

  void updateStorageUnit(StorageUnitMeta storageUnitMeta);

  // iginx 相关的缓存读写接口
  List<IginxMeta> getIginxList();

  void addIginx(IginxMeta iginxMeta);

  void removeIginx(long id);

  // 数据后端相关的缓存读写接口
  void addStorageEngine(StorageEngineMeta storageEngineMeta);

  boolean removeDummyStorageEngine(long storageEngineId);

  List<StorageEngineMeta> getStorageEngineList();

  StorageEngineMeta getStorageEngine(long id);

  List<FragmentMeta> getFragments();

  // schemaMapping 相关的缓存读写接口
  Map<String, Integer> getSchemaMapping(String schema);

  int getSchemaMappingItem(String schema, String key);

  void removeSchemaMapping(String schema);

  void removeSchemaMappingItem(String schema, String key);

  void addOrUpdateSchemaMapping(String schema, Map<String, Integer> schemaMapping);

  void addOrUpdateSchemaMappingItem(String schema, String key, int value);

  void addOrUpdateUser(UserMeta userMeta);

  void removeUser(String username);

  List<UserMeta> getUser();

  List<UserMeta> getUser(List<String> usernames);

  void timeSeriesIsUpdated(int node, int version);

  void saveColumnsData(InsertStatement statement);

  List<ColumnCalDO> getMaxValueFromColumns();

  double getSumFromColumns();

  Map<Integer, Integer> getColumnsVersionMap();

  void addOrUpdateTransformTask(TransformTaskMeta transformTask);

  void dropTransformTask(String name);

  TransformTaskMeta getTransformTask(String name);

  List<TransformTaskMeta> getTransformTasks();

  List<TransformTaskMeta> getTransformTasksByModule(String moduleName);
}
