package cn.edu.tsinghua.iginx.metadata;

import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.metadata.exception.MetaStorageException;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.StorageUnitHook;
import cn.edu.tsinghua.iginx.policy.simple.ColumnCalDO;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IMetaManager {

  /** 批量新增存储引擎节点 */
  boolean addStorageEngines(List<StorageEngineMeta> storageEngineMetas);

  /** 删除存储引擎节点（仅限于dummy） */
  boolean removeDummyStorageEngine(long storageEngineId);

  /** 获取所有的存储引擎实例的原信息（包括每个存储引擎的存储单元列表） */
  List<StorageEngineMeta> getStorageEngineList();

  List<StorageEngineMeta> getWritableStorageEngineList();

  /** 获取存储引擎实例的数量 */
  int getStorageEngineNum();

  /** 获取某个存储引擎实例的原信息（包括存储引擎的存储单元列表） */
  StorageEngineMeta getStorageEngine(long id);

  StorageUnitMeta getStorageUnit(String id);

  Map<String, StorageUnitMeta> getStorageUnits(Set<String> ids);

  List<StorageUnitMeta> getStorageUnits();

  /** 获取所有活跃的 iginx 节点的元信息 */
  List<IginxMeta> getIginxList();

  /** 获取当前 iginx 节点的 ID */
  long getIginxId();

  /** 获取所有的分片，用于 debug */
  List<FragmentMeta> getFragments();

  List<FragmentMeta> getFragmentsByStorageUnit(String storageUnitId);

  /** 获取某个du的序列和key范围 */
  Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorageUnit(String storageUnitId);

  /** 获取某个列区间的所有分片，不会返回虚拟堆叠分片 */
  Map<ColumnsInterval, List<FragmentMeta>> getFragmentMapByColumnsInterval(
      ColumnsInterval columnsInterval);

  /** 获取某个列区间的所有分片，根据参数决定是否返回虚拟堆叠分片 */
  Map<ColumnsInterval, List<FragmentMeta>> getFragmentMapByColumnsInterval(
      ColumnsInterval columnsInterval, boolean withDummyFragment);

  /** 查询某个列区间是否有虚拟堆叠分片 */
  boolean hasDummyFragment(ColumnsInterval columnsInterval);

  /** 集群中是否存在可写的存储引擎 */
  boolean hasWritableStorageEngines();

  /** 获取某个key区间的所有最新的分片（这些分片一定也都是未终结的分片） */
  Map<ColumnsInterval, FragmentMeta> getLatestFragmentMapByColumnsInterval(
      ColumnsInterval columnsInterval);

  /** 获取全部最新的分片，不会返回虚拟堆叠分片 */
  Map<ColumnsInterval, FragmentMeta> getLatestFragmentMap();

  /** 获取某个列区间在某个时间区间的所有分片，不会返回虚拟堆叠分片。 */
  Map<ColumnsInterval, List<FragmentMeta>> getFragmentMapByColumnsIntervalAndKeyInterval(
      ColumnsInterval columnsInterval, KeyInterval keyInterval);

  /** 获取某个列区间在某个key区间的所有分片，根据参数决定是否返回虚拟堆叠分片 */
  Map<ColumnsInterval, List<FragmentMeta>> getFragmentMapByColumnsIntervalAndKeyInterval(
      ColumnsInterval columnsInterval, KeyInterval keyInterval, boolean withDummyFragment);

  /** 获取某个列的所有分片（按照key排序），会返回虚拟堆叠分片 */
  List<FragmentMeta> getFragmentListByColumnName(String colName);

  /** 获取某个列的最新分片 */
  FragmentMeta getLatestFragmentByColumnName(String colName);

  /** 获取某个列在某个key区间的所有分片（按照分片时间戳排序） */
  List<FragmentMeta> getFragmentListByColumnNameAndKeyInterval(
      String colName, KeyInterval keyInterval);

  /** 创建分片和存储单元 */
  boolean createFragmentsAndStorageUnits(
      List<StorageUnitMeta> storageUnits, List<FragmentMeta> fragments);

  /**
   * 用于负载均衡，切割分片和du
   *
   * @return
   */
  FragmentMeta splitFragmentAndStorageUnit(
      StorageUnitMeta toAddStorageUnit, FragmentMeta toAddFragment, FragmentMeta fragment);

  /** 是否已经创建过分片 */
  boolean hasFragment();

  /** 创建初始分片和初始存储单元 */
  boolean createInitialFragmentsAndStorageUnits(
      List<StorageUnitMeta> storageUnits, List<FragmentMeta> initialFragments);

  /** 用于重分片，在目标节点创建相应的du */
  StorageUnitMeta generateNewStorageUnitMetaByFragment(
      FragmentMeta fragmentMeta, long targetStorageId) throws MetaStorageException;

  /**
   * 为新创建的分片选择存储引擎实例
   *
   * @return 选出的存储引擎实例 Id 列表
   */
  List<Long> selectStorageEngineIdList();

  void registerStorageEngineChangeHook(StorageEngineChangeHook hook);

  /**
   * 增加或更新 schemaMappings
   *
   * @param schema 待更新的 schema 名
   * @param schemaMapping 待更新的 schema，如果 schema 为空，则表示删除给定的 schema
   */
  void addOrUpdateSchemaMapping(String schema, Map<String, Integer> schemaMapping);

  /**
   * 增加或更新某个给定 schemaMapping 的数据项
   *
   * @param schema 待更新的 schema 名
   * @param key 待更新的数据项的名
   * @param value 待更新的数据项，如果 value = -1 表示删除该数据项
   */
  void addOrUpdateSchemaMappingItem(String schema, String key, int value);

  /**
   * 获取某个 schemaMapping
   *
   * @param schema 需要获取的 schema
   * @return schema。如果不存在则返回空指针
   */
  Map<String, Integer> getSchemaMapping(String schema);

  /**
   * 获取某个 schemaMapping 中的数据项
   *
   * @param schema 需要获取的 schema
   * @param key 需要获取的数据项的名
   * @return 数据项的值。如果不存在则返回 -1
   */
  int getSchemaMappingItem(String schema, String key);

  boolean addUser(UserMeta user);

  boolean updateUser(String username, String password, Set<AuthType> auths);

  boolean removeUser(String username);

  UserMeta getUser(String username);

  List<UserMeta> getUsers();

  List<UserMeta> getUsers(List<String> username);

  void registerStorageUnitHook(StorageUnitHook hook);

  boolean election();

  void saveColumnsData(InsertStatement statement);

  List<ColumnCalDO> getMaxValueFromColumns();

  Map<String, Double> getColumnsData();

  int updateVersion();

  Map<Integer, Integer> getColumnsVersionMap();

  boolean addTransformTask(TransformTaskMeta transformTask);

  boolean updateTransformTask(TransformTaskMeta transformTask);

  boolean dropTransformTask(String name);

  TransformTaskMeta getTransformTask(String name);

  List<TransformTaskMeta> getTransformTasks();

  List<TransformTaskMeta> getTransformTasksByModule(String moduleName);

  void updateFragmentRequests(
      Map<FragmentMeta, Long> writeRequestsMap, Map<FragmentMeta, Long> readRequestsMap)
      throws Exception;

  void updateFragmentHeat(
      Map<FragmentMeta, Long> writeHotspotMap, Map<FragmentMeta, Long> readHotspotMap)
      throws Exception;

  Pair<Map<FragmentMeta, Long>, Map<FragmentMeta, Long>> loadFragmentHeat() throws Exception;

  void updateFragmentPoints(FragmentMeta fragmentMeta, long points);

  Map<FragmentMeta, Long> loadFragmentPoints() throws Exception;

  void clearMonitors();

  void removeFragment(FragmentMeta fragmentMeta);

  void addFragment(FragmentMeta fragmentMeta);

  void endFragmentByColumnsInterval(FragmentMeta fragmentMeta, String endColumn);

  void updateFragmentByColumnsInterval(ColumnsInterval columnsInterval, FragmentMeta fragmentMeta);

  void updateMaxActiveEndKey(long endKey);

  long getMaxActiveEndKey();

  void submitMaxActiveEndKey();

  /** resolve storage engine list from config file */
  List<StorageEngineMeta> getStorageEngineListFromConf();
}
