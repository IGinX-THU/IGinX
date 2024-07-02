/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.metadata;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
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

public class MetaManagerWrapper implements IMetaManager {

  private static volatile MetaManagerWrapper INSTANCE;

  private static IMetaManager metaManager;

  /** 构造函数，在UT环境下，使用Mock的MetaManager；在正常环境下，使用DefaultMetaManager。 */
  private MetaManagerWrapper() {
    if (ConfigDescriptor.getInstance().getConfig().isUTTestEnv()) {
      metaManager = MetaManagerMock.getInstance();
    } else {
      metaManager = DefaultMetaManager.getInstance();
    }
  }

  public static MetaManagerWrapper getInstance() {
    if (INSTANCE == null) {
      synchronized (MetaManagerWrapper.class) {
        if (INSTANCE == null) {
          INSTANCE = new MetaManagerWrapper();
        }
      }
    }
    return INSTANCE;
  }

  @Override
  public boolean addStorageEngines(List<StorageEngineMeta> storageEngineMetas) {
    return metaManager.addStorageEngines(storageEngineMetas);
  }

  @Override
  public boolean removeDummyStorageEngine(long storageEngineId) {
    return metaManager.removeDummyStorageEngine(storageEngineId);
  }

  @Override
  public List<StorageEngineMeta> getStorageEngineList() {
    return metaManager.getStorageEngineList();
  }

  @Override
  public List<StorageEngineMeta> getWritableStorageEngineList() {
    return metaManager.getWritableStorageEngineList();
  }

  @Override
  public int getStorageEngineNum() {
    return metaManager.getStorageEngineNum();
  }

  @Override
  public StorageEngineMeta getStorageEngine(long id) {
    return metaManager.getStorageEngine(id);
  }

  @Override
  public StorageUnitMeta getStorageUnit(String id) {
    return metaManager.getStorageUnit(id);
  }

  @Override
  public Map<String, StorageUnitMeta> getStorageUnits(Set<String> ids) {
    return metaManager.getStorageUnits(ids);
  }

  @Override
  public List<StorageUnitMeta> getStorageUnits() {
    return metaManager.getStorageUnits();
  }

  @Override
  public List<IginxMeta> getIginxList() {
    return metaManager.getIginxList();
  }

  @Override
  public long getIginxId() {
    return metaManager.getIginxId();
  }

  @Override
  public List<FragmentMeta> getFragments() {
    return metaManager.getFragments();
  }

  @Override
  public List<FragmentMeta> getFragmentsByStorageUnit(String storageUnitId) {
    return metaManager.getFragmentsByStorageUnit(storageUnitId);
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorageUnit(String storageUnitId) {
    return metaManager.getBoundaryOfStorageUnit(storageUnitId);
  }

  @Override
  public Map<ColumnsInterval, List<FragmentMeta>> getFragmentMapByColumnsInterval(
      ColumnsInterval columnsInterval) {
    return metaManager.getFragmentMapByColumnsInterval(columnsInterval);
  }

  @Override
  public Map<ColumnsInterval, List<FragmentMeta>> getFragmentMapByColumnsInterval(
      ColumnsInterval columnsInterval, boolean withDummyFragment) {
    return metaManager.getFragmentMapByColumnsInterval(columnsInterval, withDummyFragment);
  }

  @Override
  public boolean hasDummyFragment(ColumnsInterval columnsInterval) {
    return metaManager.hasDummyFragment(columnsInterval);
  }

  @Override
  public boolean hasWritableStorageEngines() {
    return metaManager.hasWritableStorageEngines();
  }

  @Override
  public Map<ColumnsInterval, FragmentMeta> getLatestFragmentMapByColumnsInterval(
      ColumnsInterval columnsInterval) {
    return metaManager.getLatestFragmentMapByColumnsInterval(columnsInterval);
  }

  @Override
  public Map<ColumnsInterval, FragmentMeta> getLatestFragmentMap() {
    return metaManager.getLatestFragmentMap();
  }

  @Override
  public Map<ColumnsInterval, List<FragmentMeta>> getFragmentMapByColumnsIntervalAndKeyInterval(
      ColumnsInterval columnsInterval, KeyInterval keyInterval) {
    return metaManager.getFragmentMapByColumnsIntervalAndKeyInterval(columnsInterval, keyInterval);
  }

  @Override
  public Map<ColumnsInterval, List<FragmentMeta>> getFragmentMapByColumnsIntervalAndKeyInterval(
      ColumnsInterval columnsInterval, KeyInterval keyInterval, boolean withDummyFragment) {
    return metaManager.getFragmentMapByColumnsIntervalAndKeyInterval(
        columnsInterval, keyInterval, withDummyFragment);
  }

  @Override
  public List<FragmentMeta> getFragmentListByColumnName(String colName) {
    return metaManager.getFragmentListByColumnName(colName);
  }

  @Override
  public FragmentMeta getLatestFragmentByColumnName(String colName) {
    return metaManager.getLatestFragmentByColumnName(colName);
  }

  @Override
  public List<FragmentMeta> getFragmentListByColumnNameAndKeyInterval(
      String colName, KeyInterval keyInterval) {
    return metaManager.getFragmentListByColumnNameAndKeyInterval(colName, keyInterval);
  }

  @Override
  public boolean createFragmentsAndStorageUnits(
      List<StorageUnitMeta> storageUnits, List<FragmentMeta> fragments) {
    return metaManager.createFragmentsAndStorageUnits(storageUnits, fragments);
  }

  @Override
  public FragmentMeta splitFragmentAndStorageUnit(
      StorageUnitMeta toAddStorageUnit, FragmentMeta toAddFragment, FragmentMeta fragment) {
    return metaManager.splitFragmentAndStorageUnit(toAddStorageUnit, toAddFragment, fragment);
  }

  @Override
  public boolean hasFragment() {
    return metaManager.hasFragment();
  }

  @Override
  public boolean createInitialFragmentsAndStorageUnits(
      List<StorageUnitMeta> storageUnits, List<FragmentMeta> initialFragments) {
    return metaManager.createInitialFragmentsAndStorageUnits(storageUnits, initialFragments);
  }

  @Override
  public StorageUnitMeta generateNewStorageUnitMetaByFragment(
      FragmentMeta fragmentMeta, long targetStorageId) throws MetaStorageException {
    return metaManager.generateNewStorageUnitMetaByFragment(fragmentMeta, targetStorageId);
  }

  @Override
  public List<Long> selectStorageEngineIdList() {
    return metaManager.selectStorageEngineIdList();
  }

  @Override
  public void registerStorageEngineChangeHook(StorageEngineChangeHook hook) {
    metaManager.registerStorageEngineChangeHook(hook);
  }

  @Override
  public void addOrUpdateSchemaMapping(String schema, Map<String, Integer> schemaMapping) {
    metaManager.addOrUpdateSchemaMapping(schema, schemaMapping);
  }

  @Override
  public void addOrUpdateSchemaMappingItem(String schema, String key, int value) {
    metaManager.addOrUpdateSchemaMappingItem(schema, key, value);
  }

  @Override
  public Map<String, Integer> getSchemaMapping(String schema) {
    return metaManager.getSchemaMapping(schema);
  }

  @Override
  public int getSchemaMappingItem(String schema, String key) {
    return metaManager.getSchemaMappingItem(schema, key);
  }

  @Override
  public boolean addUser(UserMeta user) {
    return metaManager.addUser(user);
  }

  @Override
  public boolean updateUser(String username, String password, Set<AuthType> auths) {
    return metaManager.updateUser(username, password, auths);
  }

  @Override
  public boolean removeUser(String username) {
    return metaManager.removeUser(username);
  }

  @Override
  public UserMeta getUser(String username) {
    return metaManager.getUser(username);
  }

  @Override
  public List<UserMeta> getUsers() {
    return metaManager.getUsers();
  }

  @Override
  public List<UserMeta> getUsers(List<String> username) {
    return metaManager.getUsers(username);
  }

  @Override
  public void registerStorageUnitHook(StorageUnitHook hook) {
    metaManager.registerStorageUnitHook(hook);
  }

  @Override
  public boolean election() {
    return metaManager.election();
  }

  @Override
  public void saveColumnsData(InsertStatement statement) {
    metaManager.saveColumnsData(statement);
  }

  @Override
  public List<ColumnCalDO> getMaxValueFromColumns() {
    return metaManager.getMaxValueFromColumns();
  }

  @Override
  public Map<String, Double> getColumnsData() {
    return metaManager.getColumnsData();
  }

  @Override
  public int updateVersion() {
    return metaManager.updateVersion();
  }

  @Override
  public Map<Integer, Integer> getColumnsVersionMap() {
    return metaManager.getColumnsVersionMap();
  }

  @Override
  public boolean addTransformTask(TransformTaskMeta transformTask) {
    return metaManager.addTransformTask(transformTask);
  }

  @Override
  public boolean updateTransformTask(TransformTaskMeta transformTask) {
    return metaManager.updateTransformTask(transformTask);
  }

  @Override
  public boolean dropTransformTask(String name) {
    return metaManager.dropTransformTask(name);
  }

  @Override
  public TransformTaskMeta getTransformTask(String name) {
    return metaManager.getTransformTask(name);
  }

  @Override
  public List<TransformTaskMeta> getTransformTasks() {
    return metaManager.getTransformTasks();
  }

  @Override
  public List<TransformTaskMeta> getTransformTasksByModule(String moduleName) {
    return metaManager.getTransformTasksByModule(moduleName);
  }

  @Override
  public void updateFragmentRequests(
      Map<FragmentMeta, Long> writeRequestsMap, Map<FragmentMeta, Long> readRequestsMap)
      throws Exception {
    metaManager.updateFragmentRequests(writeRequestsMap, readRequestsMap);
  }

  @Override
  public void updateFragmentHeat(
      Map<FragmentMeta, Long> writeHotspotMap, Map<FragmentMeta, Long> readHotspotMap)
      throws Exception {
    metaManager.updateFragmentHeat(writeHotspotMap, readHotspotMap);
  }

  @Override
  public Pair<Map<FragmentMeta, Long>, Map<FragmentMeta, Long>> loadFragmentHeat()
      throws Exception {
    return metaManager.loadFragmentHeat();
  }

  @Override
  public void updateFragmentPoints(FragmentMeta fragmentMeta, long points) {
    metaManager.updateFragmentPoints(fragmentMeta, points);
  }

  @Override
  public Map<FragmentMeta, Long> loadFragmentPoints() throws Exception {
    return metaManager.loadFragmentPoints();
  }

  @Override
  public void clearMonitors() {
    metaManager.clearMonitors();
  }

  @Override
  public void removeFragment(FragmentMeta fragmentMeta) {
    metaManager.removeFragment(fragmentMeta);
  }

  @Override
  public void addFragment(FragmentMeta fragmentMeta) {
    metaManager.addFragment(fragmentMeta);
  }

  @Override
  public void endFragmentByColumnsInterval(FragmentMeta fragmentMeta, String endColumn) {
    metaManager.endFragmentByColumnsInterval(fragmentMeta, endColumn);
  }

  @Override
  public void updateFragmentByColumnsInterval(
      ColumnsInterval columnsInterval, FragmentMeta fragmentMeta) {
    metaManager.updateFragmentByColumnsInterval(columnsInterval, fragmentMeta);
  }

  @Override
  public void updateMaxActiveEndKey(long endKey) {
    metaManager.updateMaxActiveEndKey(endKey);
  }

  @Override
  public long getMaxActiveEndKey() {
    return metaManager.getMaxActiveEndKey();
  }

  @Override
  public void submitMaxActiveEndKey() {
    metaManager.submitMaxActiveEndKey();
  }

  @Override
  public List<StorageEngineMeta> getStorageEngineListFromConf() {
    return metaManager.getStorageEngineListFromConf();
  }
}
