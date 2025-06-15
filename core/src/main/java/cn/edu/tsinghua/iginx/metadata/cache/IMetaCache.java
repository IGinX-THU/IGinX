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
package cn.edu.tsinghua.iginx.metadata.cache;

import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.policy.simple.ColumnCalDO;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.transform.pojo.TriggerDescriptor;
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

  IginxMeta getIginx(long id);

  void addIginx(IginxMeta iginxMeta);

  void removeIginx(long id);

  // 数据后端相关的缓存读写接口
  void addStorageEngine(StorageEngineMeta storageEngineMeta);

  boolean removeDummyStorageEngine(
      long iginxId, long storageEngineId, boolean forAllIginx, boolean checkExist);

  List<StorageEngineMeta> getStorageEngineList();

  StorageEngineMeta getStorageEngine(long id);

  List<FragmentMeta> getFragments();

  // 连接相关的缓存读写接口

  Map<Long, Set<Long>> getIginxConnectivity();

  void refreshIginxConnectivity(Map<Long, Set<Long>> connections);

  Map<Long, Set<Long>> getStorageConnections();

  void updateStorageConnections(Map<Long, Set<Long>> connections);

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

  void addOrUpdateJobTrigger(TriggerDescriptor descriptor);

  void dropJobTrigger(String name);

  List<TriggerDescriptor> getJobTriggers();
}
