/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.metadata.cache;

import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.policy.simple.TimeSeriesCalDO;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IMetaCache {

    boolean enableFragmentCacheControl();

    // 分片相关的缓存读写接口
    void initFragment(Map<ColumnsRange, List<FragmentMeta>> fragmentListMap);

    void addFragment(FragmentMeta fragmentMeta);

    void updateFragment(FragmentMeta fragmentMeta);

    void updateFragmentByTsInterval(ColumnsRange tsInterval, FragmentMeta fragmentMeta);

    void deleteFragmentByTsInterval(ColumnsRange tsInterval, FragmentMeta fragmentMeta);

    List<FragmentMeta> getFragmentMapByExactTimeSeriesInterval(ColumnsRange tsInterval);

    Map<ColumnsRange, List<FragmentMeta>> getFragmentMapByTimeSeriesInterval(
            ColumnsRange tsInterval);

    List<FragmentMeta> getDummyFragmentsByTimeSeriesInterval(ColumnsRange tsInterval);

    Map<ColumnsRange, FragmentMeta> getLatestFragmentMap();

    Map<ColumnsRange, FragmentMeta> getLatestFragmentMapByTimeSeriesInterval(
            ColumnsRange tsInterval);

    Map<ColumnsRange, List<FragmentMeta>> getFragmentMapByTimeSeriesIntervalAndTimeInterval(
            ColumnsRange tsInterval, KeyInterval keyInterval);

    List<FragmentMeta> getDummyFragmentsByTimeSeriesIntervalAndTimeInterval(
            ColumnsRange tsInterval, KeyInterval keyInterval);

    List<FragmentMeta> getFragmentListByTimeSeriesName(String tsName);

    FragmentMeta getLatestFragmentByTimeSeriesName(String tsName);

    List<FragmentMeta> getFragmentListByTimeSeriesNameAndTimeInterval(
            String tsName, KeyInterval keyInterval);

    List<FragmentMeta> getFragmentListByStorageUnitId(String storageUnitId);

    boolean hasFragment();

    long getFragmentMinTimestamp();

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

    // 更新对应节点的元数据信息。如果对应节点的 dummy 元数据被移除，则需要删除相应的 dummy 元数据信息
    boolean updateStorageEngine(long storageID, StorageEngineMeta storageEngineMeta);

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

    void saveTimeSeriesData(InsertStatement statement);

    List<TimeSeriesCalDO> getMaxValueFromTimeSeries();

    double getSumFromTimeSeries();

    Map<Integer, Integer> getTimeseriesVersionMap();

    void addOrUpdateTransformTask(TransformTaskMeta transformTask);

    void dropTransformTask(String name);

    TransformTaskMeta getTransformTask(String name);

    List<TransformTaskMeta> getTransformTasks();
}
