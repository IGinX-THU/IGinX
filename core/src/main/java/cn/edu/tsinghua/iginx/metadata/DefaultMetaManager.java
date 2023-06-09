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
package cn.edu.tsinghua.iginx.metadata;

import static cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus.EXECUTING;
import static cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus.NON_RESHARDING;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.engine.physical.storage.StorageManager;
import cn.edu.tsinghua.iginx.exceptions.MetaStorageException;
import cn.edu.tsinghua.iginx.metadata.cache.DefaultMetaCache;
import cn.edu.tsinghua.iginx.metadata.cache.IMetaCache;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.StorageUnitHook;
import cn.edu.tsinghua.iginx.metadata.storage.IMetaStorage;
import cn.edu.tsinghua.iginx.metadata.storage.etcd.ETCDMetaStorage;
import cn.edu.tsinghua.iginx.metadata.storage.zk.ZooKeeperMetaStorage;
import cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus;
import cn.edu.tsinghua.iginx.migration.storage.StorageMigrationPlan;
import cn.edu.tsinghua.iginx.monitor.HotSpotMonitor;
import cn.edu.tsinghua.iginx.monitor.RequestsMonitor;
import cn.edu.tsinghua.iginx.policy.simple.ColumnCalDO;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.thrift.UserType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMetaManager implements IMetaManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultMetaManager.class);
    private static volatile DefaultMetaManager INSTANCE;
    private final IMetaCache cache;

    private final IMetaStorage storage;
    private final List<StorageEngineChangeHook> storageEngineChangeHooks;
    private final List<StorageUnitHook> storageUnitHooks;
    private long id;

    // 当前活跃的最大的结束时间
    private AtomicLong maxActiveEndTime = new AtomicLong(-1L);
    private AtomicInteger maxActiveEndTimeStatisticsCounter = new AtomicInteger(0);

    // 重分片状态
    private ReshardStatus reshardStatus = NON_RESHARDING;

    // 在重分片过程中，是否为提出者
    private boolean isProposer = false;

    private DefaultMetaManager() {
        cache = DefaultMetaCache.getInstance();

        switch (ConfigDescriptor.getInstance().getConfig().getMetaStorage()) {
            case Constants.ZOOKEEPER_META:
                logger.info("use zookeeper as meta storage.");
                storage = ZooKeeperMetaStorage.getInstance();
                break;
            case Constants.FILE_META:
                logger.error("file as meta storage has depreciated.");
                storage = null;
                System.exit(-1);
                break;
            case Constants.ETCD_META:
                logger.info("use etcd as meta storage");
                storage = ETCDMetaStorage.getInstance();
                break;
            default:
                // without configuration, file storage should be the safe choice
                logger.info(
                        "unknown meta storage "
                                + ConfigDescriptor.getInstance().getConfig().getMetaStorage());
                storage = null;
                System.exit(-1);
        }

        storageEngineChangeHooks = Collections.synchronizedList(new ArrayList<>());
        storageUnitHooks = Collections.synchronizedList(new ArrayList<>());

        try {
            initIginx();
            initStorageEngine();
            initStorageUnit();
            initFragment();
            initSchemaMapping();
            initPolicy();
            initUser();
            initTransform();
            initMaxActiveEndTimeStatistics();
            initReshardStatus();
            initReshardCounter();
        } catch (MetaStorageException e) {
            logger.error("init meta manager error: ", e);
            System.exit(-1);
        }
    }

    public static DefaultMetaManager getInstance() {
        if (INSTANCE == null) {
            synchronized (DefaultMetaManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new DefaultMetaManager();
                }
            }
        }
        return INSTANCE;
    }

    private void initMaxActiveEndTimeStatistics() throws MetaStorageException {
        storage.registerMaxActiveEndTimeStatisticsChangeHook(
                (endTime) -> {
                    if (endTime <= 0L) {
                        return;
                    }
                    updateMaxActiveEndKey(endTime);
                    int updatedCounter = maxActiveEndTimeStatisticsCounter.incrementAndGet();
                    logger.info(
                            "iginx node {} increment max active end time statistics counter {}",
                            this.id,
                            updatedCounter);
                });
    }

    private void initReshardStatus() throws MetaStorageException {
        storage.registerReshardStatusHook(
                status -> {
                    try {
                        reshardStatus = status;
                        if (reshardStatus.equals(EXECUTING)) {
                            storage.lockMaxActiveEndTimeStatistics();
                            storage.addOrUpdateMaxActiveEndTimeStatistics(maxActiveEndTime.get());
                            storage.releaseMaxActiveEndTimeStatistics();

                            storage.lockReshardCounter();
                            storage.incrementReshardCounter();
                            storage.releaseReshardCounter();
                        }
                        if (reshardStatus.equals(NON_RESHARDING)) {
                            if (isProposer) {
                                logger.info("iginx node {}(proposer) finish to reshard", id);
                            } else {
                                logger.info("iginx node {} finish to reshard", id);
                            }

                            isProposer = false;
                            maxActiveEndTimeStatisticsCounter.set(0);
                        }
                    } catch (MetaStorageException e) {
                        logger.error("encounter error when switching reshard status: ", e);
                    }
                });
        storage.lockReshardStatus();
        storage.removeReshardStatus();
        storage.releaseReshardStatus();
    }

    private void initReshardCounter() throws MetaStorageException {
        storage.registerReshardCounterChangeHook(
                counter -> {
                    try {
                        if (counter <= 0) {
                            return;
                        }
                        if (isProposer && counter == getIginxList().size() - 1) {
                            storage.lockReshardCounter();
                            storage.resetReshardCounter();
                            storage.releaseReshardCounter();

                            if (reshardStatus == EXECUTING) {
                                storage.lockReshardStatus();
                                storage.updateReshardStatus(NON_RESHARDING);
                                storage.releaseReshardStatus();
                            }
                        }
                    } catch (MetaStorageException e) {
                        logger.error("encounter error when updating reshard counter: ", e);
                    }
                });
        storage.lockReshardCounter();
        storage.removeReshardCounter();
        storage.releaseReshardCounter();
    }

    private void initIginx() throws MetaStorageException {
        storage.registerIginxChangeHook(
                (id, iginx) -> {
                    if (iginx == null) {
                        cache.removeIginx(id);
                    } else {
                        cache.addIginx(iginx);
                    }
                });
        for (IginxMeta iginx : storage.loadIginx().values()) {
            cache.addIginx(iginx);
        }
        IginxMeta iginx =
                new IginxMeta(
                        0L,
                        ConfigDescriptor.getInstance().getConfig().getIp(),
                        ConfigDescriptor.getInstance().getConfig().getPort(),
                        null);
        id = storage.registerIginx(iginx);
        SnowFlakeUtils.init(id);
    }

    private void initStorageEngine() throws MetaStorageException {
        storage.registerStorageChangeHook(
                (id, storageEngine) -> {
                    if (storageEngine != null) {
                        if (storageEngine.isHasData()) {
                            StorageUnitMeta dummyStorageUnit = storageEngine.getDummyStorageUnit();
                            dummyStorageUnit.setStorageEngineId(id);
                            dummyStorageUnit.setId(StorageUnitMeta.generateDummyStorageUnitID(id));
                            dummyStorageUnit.setMasterId(dummyStorageUnit.getId());
                            FragmentMeta dummyFragment = storageEngine.getDummyFragment();
                            dummyFragment.setMasterStorageUnit(dummyStorageUnit);
                            dummyFragment.setMasterStorageUnitId(dummyStorageUnit.getId());
                        }
                        cache.addStorageEngine(storageEngine);
                        for (StorageEngineChangeHook hook : storageEngineChangeHooks) {
                            hook.onChanged(null, storageEngine);
                        }
                        if (storageEngine.isHasData()) {
                            for (StorageUnitHook storageUnitHook : storageUnitHooks) {
                                storageUnitHook.onChange(null, storageEngine.getDummyStorageUnit());
                            }
                        }
                    }
                });
        storage.loadStorageEngine(resolveStorageEngineFromConf());
    }

    private void initStorageUnit() throws MetaStorageException {
        storage.registerStorageUnitChangeHook(
                (id, storageUnit) -> {
                    if (storageUnit == null) {
                        return;
                    }
                    if (storageUnit.getCreatedBy() == DefaultMetaManager.this.id) { // 本地创建的
                        return;
                    }
                    if (storageUnit.isInitialStorageUnit()) { // 初始分片不通过异步事件更新
                        return;
                    }
                    if (!cache.hasStorageUnit()) {
                        return;
                    }
                    StorageUnitMeta originStorageUnitMeta = cache.getStorageUnit(id);
                    if (originStorageUnitMeta == null) {
                        if (!storageUnit.isMaster()) { // 需要加入到主节点的子节点列表中
                            StorageUnitMeta masterStorageUnitMeta =
                                    cache.getStorageUnit(storageUnit.getMasterId());
                            if (masterStorageUnitMeta == null) { // 子节点先于主节点加入系统中，不应该发生，报错
                                logger.error(
                                        "unexpected storage unit "
                                                + storageUnit.toString()
                                                + ", because it does not has a master storage unit");
                            } else {
                                masterStorageUnitMeta.addReplica(storageUnit);
                            }
                        }
                    } else {
                        if (storageUnit.isMaster()) {
                            storageUnit.setReplicas(originStorageUnitMeta.getReplicas());
                        } else {
                            StorageUnitMeta masterStorageUnitMeta =
                                    cache.getStorageUnit(storageUnit.getMasterId());
                            if (masterStorageUnitMeta == null) { // 子节点先于主节点加入系统中，不应该发生，报错
                                logger.error(
                                        "unexpected storage unit "
                                                + storageUnit.toString()
                                                + ", because it does not has a master storage unit");
                            } else {
                                masterStorageUnitMeta.removeReplica(originStorageUnitMeta);
                                masterStorageUnitMeta.addReplica(storageUnit);
                            }
                        }
                    }
                    if (originStorageUnitMeta != null) {
                        cache.updateStorageUnit(storageUnit);
                        cache.getStorageEngine(storageUnit.getStorageEngineId())
                                .removeStorageUnit(originStorageUnitMeta.getId());
                    } else {
                        cache.addStorageUnit(storageUnit);
                    }
                    cache.getStorageEngine(storageUnit.getStorageEngineId())
                            .addStorageUnit(storageUnit);
                    for (StorageUnitHook storageUnitHook : storageUnitHooks) {
                        storageUnitHook.onChange(originStorageUnitMeta, storageUnit);
                    }
                });
    }

    private void initFragment() throws MetaStorageException {
        storage.registerFragmentChangeHook(
                (create, fragment) -> {
                    if (fragment == null) return;
                    if (create && fragment.getCreatedBy() == DefaultMetaManager.this.id) {
                        return;
                    }
                    if (!create && fragment.getUpdatedBy() == DefaultMetaManager.this.id) {
                        return;
                    }
                    if (fragment.isInitialFragment()) { // 初始分片不通过异步事件更新
                        return;
                    }
                    if (!cache.hasFragment()) {
                        return;
                    }
                    fragment.setMasterStorageUnit(
                            cache.getStorageUnit(fragment.getMasterStorageUnitId()));
                    if (create) {
                        cache.addFragment(fragment);
                    } else {
                        cache.updateFragment(fragment);
                    }
                });
    }

    private void initSchemaMapping() throws MetaStorageException {
        storage.registerSchemaMappingChangeHook(
                (schema, schemaMapping) -> {
                    if (schemaMapping == null || schemaMapping.size() == 0) {
                        cache.removeSchemaMapping(schema);
                    } else {
                        cache.addOrUpdateSchemaMapping(schema, schemaMapping);
                    }
                });
        for (Map.Entry<String, Map<String, Integer>> schemaEntry :
                storage.loadSchemaMapping().entrySet()) {
            cache.addOrUpdateSchemaMapping(schemaEntry.getKey(), schemaEntry.getValue());
        }
    }

    private void initPolicy() {
        storage.registerTimeseriesChangeHook(cache::timeSeriesIsUpdated);
        storage.registerVersionChangeHook(
                (version, num) -> {
                    double sum = cache.getSumFromTimeSeries();
                    Map<String, Double> timeseriesData =
                            cache.getMaxValueFromTimeSeries()
                                    .stream()
                                    .collect(
                                            Collectors.toMap(
                                                    ColumnCalDO::getColumn, ColumnCalDO::getValue));
                    double countSum =
                            timeseriesData.values().stream().mapToDouble(Double::doubleValue).sum();
                    if (countSum > 1e-9) {
                        timeseriesData.forEach((k, v) -> timeseriesData.put(k, v / countSum * sum));
                    }
                    try {
                        storage.updateTimeseriesData(timeseriesData, getIginxId(), version);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
        int num = 0;
        try {
            storage.registerPolicy(getIginxId(), num);
            // 从元数据管理器取写入的最大时间戳
            maxActiveEndTime.set(storage.getMaxActiveEndTimeStatistics());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initUser() throws MetaStorageException {
        storage.registerUserChangeHook(
                (username, user) -> {
                    if (user == null) {
                        cache.removeUser(username);
                    } else {
                        cache.addOrUpdateUser(user);
                    }
                });
        for (UserMeta user : storage.loadUser(resolveUserFromConf())) {
            cache.addOrUpdateUser(user);
        }
    }

    private void initTransform() throws MetaStorageException {
        storage.registerTransformChangeHook(
                ((name, transformTask) -> {
                    if (transformTask == null) {
                        cache.dropTransformTask(name);
                    } else {
                        cache.addOrUpdateTransformTask(transformTask);
                    }
                }));
        for (TransformTaskMeta task : storage.loadTransformTask()) {
            cache.addOrUpdateTransformTask(task);
        }
    }

    @Override
    public boolean addStorageEngines(List<StorageEngineMeta> storageEngineMetas) {
        try {
            for (StorageEngineMeta storageEngineMeta : storageEngineMetas) {
                long id = storage.addStorageEngine(storageEngineMeta);
                storageEngineMeta.setId(id);
                if (storageEngineMeta.isHasData()) {
                    StorageUnitMeta dummyStorageUnit = storageEngineMeta.getDummyStorageUnit();
                    dummyStorageUnit.setStorageEngineId(id);
                    dummyStorageUnit.setId(StorageUnitMeta.generateDummyStorageUnitID(id));
                    dummyStorageUnit.setMasterId(dummyStorageUnit.getId());
                    FragmentMeta dummyFragment = storageEngineMeta.getDummyFragment();
                    dummyFragment.setMasterStorageUnit(dummyStorageUnit);
                    dummyFragment.setMasterStorageUnitId(dummyStorageUnit.getId());
                }
                cache.addStorageEngine(storageEngineMeta);
            }
            return true;
        } catch (MetaStorageException e) {
            logger.error("add storage engines error:", e);
        }
        return false;
    }

    @Override
    public boolean storeMigrationPlan(StorageMigrationPlan plan) {
        return storage.storeMigrationPlan(plan);
    }

    @Override
    public List<StorageMigrationPlan> scanStorageMigrationPlan() {
        return storage.scanStorageMigrationPlan();
    }

    @Override
    public StorageMigrationPlan getStorageMigrationPlan(long storageId) {
        return storage.getStorageMigrationPlan(storageId);
    }

    @Override
    public boolean transferMigrationPlan(long id, long from, long to) {
        return storage.transferMigrationPlan(id, from, to);
    }

    @Override
    public boolean deleteMigrationPlan(long id) {
        return storage.deleteMigrationPlan(id);
    }

    @Override
    public Map<String, String> startMigrationStorageUnits(
            Map<String, Long> migrationMap, boolean migrationData) {
        if (migrationData) {
            return startMigrationStorageUnitsWithData(migrationMap);
        }
        return startMigrationStorageUnitsWithoutData(migrationMap);
    }

    private Map<String, String> startMigrationStorageUnitsWithData(Map<String, Long> migrationMap) {
        try {
            Map<String, String> migrationStorageUnitMap = new HashMap<>();
            storage.lockStorageUnit();
            for (String storageUnitId : migrationMap.keySet()) {
                StorageUnitMeta storageUnit = getStorageUnit(storageUnitId);
                if (storageUnit.getState() == StorageUnitState.DISCARD) { // 已经迁移完了
                    continue;
                }
                if (storageUnit.getMigrationTo() != null) { // 正在迁移中
                    migrationStorageUnitMap.put(storageUnitId, storageUnit.getMigrationTo());
                    continue;
                }
                String newStorageUnitId = storage.addStorageUnit();
                StorageUnitMeta clonedStorageUnit = storageUnit.clone();
                StorageUnitMeta newStorageUnit =
                        clonedStorageUnit.migrationStorageUnitMeta(
                                newStorageUnitId, id, migrationMap.get(storageUnitId));
                // 更新新的 storage unit
                cache.updateStorageUnit(newStorageUnit);
                for (StorageUnitHook hook : storageUnitHooks) {
                    hook.onChange(null, newStorageUnit);
                }
                storage.updateStorageUnit(newStorageUnit);
                // 更新旧的 storage unit
                cache.updateStorageUnit(clonedStorageUnit);
                for (StorageUnitHook hook : storageUnitHooks) {
                    hook.onChange(storageUnit, clonedStorageUnit);
                }
                storage.updateStorageUnit(clonedStorageUnit);
                migrationStorageUnitMap.put(storageUnitId, newStorageUnitId);
            }
            return migrationStorageUnitMap;
        } catch (MetaStorageException e) {
            logger.error("migration storage unit error: ", e);
        } finally {
            try {
                storage.releaseStorageUnit();
            } catch (MetaStorageException e) {
                logger.error("release storage unit lock error: ", e);
            }
        }
        return null;
    }

    private Map<String, String> startMigrationStorageUnitsWithoutData(
            Map<String, Long> migrationMap) { // 这里的 key 指的是实际进行迁移的分片，value 指的是实际的目的地
        try {
            Map<String, String> migrationStorageUnitMap = new HashMap<>();

            List<StorageUnitMeta> beforeStorageUnits = new ArrayList<>();
            List<StorageUnitMeta> afterStorageUnits = new ArrayList<>();

            storage.lockStorageUnit();
            for (String storageUnitId : migrationMap.keySet()) {
                String newStorageUnitId = storage.addStorageUnit();

                StorageUnitMeta storageUnit =
                        getStorageUnit(storageUnitId); // 宕机的存储单元，直接标记成 Discard
                if (storageUnit.getState()
                        == StorageUnitState.DISCARD) { // 正处在迁移或者迁移的中间状态，表示当前的请求为一个重试的请求
                    StorageUnitMeta targetUnit = getStorageUnit(storageUnit.getMigrationTo());
                    if (targetUnit.getState() == StorageUnitState.CREATING) {
                        // 这里需要选出来一个副本，作为数据迁移的源头
                        if (storageUnit.isMaster()) {
                            if (storageUnit.getReplicas() != null
                                    && storageUnit.getReplicas().size() > 0) {
                                migrationStorageUnitMap.put(
                                        storageUnit
                                                .getReplicas()
                                                .get(storageUnit.getReplicas().size() - 1)
                                                .getId(),
                                        newStorageUnitId);
                            }
                        } else { // 失效的分片是副本的话，只需要找到主本，使用主本作为数据源即可
                            migrationStorageUnitMap.put(
                                    storageUnit.getMasterId(), newStorageUnitId);
                        }
                    }
                    continue;
                }

                StorageUnitMeta clonedStorageUnit = storageUnit.clone();
                beforeStorageUnits.add(storageUnit);
                afterStorageUnits.add(clonedStorageUnit);

                StorageUnitMeta newStorageUnit =
                        clonedStorageUnit.migrationStorageUnitMeta(
                                newStorageUnitId, id, migrationMap.get(storageUnitId));
                clonedStorageUnit.setState(StorageUnitState.DISCARD);

                beforeStorageUnits.add(null);
                afterStorageUnits.add(newStorageUnit);

                // 为宕机的副本找到合适的主本
                if (storageUnit.isMaster()) {
                    if (storageUnit.getReplicas() != null && storageUnit.getReplicas().size() > 0) {
                        migrationStorageUnitMap.put(
                                storageUnit.getReplicas().get(0).getId(), newStorageUnitId);
                    }
                } else { // 失效的分片是副本的话，只需要找到主本，使用主本作为数据源即可
                    migrationStorageUnitMap.put(storageUnit.getMasterId(), newStorageUnitId);
                }

                // 变更副本关系
                if (storageUnit.isMaster()) {
                    List<StorageUnitMeta> slaveUnits = storageUnit.getReplicas();
                    for (StorageUnitMeta unit : slaveUnits) {
                        StorageUnitMeta clone = unit.clone();
                        clone.setMasterId(newStorageUnitId);
                        beforeStorageUnits.add(unit);
                        afterStorageUnits.add(clone);
                        newStorageUnit.addReplica(clone);
                    }
                } else {
                    StorageUnitMeta masterUnit = getStorageUnit(storageUnit.getMasterId());
                    masterUnit.addReplica(newStorageUnit);
                    masterUnit.removeReplica(storageUnit);
                }
            }

            for (int i = 0; i < beforeStorageUnits.size(); i++) {
                StorageUnitMeta before = beforeStorageUnits.get(i);
                StorageUnitMeta after = afterStorageUnits.get(i);
                cache.updateStorageUnit(after);
                for (StorageUnitHook hook : storageUnitHooks) {
                    hook.onChange(before, after);
                }
                storage.updateStorageUnit(after);
            }

            return migrationStorageUnitMap;
        } catch (MetaStorageException e) {
            logger.error("migration storage unit error: ", e);
        } finally {
            try {
                storage.releaseStorageUnit();
            } catch (MetaStorageException e) {
                logger.error("release storage unit lock error: ", e);
            }
        }
        return null;
    }

    @Override
    public boolean finishMigrationStorageUnit(String storageUnitId, boolean migrationData) {
        if (migrationData) {
            return finishMigrationStorageUnitWithData(storageUnitId);
        }
        return finishMigrationStorageUnitWithoutData(storageUnitId);
    }

    public boolean finishMigrationStorageUnitWithData(String storageUnitId) {
        logger.info("start migration for {} {}", storageUnitId, true);
        try {
            List<StorageUnitMeta> beforeStorageUnits = new ArrayList<>();
            List<StorageUnitMeta> afterStorageUnits = new ArrayList<>();
            storage.lockStorageUnit();
            StorageUnitMeta sourceStorageUnit = getStorageUnit(storageUnitId);
            StorageUnitMeta clonedSourceStorageUnit = sourceStorageUnit.clone();
            clonedSourceStorageUnit.setState(StorageUnitState.DISCARD);
            beforeStorageUnits.add(sourceStorageUnit);
            afterStorageUnits.add(clonedSourceStorageUnit);

            StorageUnitMeta targetStorageUnit = getStorageUnit(sourceStorageUnit.getMigrationTo());
            StorageUnitMeta clonedTargetStorageUnit = targetStorageUnit.clone();
            clonedTargetStorageUnit.setState(StorageUnitState.NORMAL);
            beforeStorageUnits.add(targetStorageUnit);
            afterStorageUnits.add(clonedTargetStorageUnit);
            if (sourceStorageUnit.isMaster()) {
                List<StorageUnitMeta> slaveUnits = sourceStorageUnit.getReplicas();
                for (StorageUnitMeta unit : slaveUnits) {
                    StorageUnitMeta clone = unit.clone();
                    clone.setMasterId(clonedTargetStorageUnit.getId());
                    beforeStorageUnits.add(unit);
                    afterStorageUnits.add(clone);
                    clonedTargetStorageUnit.addReplica(clone);
                }
            } else {
                StorageUnitMeta masterStorageUnit =
                        getStorageUnit(clonedSourceStorageUnit.getMasterId());
                masterStorageUnit.addReplica(clonedTargetStorageUnit);
                masterStorageUnit.removeReplica(clonedSourceStorageUnit);
            }

            for (int i = 0; i < beforeStorageUnits.size(); i++) {
                StorageUnitMeta before = beforeStorageUnits.get(i);
                StorageUnitMeta after = afterStorageUnits.get(i);
                cache.updateStorageUnit(after);
                for (StorageUnitHook hook : storageUnitHooks) {
                    hook.onChange(before, after);
                }
                storage.updateStorageUnit(after);
            }

            logger.info("finish migration for {} {}", storageUnitId, true);
            return true;
        } catch (MetaStorageException e) {
            logger.error("migration storage unit error: ", e);
        } finally {
            try {
                storage.releaseStorageUnit();
            } catch (MetaStorageException e) {
                logger.error("release storage unit lock error: ", e);
            }
        }
        return false;
    }

    public boolean finishMigrationStorageUnitWithoutData(String storageUnitId) {
        logger.info("call finish migration for {} {}", storageUnitId, false);
        try {
            storage.lockStorageUnit();
            StorageUnitMeta sourceStorageUnit = getStorageUnit(storageUnitId);
            String targetStorageUnitId = sourceStorageUnit.getMigrationTo();

            StorageUnitMeta storageUnit = getStorageUnit(targetStorageUnitId);
            StorageUnitMeta clonedStorageUnit = storageUnit.clone();
            if (storageUnit.getState() == StorageUnitState.NORMAL) {
                return true;
            }
            clonedStorageUnit.setState(StorageUnitState.NORMAL);
            cache.updateStorageUnit(clonedStorageUnit);
            for (StorageUnitHook hook : storageUnitHooks) {
                hook.onChange(storageUnit, clonedStorageUnit);
            }
            storage.updateStorageUnit(clonedStorageUnit);
            logger.info("finish migration for {} {}", storageUnitId, false);
            return true;
        } catch (MetaStorageException e) {
            logger.error("migration storage unit error: ", e);
        } finally {
            try {
                storage.releaseStorageUnit();
            } catch (MetaStorageException e) {
                logger.error("release storage unit lock error: ", e);
            }
        }
        return false;
    }

    @Override
    public boolean updateStorageUnit(StorageUnitMeta storageUnit) {
        return false;
    }

    @Override
    public boolean updateStorageEngine(long storageID, StorageEngineMeta storageEngineMeta) {
        if (getStorageEngine(storageID) == null) {
            return false;
        }
        try {
            storageEngineMeta.setId(storageID);
            storage.updateStorageEngine(
                    storageID, storageEngineMeta); // 如果删除成功，则后续更新对应的 dummyFragament 的元数据
            if (storageEngineMeta.isHasData()) { // 确保内部数据的一致性
                StorageUnitMeta dummyStorageUnit = storageEngineMeta.getDummyStorageUnit();
                dummyStorageUnit.setStorageEngineId(storageID);
                dummyStorageUnit.setId(StorageUnitMeta.generateDummyStorageUnitID(storageID));
                dummyStorageUnit.setMasterId(dummyStorageUnit.getId());
                FragmentMeta dummyFragment = storageEngineMeta.getDummyFragment();
                dummyFragment.setMasterStorageUnit(dummyStorageUnit);
                dummyFragment.setMasterStorageUnitId(dummyStorageUnit.getId());
            }
            return cache.updateStorageEngine(storageID, storageEngineMeta);
        } catch (MetaStorageException e) {
            logger.error("update storage engines error:", e);
        }
        return false;
    }

    @Override
    public List<StorageEngineMeta> getStorageEngineList() {
        return new ArrayList<>(cache.getStorageEngineList());
    }

    @Override
    public List<StorageEngineMeta> getWriteableStorageEngineList() {
        return cache.getStorageEngineList()
                .stream()
                .filter(e -> !e.isReadOnly())
                .collect(Collectors.toList());
    }

    @Override
    public int getStorageEngineNum() {
        return cache.getStorageEngineList().size();
    }

    @Override
    public StorageEngineMeta getStorageEngine(long id) {
        return cache.getStorageEngine(id);
    }

    @Override
    public StorageUnitMeta getStorageUnit(String id) {
        return cache.getStorageUnit(id);
    }

    @Override
    public Map<String, StorageUnitMeta> getStorageUnits(Set<String> ids) {
        return cache.getStorageUnits(ids);
    }

    @Override
    public List<StorageUnitMeta> getStorageUnits() {
        return cache.getStorageUnits();
    }

    @Override
    public List<IginxMeta> getIginxList() {
        return new ArrayList<>(cache.getIginxList());
    }

    @Override
    public long getIginxId() {
        return id;
    }

    @Override
    public List<FragmentMeta> getFragments() {
        return cache.getFragments();
    }

    @Override
    public List<FragmentMeta> getFragmentsByStorageUnit(String storageUnitId) {
        return cache.getFragmentListByStorageUnitId(storageUnitId);
    }

    @Override
    public Pair<ColumnsRange, KeyInterval> getBoundaryOfStorageUnit(String storageUnitId) {
        List<FragmentMeta> fragmentMetaList = cache.getFragmentListByStorageUnitId(storageUnitId);

        String startPath = fragmentMetaList.get(0).getColumnsRange().getStartColumn();
        String endPath = fragmentMetaList.get(0).getColumnsRange().getEndColumn();
        long startTime = fragmentMetaList.get(0).getKeyInterval().getStartKey();
        long endTime = fragmentMetaList.get(0).getKeyInterval().getEndKey();

        for (int i = 1; i < fragmentMetaList.size(); i++) {
            FragmentMeta meta = fragmentMetaList.get(i);
            if (meta.getKeyInterval().getStartKey() < startTime) {
                startTime = meta.getKeyInterval().getStartKey();
            }
            if (meta.getKeyInterval().getEndKey() > endTime) {
                endTime = meta.getKeyInterval().getEndKey();
            }
            if (startPath != null) {
                if (StringUtils.compare(startPath, meta.getColumnsRange().getStartColumn(), true)
                        > 0) {
                    startPath = meta.getColumnsRange().getStartColumn();
                }
            }
            if (endPath != null) {
                if (StringUtils.compare(endPath, meta.getColumnsRange().getEndColumn(), false)
                        < 0) {
                    endPath = meta.getColumnsRange().getEndColumn();
                }
            }
        }
        return new Pair<>(
                new ColumnsInterval(startPath, endPath), new KeyInterval(startTime, endTime));
    }

    @Override
    public Map<ColumnsRange, List<FragmentMeta>> getFragmentMapByColumnsRange(
            ColumnsRange columnsRange) {
        return getFragmentMapByColumnsRange(columnsRange, false);
    }

    @Override
    public Map<ColumnsRange, List<FragmentMeta>> getFragmentMapByColumnsRange(
            ColumnsRange columnsRange, boolean withDummyFragment) {
        Map<ColumnsRange, List<FragmentMeta>> fragmentsMap;
        if (cache.enableFragmentCacheControl()
                && cache.getFragmentMinTimestamp() > 0L) { // 最老的分片被逐出去了
            KeyInterval beforeKeyInterval = new KeyInterval(0L, cache.getFragmentMinTimestamp());
            fragmentsMap =
                    storage.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                            columnsRange, beforeKeyInterval);
            updateStorageUnitReference(fragmentsMap);
            Map<ColumnsRange, List<FragmentMeta>> recentFragmentsMap =
                    cache.getFragmentMapByTimeSeriesInterval(columnsRange);
            for (ColumnsRange ts : recentFragmentsMap.keySet()) {
                List<FragmentMeta> fragments = recentFragmentsMap.get(ts);
                if (fragmentsMap.containsKey(ts)) {
                    fragmentsMap.get(ts).addAll(fragments);
                } else {
                    recentFragmentsMap.put(ts, fragments);
                }
            }
        } else {
            fragmentsMap = cache.getFragmentMapByTimeSeriesInterval(columnsRange);
        }
        if (withDummyFragment) {
            List<FragmentMeta> fragmentList =
                    cache.getDummyFragmentsByTimeSeriesInterval(columnsRange);
            mergeToFragmentMap(fragmentsMap, fragmentList);
        }
        return fragmentsMap;
    }

    @Override
    public boolean hasDummyFragment(ColumnsRange columnsRange) {
        List<FragmentMeta> fragmentList = cache.getDummyFragmentsByTimeSeriesInterval(columnsRange);
        return !fragmentList.isEmpty();
    }

    @Override
    public Map<ColumnsRange, FragmentMeta> getLatestFragmentMapByColumnsRange(
            ColumnsRange columnsRange) {
        return cache.getLatestFragmentMapByTimeSeriesInterval(columnsRange);
    }

    @Override
    public Map<ColumnsRange, FragmentMeta> getLatestFragmentMap() {
        return cache.getLatestFragmentMap();
    }

    @Override
    public Map<ColumnsRange, List<FragmentMeta>> getFragmentMapByColumnsIntervalAndKeyInterval(
            ColumnsRange columnsRange, KeyInterval keyInterval) {
        return getFragmentMapByColumnsIntervalAndKeyInterval(columnsRange, keyInterval, false);
    }

    @Override
    public Map<ColumnsRange, List<FragmentMeta>> getFragmentMapByColumnsIntervalAndKeyInterval(
            ColumnsRange columnsRange, KeyInterval keyInterval, boolean withDummyFragment) {
        Map<ColumnsRange, List<FragmentMeta>> fragmentsMap;
        if (cache.enableFragmentCacheControl()
                && keyInterval.getStartKey() < cache.getFragmentMinTimestamp()) {
            KeyInterval beforeKeyInterval =
                    new KeyInterval(keyInterval.getStartKey(), cache.getFragmentMinTimestamp());
            fragmentsMap =
                    storage.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                            columnsRange, beforeKeyInterval);
            updateStorageUnitReference(fragmentsMap);
            Map<ColumnsRange, List<FragmentMeta>> recentFragmentsMap =
                    cache.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                            columnsRange, keyInterval);
            for (ColumnsRange ts : recentFragmentsMap.keySet()) {
                List<FragmentMeta> fragments = recentFragmentsMap.get(ts);
                if (fragmentsMap.containsKey(ts)) {
                    fragmentsMap.get(ts).addAll(fragments);
                } else {
                    recentFragmentsMap.put(ts, fragments);
                }
            }
        } else {
            fragmentsMap =
                    cache.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                            columnsRange, keyInterval);
        }
        if (withDummyFragment) {
            List<FragmentMeta> fragmentList =
                    cache.getDummyFragmentsByTimeSeriesIntervalAndTimeInterval(
                            columnsRange, keyInterval);
            mergeToFragmentMap(fragmentsMap, fragmentList);
        }
        return fragmentsMap;
    }

    private void mergeToFragmentMap(
            Map<ColumnsRange, List<FragmentMeta>> fragmentsMap, List<FragmentMeta> fragmentList) {
        for (FragmentMeta fragment : fragmentList) {
            ColumnsRange tsInterval = fragment.getColumnsRange();
            if (!fragmentsMap.containsKey(tsInterval)) {
                fragmentsMap.put(tsInterval, new ArrayList<>());
            }
            List<FragmentMeta> currentFragmentList = fragmentsMap.get(tsInterval);
            int index = 0;
            while (index < currentFragmentList.size()) {
                if (currentFragmentList.get(index).getKeyInterval().getStartKey()
                        <= fragment.getKeyInterval().getStartKey()) {
                    index++;
                } else {
                    break;
                }
            }
            currentFragmentList.add(index, fragment);
        }
    }

    @Override
    public List<FragmentMeta> getFragmentListByColumnName(String colName) {
        if (cache.enableFragmentCacheControl() && cache.getFragmentMinTimestamp() > 0L) {
            KeyInterval beforeKeyInterval = new KeyInterval(0L, cache.getFragmentMinTimestamp());
            List<FragmentMeta> fragments =
                    storage.getFragmentListByTimeSeriesNameAndTimeInterval(
                            colName, beforeKeyInterval);
            updateStorageUnitReference(fragments);
            fragments.addAll(cache.getFragmentListByTimeSeriesName(colName));
            return fragments;
        }
        return cache.getFragmentListByTimeSeriesName(colName);
    }

    @Override
    public FragmentMeta getLatestFragmentByColumnName(String colName) { // 最新的分片数据必须被缓存
        return cache.getLatestFragmentByTimeSeriesName(colName);
    }

    @Override
    public List<FragmentMeta> getFragmentListByColumnNameAndKeyInterval(
            String colName, KeyInterval keyInterval) {
        if (cache.enableFragmentCacheControl()
                && keyInterval.getStartKey() < cache.getFragmentMinTimestamp()) {
            KeyInterval beforeKeyInterval =
                    new KeyInterval(keyInterval.getStartKey(), cache.getFragmentMinTimestamp());
            List<FragmentMeta> fragments =
                    storage.getFragmentListByTimeSeriesNameAndTimeInterval(
                            colName, beforeKeyInterval);
            updateStorageUnitReference(fragments);
            fragments.addAll(
                    cache.getFragmentListByTimeSeriesNameAndTimeInterval(colName, keyInterval));
            return fragments;
        }
        return cache.getFragmentListByTimeSeriesNameAndTimeInterval(colName, keyInterval);
    }

    @Override
    public boolean createFragmentsAndStorageUnits(
            List<StorageUnitMeta> storageUnits, List<FragmentMeta> fragments) {
        checkFragmentCompletion(fragments);
        try {
            storage.lockFragment();
            storage.lockStorageUnit();

            Map<String, StorageUnitMeta> fakeIdToStorageUnit = new HashMap<>(); // 假名翻译工具
            for (StorageUnitMeta masterStorageUnit : storageUnits) {
                masterStorageUnit.setCreatedBy(id);
                String fakeName = masterStorageUnit.getId();
                String actualName = storage.addStorageUnit();
                StorageUnitMeta actualMasterStorageUnit =
                        masterStorageUnit.renameStorageUnitMeta(actualName, actualName);
                cache.updateStorageUnit(actualMasterStorageUnit);
                for (StorageUnitHook hook : storageUnitHooks) {
                    hook.onChange(null, actualMasterStorageUnit);
                }
                storage.updateStorageUnit(actualMasterStorageUnit);
                fakeIdToStorageUnit.put(fakeName, actualMasterStorageUnit);
                for (StorageUnitMeta slaveStorageUnit : masterStorageUnit.getReplicas()) {
                    slaveStorageUnit.setCreatedBy(id);
                    String slaveFakeName = slaveStorageUnit.getId();
                    String slaveActualName = storage.addStorageUnit();
                    StorageUnitMeta actualSlaveStorageUnit =
                            slaveStorageUnit.renameStorageUnitMeta(slaveActualName, actualName);
                    actualMasterStorageUnit.addReplica(actualSlaveStorageUnit);
                    for (StorageUnitHook hook : storageUnitHooks) {
                        hook.onChange(null, actualSlaveStorageUnit);
                    }
                    cache.updateStorageUnit(actualSlaveStorageUnit);
                    storage.updateStorageUnit(actualSlaveStorageUnit);
                    fakeIdToStorageUnit.put(slaveFakeName, actualSlaveStorageUnit);
                }
            }

            Map<ColumnsRange, FragmentMeta> latestFragments = getLatestFragmentMap();
            for (FragmentMeta originalFragmentMeta : latestFragments.values()) {
                FragmentMeta fragmentMeta =
                        originalFragmentMeta.endFragmentMeta(
                                fragments.get(0).getKeyInterval().getStartKey());
                // 在更新分片时，先更新本地
                fragmentMeta.setUpdatedBy(id);
                cache.updateFragment(fragmentMeta);
                storage.updateFragment(fragmentMeta);
            }

            for (FragmentMeta fragmentMeta : fragments) {
                fragmentMeta.setCreatedBy(id);
                fragmentMeta.setInitialFragment(false);
                StorageUnitMeta storageUnit =
                        fakeIdToStorageUnit.get(fragmentMeta.getFakeStorageUnitId());
                if (storageUnit.isMaster()) {
                    fragmentMeta.setMasterStorageUnit(storageUnit);
                } else {
                    fragmentMeta.setMasterStorageUnit(getStorageUnit(storageUnit.getMasterId()));
                }
                cache.addFragment(fragmentMeta);
                storage.addFragment(fragmentMeta);
            }
            return true;
        } catch (MetaStorageException e) {
            logger.error("create fragment error: ", e);
        } finally {
            try {
                storage.releaseFragment();
                storage.releaseStorageUnit();
            } catch (MetaStorageException e) {
                logger.error("release fragment lock error: ", e);
            }
        }
        return false;
    }

    @Override
    public FragmentMeta splitFragmentAndStorageUnit(
            StorageUnitMeta toAddStorageUnit, FragmentMeta toAddFragment, FragmentMeta fragment) {
        try {
            storage.lockFragment();
            storage.lockStorageUnit();

            // 更新du
            logger.info("update du");
            toAddStorageUnit.setCreatedBy(id);
            String actualName = storage.addStorageUnit();
            StorageUnitMeta actualMasterStorageUnit =
                    toAddStorageUnit.renameStorageUnitMeta(actualName, actualName);
            cache.updateStorageUnit(actualMasterStorageUnit);
            for (StorageUnitHook hook : storageUnitHooks) {
                hook.onChange(null, actualMasterStorageUnit);
            }
            storage.updateStorageUnit(actualMasterStorageUnit);
            for (StorageUnitMeta slaveStorageUnit : toAddStorageUnit.getReplicas()) {
                slaveStorageUnit.setCreatedBy(id);
                String slaveActualName = storage.addStorageUnit();
                StorageUnitMeta actualSlaveStorageUnit =
                        slaveStorageUnit.renameStorageUnitMeta(slaveActualName, actualName);
                actualMasterStorageUnit.addReplica(actualSlaveStorageUnit);
                for (StorageUnitHook hook : storageUnitHooks) {
                    hook.onChange(null, actualSlaveStorageUnit);
                }
                cache.updateStorageUnit(actualSlaveStorageUnit);
                storage.updateStorageUnit(actualSlaveStorageUnit);
            }

            // 结束旧分片
            cache.deleteFragmentByTsInterval(fragment.getColumnsRange(), fragment);
            fragment = fragment.endFragmentMeta(toAddFragment.getKeyInterval().getStartKey());
            cache.addFragment(fragment);
            fragment.setUpdatedBy(id);
            storage.updateFragment(fragment);

            // 更新新分片
            toAddFragment.setCreatedBy(id);
            toAddFragment.setInitialFragment(false);
            if (toAddStorageUnit.isMaster()) {
                toAddFragment.setMasterStorageUnit(actualMasterStorageUnit);
            } else {
                toAddFragment.setMasterStorageUnit(
                        getStorageUnit(actualMasterStorageUnit.getMasterId()));
            }
            cache.addFragment(toAddFragment);
            storage.addFragment(toAddFragment);
        } catch (MetaStorageException e) {
            logger.error("create fragment error: ", e);
        } finally {
            try {
                storage.releaseFragment();
                storage.releaseStorageUnit();
            } catch (MetaStorageException e) {
                logger.error("release fragment lock error: ", e);
            }
        }

        return fragment;
    }

    @Override
    public void removeFragment(FragmentMeta fragmentMeta) {
        try {
            storage.lockFragment();
            cache.deleteFragmentByTsInterval(fragmentMeta.getColumnsRange(), fragmentMeta);
            storage.removeFragment(fragmentMeta);
        } catch (MetaStorageException e) {
            logger.error("remove fragment error: ", e);
        } finally {
            try {
                storage.releaseFragment();
            } catch (MetaStorageException e) {
                logger.error("release fragment lock error: ", e);
            }
        }
    }

    @Override
    public void addFragment(FragmentMeta fragmentMeta) {
        try {
            storage.lockFragment();
            cache.addFragment(fragmentMeta);
            storage.addFragment(fragmentMeta);
        } catch (MetaStorageException e) {
            logger.error("add fragment error: ", e);
        } finally {
            try {
                storage.releaseFragment();
            } catch (MetaStorageException e) {
                logger.error("release fragment lock error: ", e);
            }
        }
    }

    @Override
    public void endFragmentByColumnsRange(FragmentMeta fragmentMeta, String endColumn) {
        try {
            storage.lockFragment();
            ColumnsRange sourceTsInterval =
                    new ColumnsInterval(
                            fragmentMeta.getColumnsRange().getStartColumn(),
                            fragmentMeta.getColumnsRange().getEndColumn());
            cache.deleteFragmentByTsInterval(fragmentMeta.getColumnsRange(), fragmentMeta);
            fragmentMeta.getColumnsRange().setEndColumn(endColumn);
            cache.addFragment(fragmentMeta);
            storage.updateFragmentByColumnsRange(sourceTsInterval, fragmentMeta);
        } catch (MetaStorageException e) {
            logger.error("end fragment by time series interval error: ", e);
        } finally {
            try {
                storage.releaseFragment();
            } catch (MetaStorageException e) {
                logger.error("release fragment lock error: ", e);
            }
        }
    }

    @Override
    public void updateFragmentByColumnsRange(ColumnsRange columnsRange, FragmentMeta fragmentMeta) {
        try {
            storage.lockFragment();
            cache.updateFragmentByTsInterval(columnsRange, fragmentMeta);
            storage.updateFragmentByColumnsRange(columnsRange, fragmentMeta);
        } catch (Exception e) {
            logger.error("update fragment error: ", e);
        } finally {
            try {
                storage.releaseFragment();
            } catch (MetaStorageException e) {
                logger.error("release fragment lock error: ", e);
            }
        }
    }

    @Override
    public boolean hasFragment() {
        return cache.hasFragment();
    }

    private void checkInitialFragmentCompletion(List<FragmentMeta> fragments) {
        Map<Long, List<FragmentMeta>> fragmentsByStartTime = new HashMap<>();
        for (FragmentMeta fragment : fragments) {
            List<FragmentMeta> fragmentList =
                    fragmentsByStartTime.computeIfAbsent(
                            fragment.getKeyInterval().getStartKey(), e -> new ArrayList<>());
            fragmentList.add(fragment);
        }
        // 检查空间边界是否完备
        for (long startTime : fragmentsByStartTime.keySet()) {
            List<FragmentMeta> fragmentList = fragmentsByStartTime.get(startTime);
            long endTime = -1;
            Map<String, Integer> borders = new HashMap<>();
            for (FragmentMeta fragment : fragmentList) {
                if (endTime == -1) {
                    endTime = fragment.getKeyInterval().getEndKey();
                }
                if (endTime != fragment.getKeyInterval().getEndKey()) {
                    logger.error(
                            "fragments which have the same start time should also have the same end time");
                    return;
                }
                String startTs = fragment.getColumnsRange().getStartColumn();
                String endTs = fragment.getColumnsRange().getEndColumn();
                borders.put(startTs, borders.getOrDefault(startTs, 0) - 1);
                borders.put(endTs, borders.getOrDefault(endTs, 0) + 1);
            }
            for (String border : borders.keySet()) {
                if (borders.get(border) != 0) {
                    logger.error("initial fragments should be completion");
                    return;
                }
            }
        }
        // 检查时间边界是否完备
        Map<Long, Integer> timeBorders = new HashMap<>();
        for (long startTime : fragmentsByStartTime.keySet()) {
            long endTime = fragmentsByStartTime.get(startTime).get(0).getKeyInterval().getEndKey();
            timeBorders.put(startTime, timeBorders.getOrDefault(startTime, 0) - 1);
            timeBorders.put(endTime, timeBorders.getOrDefault(endTime, 0) + 1);
        }
        boolean seeZeroTime = false, seeMaxTime = false;
        for (long time : timeBorders.keySet()) {
            if (time == 0) {
                seeZeroTime = true;
                if (timeBorders.get(time) != -1) {
                    logger.error("initial fragments should be completion");
                    return;
                }
                continue;
            }
            if (time == Long.MAX_VALUE) {
                seeMaxTime = true;
                if (timeBorders.get(time) != 1) {
                    logger.error("initial fragments should be completion");
                    return;
                }
                continue;
            }
            if (timeBorders.get(time) != 0) {
                logger.error("initial fragments should be completion");
                return;
            }
        }
        if (!seeZeroTime || !seeMaxTime) {
            logger.error("initial fragments should be completion");
        }
    }

    private void checkFragmentCompletion(List<FragmentMeta> fragments) {
        long startTime = -1;
        Map<String, Integer> borders = new HashMap<>();
        for (FragmentMeta fragment : fragments) {
            if (fragment.getKeyInterval().getEndKey() != Long.MAX_VALUE) {
                logger.error("end time for new fragment should be Long.MAX_VALUE");
                return;
            }
            if (startTime == -1) {
                startTime = fragment.getKeyInterval().getStartKey();
            }
            if (startTime != fragment.getKeyInterval().getStartKey()) {
                logger.error(
                        "new fragments created at the same time should have the same start time");
                return;
            }
            String startTs = fragment.getColumnsRange().getStartColumn();
            String endTs = fragment.getColumnsRange().getEndColumn();
            borders.put(startTs, borders.getOrDefault(startTs, 0) - 1);
            borders.put(endTs, borders.getOrDefault(endTs, 0) + 1);
        }
        for (String border : borders.keySet()) {
            if (borders.get(border) != 0) {
                logger.error("new fragments created at the same time should be completion");
                return;
            }
        }
    }

    @Override
    public boolean createInitialFragmentsAndStorageUnits(
            List<StorageUnitMeta> storageUnits, List<FragmentMeta> initialFragments) {
        // 必须同时初始化 fragment 和 cache，并且这个方法的主体部分在任意时刻
        // 只能由某个 iginx 的某个线程执行
        if (cache.hasFragment() && cache.hasStorageUnit()) {
            return false;
        }
        checkInitialFragmentCompletion(initialFragments);
        List<StorageUnitMeta> newStorageUnits = new ArrayList<>();
        try {
            storage.lockFragment();
            storage.lockStorageUnit();

            // 接下来的部分只有一个 iginx 的一个线程执行
            if (cache.hasFragment() && cache.hasStorageUnit()) {
                return false;
            }
            // 查看一下服务器上是不是已经有了
            Map<String, StorageUnitMeta> globalStorageUnits = storage.loadStorageUnit();
            if (globalStorageUnits != null
                    && !globalStorageUnits.isEmpty()) { // 服务器上已经有人创建过了，本地只需要加载
                Map<ColumnsRange, List<FragmentMeta>> globalFragmentMap = storage.loadFragment();
                newStorageUnits.addAll(globalStorageUnits.values());
                newStorageUnits.sort(Comparator.comparing(StorageUnitMeta::getId));
                logger.warn("server has created storage unit, just need to load.");
                logger.warn("notify storage unit listeners.");
                for (StorageUnitHook hook : storageUnitHooks) {
                    for (StorageUnitMeta meta : newStorageUnits) {
                        hook.onChange(null, meta);
                    }
                }
                logger.warn("notify storage unit listeners finished.");
                // 再初始化缓存
                cache.initStorageUnit(globalStorageUnits);
                cache.initFragment(globalFragmentMap);
                return false;
            }

            // 确实没有人创建过，以我为准
            Map<String, StorageUnitMeta> fakeIdToStorageUnit = new HashMap<>(); // 假名翻译工具
            for (StorageUnitMeta masterStorageUnit : storageUnits) {
                masterStorageUnit.setCreatedBy(id);
                String fakeName = masterStorageUnit.getId();
                String actualName = storage.addStorageUnit();
                StorageUnitMeta actualMasterStorageUnit =
                        masterStorageUnit.renameStorageUnitMeta(actualName, actualName);
                storage.updateStorageUnit(actualMasterStorageUnit);
                fakeIdToStorageUnit.put(fakeName, actualMasterStorageUnit);
                for (StorageUnitMeta slaveStorageUnit : masterStorageUnit.getReplicas()) {
                    slaveStorageUnit.setCreatedBy(id);
                    String slaveFakeName = slaveStorageUnit.getId();
                    String slaveActualName = storage.addStorageUnit();
                    StorageUnitMeta actualSlaveStorageUnit =
                            slaveStorageUnit.renameStorageUnitMeta(slaveActualName, actualName);
                    actualMasterStorageUnit.addReplica(actualSlaveStorageUnit);
                    storage.updateStorageUnit(actualSlaveStorageUnit);
                    fakeIdToStorageUnit.put(slaveFakeName, actualSlaveStorageUnit);
                }
            }
            initialFragments.sort(Comparator.comparingLong(o -> o.getKeyInterval().getStartKey()));
            for (FragmentMeta fragmentMeta : initialFragments) {
                fragmentMeta.setCreatedBy(id);
                StorageUnitMeta storageUnit =
                        fakeIdToStorageUnit.get(fragmentMeta.getFakeStorageUnitId());
                if (storageUnit.isMaster()) {
                    fragmentMeta.setMasterStorageUnit(storageUnit);
                } else {
                    fragmentMeta.setMasterStorageUnit(getStorageUnit(storageUnit.getMasterId()));
                }
                storage.addFragment(fragmentMeta);
            }
            Map<String, StorageUnitMeta> loadedStorageUnits = storage.loadStorageUnit();
            newStorageUnits.addAll(loadedStorageUnits.values());
            newStorageUnits.sort(Comparator.comparing(StorageUnitMeta::getId));
            // 先通知
            logger.warn("i have created storage unit.");
            logger.warn("notify storage unit listeners.");
            for (StorageUnitHook hook : storageUnitHooks) {
                for (StorageUnitMeta meta : newStorageUnits) {
                    hook.onChange(null, meta);
                }
            }
            logger.warn("notify storage unit listeners finished.");
            // 再初始化缓存
            cache.initStorageUnit(loadedStorageUnits);
            cache.initFragment(storage.loadFragment());
            return true;
        } catch (MetaStorageException e) {
            logger.error("encounter error when init fragment: ", e);
        } finally {
            try {
                storage.releaseStorageUnit();
                storage.releaseFragment();
            } catch (MetaStorageException e) {
                logger.error("encounter error when release fragment lock: ", e);
            }
        }
        return false;
    }

    @Override
    public StorageUnitMeta generateNewStorageUnitMetaByFragment(
            FragmentMeta fragmentMeta, long targetStorageId) throws MetaStorageException {
        String actualName = storage.addStorageUnit();
        StorageUnitMeta storageUnitMeta =
                new StorageUnitMeta(actualName, targetStorageId, actualName, true, false);
        storageUnitMeta.setCreatedBy(getIginxId());

        cache.updateStorageUnit(storageUnitMeta);
        for (StorageUnitHook hook : storageUnitHooks) {
            hook.onChange(null, storageUnitMeta);
        }
        storage.updateStorageUnit(storageUnitMeta);
        return storageUnitMeta;
    }

    @Override
    public List<Long> selectStorageEngineIdList() {
        List<Long> storageEngineIdList =
                getWriteableStorageEngineList()
                        .stream()
                        .map(StorageEngineMeta::getId)
                        .collect(Collectors.toList());
        if (storageEngineIdList.size()
                <= 1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum()) {
            return storageEngineIdList;
        }
        Random random = new Random();
        for (int i = 0; i < storageEngineIdList.size(); i++) {
            int next = random.nextInt(storageEngineIdList.size());
            Long value = storageEngineIdList.get(next);
            storageEngineIdList.set(next, storageEngineIdList.get(i));
            storageEngineIdList.set(i, value);
        }
        return storageEngineIdList.subList(
                0, 1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum());
    }

    @Override
    public void registerStorageEngineChangeHook(StorageEngineChangeHook hook) {
        if (hook != null) {
            this.storageEngineChangeHooks.add(hook);
        }
    }

    @Override
    public void addOrUpdateSchemaMapping(String schema, Map<String, Integer> schemaMapping) {
        try {
            storage.updateSchemaMapping(schema, schemaMapping);
            if (schemaMapping == null) {
                cache.removeSchemaMapping(schema);
            } else {
                cache.addOrUpdateSchemaMapping(schema, schemaMapping);
            }
        } catch (MetaStorageException e) {
            logger.error("update schema mapping error: ", e);
        }
    }

    @Override
    public void addOrUpdateSchemaMappingItem(String schema, String key, int value) {
        Map<String, Integer> schemaMapping = cache.getSchemaMapping(schema);
        if (schemaMapping == null) {
            schemaMapping = new HashMap<>();
        }
        if (value == -1) {
            schemaMapping.remove(key);
        } else {
            schemaMapping.put(key, value);
        }
        try {
            storage.updateSchemaMapping(schema, schemaMapping);
            if (value == -1) {
                cache.removeSchemaMappingItem(schema, key);
            } else {
                cache.addOrUpdateSchemaMappingItem(schema, key, value);
            }
        } catch (MetaStorageException e) {
            logger.error("update schema mapping error: ", e);
        }
    }

    @Override
    public Map<String, Integer> getSchemaMapping(String schema) {
        return cache.getSchemaMapping(schema);
    }

    @Override
    public int getSchemaMappingItem(String schema, String key) {
        return cache.getSchemaMappingItem(schema, key);
    }

    private List<StorageEngineMeta> resolveStorageEngineFromConf() {
        List<StorageEngineMeta> storageEngineMetaList = new ArrayList<>();
        String[] storageEngineStrings =
                ConfigDescriptor.getInstance().getConfig().getStorageEngineList().split(",");
        for (int i = 0; i < storageEngineStrings.length; i++) {
            if (storageEngineStrings[i].length() == 0) {
                continue;
            }
            String[] storageEngineParts = storageEngineStrings[i].split("#");
            String ip = storageEngineParts[0];
            int port = -1;
            if (!storageEngineParts[1].equals("")) {
                port = Integer.parseInt(storageEngineParts[1]);
            }
            String storageEngine = storageEngineParts[2];
            Map<String, String> extraParams = new HashMap<>();
            String[] KAndV;
            for (int j = 3; j < storageEngineParts.length; j++) {
                if (storageEngineParts[j].contains("\"")) {
                    KAndV = storageEngineParts[j].split("\"");
                    extraParams.put(KAndV[0].substring(0, KAndV[0].length() - 1), KAndV[1]);
                } else {
                    KAndV = storageEngineParts[j].split("=");
                    if (KAndV.length != 2) {
                        logger.error(
                                "unexpected storage engine meta info: " + storageEngineStrings[i]);
                        continue;
                    }
                    extraParams.put(KAndV[0], KAndV[1]);
                }
            }
            boolean hasData =
                    Boolean.parseBoolean(extraParams.getOrDefault(Constants.HAS_DATA, "false"));
            String dataPrefix = null;
            if (hasData && extraParams.containsKey(Constants.DATA_PREFIX)) {
                dataPrefix = extraParams.get(Constants.DATA_PREFIX);
            }
            boolean readOnly =
                    Boolean.parseBoolean(extraParams.getOrDefault(Constants.IS_READ_ONLY, "false"));
            StorageEngineMeta storage =
                    new StorageEngineMeta(
                            i,
                            ip,
                            port,
                            hasData,
                            dataPrefix,
                            readOnly,
                            extraParams,
                            storageEngine,
                            id);
            if (hasData) {
                StorageUnitMeta dummyStorageUnit =
                        new StorageUnitMeta(StorageUnitMeta.generateDummyStorageUnitID(i), i);
                Pair<ColumnsRange, KeyInterval> boundary =
                        StorageManager.getBoundaryOfStorage(storage);
                FragmentMeta dummyFragment;
                if (dataPrefix == null) {
                    dummyFragment = new FragmentMeta(boundary.k, boundary.v, dummyStorageUnit);
                } else {
                    dummyFragment =
                            new FragmentMeta(
                                    new ColumnsInterval(
                                            dataPrefix, StringUtils.nextString(dataPrefix)),
                                    boundary.v,
                                    dummyStorageUnit);
                }
                dummyFragment.setDummyFragment(true);
                storage.setDummyStorageUnit(dummyStorageUnit);
                storage.setDummyFragment(dummyFragment);
            }
            storageEngineMetaList.add(storage);
        }
        return storageEngineMetaList;
    }

    private UserMeta resolveUserFromConf() {
        String username = ConfigDescriptor.getInstance().getConfig().getUsername();
        String password = ConfigDescriptor.getInstance().getConfig().getPassword();
        UserType userType = UserType.Administrator;
        Set<AuthType> auths = new HashSet<>();
        auths.add(AuthType.Read);
        auths.add(AuthType.Write);
        auths.add(AuthType.Admin);
        auths.add(AuthType.Cluster);
        return new UserMeta(username, password, userType, auths);
    }

    @Override
    public boolean addUser(UserMeta user) {
        try {
            storage.addUser(user);
            cache.addOrUpdateUser(user);
            return true;
        } catch (MetaStorageException e) {
            logger.error("add user error: ", e);
            return false;
        }
    }

    @Override
    public boolean updateUser(String username, String password, Set<AuthType> auths) {
        List<UserMeta> users = cache.getUser(Collections.singletonList(username));
        if (users.size() == 0) { // 待更新的用户不存在
            return false;
        }
        UserMeta user = users.get(0);
        if (password != null) {
            user.setPassword(password);
        }
        if (auths != null) {
            user.setAuths(auths);
        }
        try {
            storage.updateUser(user);
            cache.addOrUpdateUser(user);
            return true;
        } catch (MetaStorageException e) {
            logger.error("update user error: ", e);
            return false;
        }
    }

    @Override
    public boolean removeUser(String username) {
        try {
            storage.removeUser(username);
            cache.removeUser(username);
            return true;
        } catch (MetaStorageException e) {
            logger.error("remove user error: ", e);
            return false;
        }
    }

    @Override
    public UserMeta getUser(String username) {
        List<UserMeta> users = cache.getUser(Collections.singletonList(username));
        if (users.size() == 0) {
            return null;
        }
        return users.get(0);
    }

    protected void updateStorageUnitReference(Map<ColumnsRange, List<FragmentMeta>> fragmentsMap) {
        for (List<FragmentMeta> fragments : fragmentsMap.values()) {
            for (FragmentMeta fragment : fragments) {
                fragment.setMasterStorageUnit(
                        cache.getStorageUnit(fragment.getMasterStorageUnitId()));
            }
        }
    }

    protected void updateStorageUnitReference(List<FragmentMeta> fragments) {
        for (FragmentMeta fragment : fragments) {
            fragment.setMasterStorageUnit(cache.getStorageUnit(fragment.getMasterStorageUnitId()));
        }
    }

    @Override
    public List<UserMeta> getUsers() {
        return cache.getUser();
    }

    @Override
    public List<UserMeta> getUsers(List<String> username) {
        return cache.getUser(username);
    }

    @Override
    public void registerStorageUnitHook(StorageUnitHook hook) {
        this.storageUnitHooks.add(hook);
    }

    @Override
    public boolean election() {
        return storage.election();
    }

    @Override
    public void saveColumnsData(InsertStatement statement) {
        cache.saveTimeSeriesData(statement);
    }

    @Override
    public List<ColumnCalDO> getMaxValueFromColumns() {
        return cache.getMaxValueFromTimeSeries();
    }

    @Override
    public Map<String, Double> getColumnsData() {
        return storage.getTimeseriesData();
    }

    @Override
    public int updateVersion() {
        return storage.updateVersion();
    }

    @Override
    public Map<Integer, Integer> getColumnsVersionMap() {
        return cache.getTimeseriesVersionMap();
    }

    @Override
    public boolean addTransformTask(TransformTaskMeta transformTask) {
        try {
            storage.addTransformTask(transformTask);
            cache.addOrUpdateTransformTask(transformTask);
            return true;
        } catch (MetaStorageException e) {
            logger.error("add transform task error: ", e);
            return false;
        }
    }

    @Override
    public boolean updateTransformTask(TransformTaskMeta transformTask) {
        try {
            storage.updateTransformTask(transformTask);
            cache.addOrUpdateTransformTask(transformTask);
            return true;
        } catch (MetaStorageException e) {
            logger.error("add transform task error: ", e);
            return false;
        }
    }

    @Override
    public boolean dropTransformTask(String name) {
        try {
            cache.dropTransformTask(name);
            storage.dropTransformTask(name);
            return true;
        } catch (MetaStorageException e) {
            logger.error("drop transform task error: ", e);
            return false;
        }
    }

    @Override
    public TransformTaskMeta getTransformTask(String name) {
        return cache.getTransformTask(name);
    }

    @Override
    public List<TransformTaskMeta> getTransformTasks() {
        return cache.getTransformTasks();
    }

    @Override
    public void updateFragmentRequests(
            Map<FragmentMeta, Long> writeRequestsMap, Map<FragmentMeta, Long> readRequestsMap) {
        try {
            storage.lockFragmentRequestsCounter();
            storage.updateFragmentRequests(writeRequestsMap, readRequestsMap);
            storage.incrementFragmentRequestsCounter();
            storage.releaseFragmentRequestsCounter();
        } catch (Exception e) {
            logger.error("encounter error when update fragment requests: ", e);
        }
    }

    @Override
    public void updateFragmentHeat(
            Map<FragmentMeta, Long> writeHotspotMap, Map<FragmentMeta, Long> readHotspotMap) {
        try {
            storage.lockFragmentHeatCounter();
            storage.updateFragmentHeat(writeHotspotMap, readHotspotMap);
            storage.incrementFragmentHeatCounter();
            storage.releaseFragmentHeatCounter();
        } catch (Exception e) {
            logger.error("encounter error when update fragment heat: ", e);
        }
    }

    @Override
    public void clearMonitors() {
        try {
            Thread.sleep(1000);
            if (getIginxList().get(0).getId() == getIginxId()) {
                storage.lockFragmentRequestsCounter();
                storage.lockFragmentHeatCounter();
                storage.lockTimeseriesHeatCounter();

                storage.resetFragmentRequestsCounter();
                storage.resetFragmentHeatCounter();
                storage.resetTimeseriesHeatCounter();
                storage.removeFragmentRequests();
                storage.removeFragmentHeat();
                storage.removeTimeseriesHeat();

                storage.releaseFragmentRequestsCounter();
                storage.releaseFragmentHeatCounter();
                storage.releaseTimeseriesHeatCounter();
            }
            HotSpotMonitor.getInstance().clear();
            RequestsMonitor.getInstance().clear();
        } catch (Exception e) {
            logger.error("encounter error when clear monitors: ", e);
        }
    }

    @Override
    public Pair<Map<FragmentMeta, Long>, Map<FragmentMeta, Long>> loadFragmentHeat() {
        try {
            return storage.loadFragmentHeat(cache);
        } catch (Exception e) {
            logger.error("encounter error when remove fragment heat: ", e);
            return new Pair<>(new HashMap<>(), new HashMap<>());
        }
    }

    @Override
    public void updateFragmentPoints(FragmentMeta fragmentMeta, long points) {
        try {
            storage.updateFragmentPoints(fragmentMeta, points);
        } catch (Exception e) {
            logger.error("encounter error when add fragment points: ", e);
        }
    }

    @Override
    public Map<FragmentMeta, Long> loadFragmentPoints() {
        try {
            return storage.loadFragmentPoints(cache);
        } catch (Exception e) {
            logger.error("encounter error when load fragment points: ", e);
            return new HashMap<>();
        }
    }

    @Override
    public void updateMaxActiveEndKey(long endKey) {
        maxActiveEndTime.getAndUpdate(
                e ->
                        Math.max(
                                e,
                                endKey
                                        + ConfigDescriptor.getInstance()
                                                        .getConfig()
                                                        .getReshardFragmentTimeMargin()
                                                * 1000));
    }

    @Override
    public long getMaxActiveEndKey() {
        return maxActiveEndTime.get();
    }

    @Override
    public void submitMaxActiveEndKey() {
        try {
            storage.lockMaxActiveEndTimeStatistics();
            storage.addOrUpdateMaxActiveEndTimeStatistics(maxActiveEndTime.get());
            storage.releaseMaxActiveEndTimeStatistics();
        } catch (MetaStorageException e) {
            logger.error("encounter error when submitting max active time: ", e);
        }
    }
}
