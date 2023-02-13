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
package cn.edu.tsinghua.iginx.engine.physical;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.MemoryPhysicalTaskDispatcher;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.PhysicalOptimizer;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.PhysicalOptimizerManager;
import cn.edu.tsinghua.iginx.engine.physical.storage.StorageManager;
import cn.edu.tsinghua.iginx.engine.physical.storage.execute.StoragePhysicalTaskExecutor;
import cn.edu.tsinghua.iginx.engine.physical.task.*;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawData;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Migration;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.util.Collections;

import org.apache.curator.shaded.com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class PhysicalEngineImpl implements PhysicalEngine {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(PhysicalEngineImpl.class);

//    private final ExecutorService migrationService = new ThreadPoolExecutor(5,
//            Integer.MAX_VALUE,
//            60L, TimeUnit.SECONDS, new SynchronousQueue<>());

    private static final PhysicalEngineImpl INSTANCE = new PhysicalEngineImpl();

    private final PhysicalOptimizer optimizer;

    private final MemoryPhysicalTaskDispatcher memoryTaskExecutor;

    private final StoragePhysicalTaskExecutor storageTaskExecutor;

    private final RateLimiter rateLimiter = RateLimiter.create(500000);

    private PhysicalEngineImpl() {
        optimizer = PhysicalOptimizerManager.getInstance().getOptimizer(ConfigDescriptor.getInstance().getConfig().getPhysicalOptimizer());
        memoryTaskExecutor = MemoryPhysicalTaskDispatcher.getInstance();
        storageTaskExecutor = StoragePhysicalTaskExecutor.getInstance();
        storageTaskExecutor.init(memoryTaskExecutor, optimizer.getReplicaDispatcher());
        memoryTaskExecutor.startDispatcher();
    }

    public static PhysicalEngineImpl getInstance() {
        return INSTANCE;
    }

    @Override
    public RowStream execute(RequestContext context, Operator root) throws PhysicalException {
        if (OperatorType.isGlobalOperator(root.getType())) { // 全局任务临时兼容逻辑
            // 迁移任务单独处理
            if (root.getType() == OperatorType.Migration) {
                Migration migration = (Migration) root;
                FragmentMeta toMigrateFragment = migration.getFragmentMeta();
                StorageUnitMeta targetStorageUnitMeta = migration.getTargetStorageUnitMeta();
                TimeInterval timeInterval = toMigrateFragment.getTimeInterval();
                long startTs = timeInterval.getStartTime();
                long lastTs = timeInterval.getEndTime();
                if (migration.getSourceStorageUnitId() != null) {
                    lastTs = GlobalCache.storageUnitLastTs.get(migration.getSourceStorageUnitId());
                }
                // 查询分区数据
                List<Operator> operators = new ArrayList<>();
                Project project = new Project(new FragmentSource(toMigrateFragment.copy(new TimeInterval(startTs, lastTs))), Collections.singletonList("*"), null);
                operators.add(project);

                StoragePhysicalTask physicalTask = new StoragePhysicalTask(operators, context);

                if (migration.getSourceStorageUnitId() != null) {
                    storageTaskExecutor.commitWithTargetStorageUnitId(physicalTask, migration.getSourceStorageUnitId());
                } else {
                    storageTaskExecutor.commit(physicalTask);
                }

                logger.info("wait for select result...");
                TaskExecuteResult selectResult = physicalTask.getResult();
                logger.info("wait for select result success");
                RowStream selectRowStream = selectResult.getRowStream();

                logger.info("[FaultTolerance][PhysicalEngineImpl] migration select exception: {}, rowStream: {}", selectResult.getException(), selectRowStream);


                List<String> selectResultPaths = new ArrayList<>();
                List<DataType> selectResultTypes = new ArrayList<>();
                selectRowStream.getHeader().getFields().forEach(field -> {
                    selectResultPaths.add(field.getName());
                    selectResultTypes.add(field.getType());
                });

                List<Long> timestampList = new ArrayList<>();
                List<ByteBuffer> valuesList = new ArrayList<>();
                List<Bitmap> bitmapList = new ArrayList<>();
                List<ByteBuffer> bitmapBufferList = new ArrayList<>();

//                List<Future<Boolean>> futures = new ArrayList<>();

                boolean hasTimestamp = selectRowStream.getHeader().hasKey();
                while (selectRowStream.hasNext()) {
                    Row row = selectRowStream.next();
                    Object[] rowValues = row.getValues();
                    valuesList.add(ByteUtils.getRowByteBuffer(rowValues, selectResultTypes));
                    Bitmap bitmap = new Bitmap(rowValues.length);
                    for (int i = 0; i < rowValues.length; i++) {
                        if (rowValues[i] != null) {
                            bitmap.mark(i);
                        }
                    }
                    bitmapBufferList.add(ByteBuffer.wrap(bitmap.getBytes()));
                    bitmapList.add(bitmap);
                    if (hasTimestamp) {
                        timestampList.add(row.getKey());
                    }

                    // 按行批量插入数据
                    if (timestampList.size() == ConfigDescriptor.getInstance().getConfig()
                        .getMigrationBatchSize()) {
                        //logger.info("Migration Progress of {}: {} to {}", migration.getSourceStorageUnitId(), timestampList.get(timestampList.size() - 1), lastTs);
                        List<Long> currTimestampList = timestampList;
                        List<ByteBuffer> currValuesList = valuesList;
                        List<Bitmap> currBitmapList = bitmapList;
                        List<ByteBuffer> currBitmapBufferList = bitmapBufferList;
//                        futures.add(migrationService.submit(() -> {
//                            try {
//                                insertDataByBatch(currTimestampList, currValuesList, currBitmapList, currBitmapBufferList,
//                                        toMigrateFragment, selectResultPaths, selectResultTypes, targetStorageUnitMeta.getId());
//                                return true;
//                            } catch (Exception e) {
//                                logger.error("Migration data failure!", e);
//                                return false;
//                            }
//                        }));
                        insertDataByBatch(currTimestampList, currValuesList, currBitmapList, currBitmapBufferList,
                                toMigrateFragment, selectResultPaths, selectResultTypes, targetStorageUnitMeta.getId());
                        timestampList = new ArrayList<>();
                        valuesList = new ArrayList<>();
                        bitmapList = new ArrayList<>();
                        bitmapBufferList = new ArrayList<>();
                    }
                }
                insertDataByBatch(timestampList, valuesList, bitmapList, bitmapBufferList,
                    toMigrateFragment, selectResultPaths, selectResultTypes, targetStorageUnitMeta.getId());

//                for (Future<Boolean> future: futures) {
//                    try {
//                        future.get();
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }

                // 设置分片现在所属的du
                //toMigrateFragment.setMasterStorageUnit(targetStorageUnitMeta);
                logger.info("Migration for {} finished", migration.getSourceStorageUnitId());
                return selectResult.getRowStream();
            } else {
                GlobalPhysicalTask task = new GlobalPhysicalTask(root, context);
                TaskExecuteResult result = storageTaskExecutor.executeGlobalTask(task);
                if (result.getException() != null) {
                    throw result.getException();
                }
                return result.getRowStream();
            }
        }
        PhysicalTask task = optimizer.optimize(context, root);
        List<StoragePhysicalTask> storageTasks = new ArrayList<>();
        List<MemoryPhysicalTask> memoryTasks = new ArrayList<>();
        getLeafTasks(storageTasks, memoryTasks, task);
        storageTaskExecutor.commit(storageTasks);
        memoryTaskExecutor.commit(memoryTasks);
        TaskExecuteResult result = task.getResult();
        if (result.getException() != null) {
            throw result.getException();
        }
        return result.getRowStream();
    }

    private void insertDataByBatch(List<Long> timestampList, List<ByteBuffer> valuesList,
        List<Bitmap> bitmapList, List<ByteBuffer> bitmapBufferList, FragmentMeta toMigrateFragment,
        List<String> selectResultPaths, List<DataType> selectResultTypes, String storageUnitId)
        throws PhysicalException {
        // 按行批量插入数据
        int metrics = 0;
        for (Bitmap bitmap: bitmapList) {
            metrics += bitmap.getCount();
        }
        rateLimiter.acquire(metrics);

        RawData rowData = new RawData(selectResultPaths, Collections.emptyList(), timestampList,
            ByteUtils.getRowValuesByDataType(valuesList, selectResultTypes, bitmapBufferList),
            selectResultTypes, bitmapList, RawDataType.NonAlignedRow);
        RowDataView rowDataView = new RowDataView(rowData, 0, selectResultPaths.size(), 0,
            timestampList.size());
        List<Operator> insertOperators = new ArrayList<>();
        insertOperators.add(new Insert(new FragmentSource(toMigrateFragment), rowDataView));
        StoragePhysicalTask insertPhysicalTask = new StoragePhysicalTask(insertOperators, null);
        long startTs = System.currentTimeMillis();
        storageTaskExecutor.commitWithTargetStorageUnitId(insertPhysicalTask, storageUnitId);
        TaskExecuteResult insertResult = insertPhysicalTask.getResult();
        long span = System.currentTimeMillis() - startTs;
        logger.info("[FaultTolerance][PhysicalEngineImpl][YuanZi][Throughput] migration throughput: {}, span: {} ms", metrics, span);
        if (insertResult.getException() != null) {
            throw insertResult.getException();
        }
    }

    private void getLeafTasks(List<StoragePhysicalTask> storageTasks, List<MemoryPhysicalTask> memoryTasks, PhysicalTask root) {
        if (root == null) {
            return;
        }
        if (root.getType() == TaskType.Storage) {
            storageTasks.add((StoragePhysicalTask) root);
        } else if (root.getType() == TaskType.BinaryMemory) {
            BinaryMemoryPhysicalTask task = (BinaryMemoryPhysicalTask) root;
            getLeafTasks(storageTasks, memoryTasks, task.getParentTaskA());
            getLeafTasks(storageTasks, memoryTasks, task.getParentTaskB());
        } else if (root.getType() == TaskType.UnaryMemory) {
            UnaryMemoryPhysicalTask task = (UnaryMemoryPhysicalTask) root;
            if (task.hasParentTask()) {
                getLeafTasks(storageTasks, memoryTasks, task.getParentTask());
            } else {
                memoryTasks.add(task);
            }
        } else if (root.getType() == TaskType.MultipleMemory) {
            MultipleMemoryPhysicalTask task = (MultipleMemoryPhysicalTask) root;
            for (PhysicalTask parentTask : task.getParentTasks()) {
                getLeafTasks(storageTasks, memoryTasks, parentTask);
            }
        }
    }

    @Override
    public ConstraintManager getConstraintManager() {
        return optimizer.getConstraintManager();
    }

    @Override
    public StorageManager getStorageManager() {
        return storageTaskExecutor.getStorageManager();
    }
}
