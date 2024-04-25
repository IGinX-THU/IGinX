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

import static cn.edu.tsinghua.iginx.engine.physical.task.utils.TaskUtils.getBottomTasks;

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
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Migration;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.migration.MigrationPhysicalExecutor;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhysicalEngineImpl implements PhysicalEngine {
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(PhysicalEngineImpl.class);

  private static final PhysicalEngineImpl INSTANCE = new PhysicalEngineImpl();

  private final PhysicalOptimizer optimizer;

  private final MemoryPhysicalTaskDispatcher memoryTaskExecutor;

  private final StoragePhysicalTaskExecutor storageTaskExecutor;

  private PhysicalEngineImpl() {
    optimizer =
        PhysicalOptimizerManager.getInstance()
            .getOptimizer(ConfigDescriptor.getInstance().getConfig().getPhysicalOptimizer());
    memoryTaskExecutor = MemoryPhysicalTaskDispatcher.getInstance();
    storageTaskExecutor = StoragePhysicalTaskExecutor.getInstance();
    storageTaskExecutor.init(memoryTaskExecutor, optimizer.getReplicaDispatcher());
    memoryTaskExecutor.startDispatcher();
  }

  public static PhysicalEngineImpl getInstance() {
    return INSTANCE;
  }

  @Override
  public RowStream execute(RequestContext ctx, Operator root) throws PhysicalException {
    if (OperatorType.isGlobalOperator(root.getType())) { // 全局任务临时兼容逻辑
      // 迁移任务单独处理
      if (root.getType() == OperatorType.Migration) {
        return MigrationPhysicalExecutor.getInstance()
            .execute(ctx, (Migration) root, storageTaskExecutor);
      } else {
        GlobalPhysicalTask task = new GlobalPhysicalTask(root, ctx);
        TaskExecuteResult result = storageTaskExecutor.executeGlobalTask(task);
        if (result.getException() != null) {
          throw result.getException();
        }
        return result.getRowStream();
      }
    }
    PhysicalTask task = optimizer.optimize(root, ctx);
    ctx.setPhysicalTree(task);
    List<PhysicalTask> bottomTasks = new ArrayList<>();
    getBottomTasks(bottomTasks, task);
    commitBottomTasks(bottomTasks);
    TaskExecuteResult result = task.getResult();
    if (result.getException() != null) {
      throw result.getException();
    }
    return result.getRowStream();
  }

  private void commitBottomTasks(List<PhysicalTask> bottomTasks) {
    List<StoragePhysicalTask> storageTasks = new ArrayList<>();
    List<GlobalPhysicalTask> globalTasks = new ArrayList<>();
    List<IGinXPhysicalTask> iGinXTasks = new ArrayList<>();
    for (PhysicalTask task : bottomTasks) {
      if (task.getType().equals(TaskType.Storage)) {
        storageTasks.add((StoragePhysicalTask) task);
      } else if (task.getType().equals(TaskType.Global)) {
        globalTasks.add((GlobalPhysicalTask) task);
      } else if (task instanceof IGinXPhysicalTask) {
        iGinXTasks.add((IGinXPhysicalTask) task);
      }
    }
    storageTaskExecutor.commit(storageTasks);
    for (IGinXPhysicalTask iGinXTask : iGinXTasks) {
      memoryTaskExecutor.addMemoryTask(iGinXTask);
    }
    for (GlobalPhysicalTask globalTask : globalTasks) {
      storageTaskExecutor.executeGlobalTask(globalTask);
    }
  }

  @Override
  public PhysicalOptimizer getOptimizer() {
    return optimizer;
  }

  @Override
  public ConstraintManager getConstraintManager() {
    return optimizer.getConstraintManager();
  }

  @Override
  public StoragePhysicalTaskExecutor getStoragePhysicalTaskExecutor() {
    return storageTaskExecutor;
  }

  @Override
  public StorageManager getStorageManager() {
    return storageTaskExecutor.getStorageManager();
  }
}
