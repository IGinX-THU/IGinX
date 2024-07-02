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
  private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalEngineImpl.class);

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
    for (PhysicalTask task : bottomTasks) {
      if (task.getType().equals(TaskType.Storage)) {
        storageTasks.add((StoragePhysicalTask) task);
      } else if (task.getType().equals(TaskType.Global)) {
        globalTasks.add((GlobalPhysicalTask) task);
      }
    }
    storageTaskExecutor.commit(storageTasks);
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
