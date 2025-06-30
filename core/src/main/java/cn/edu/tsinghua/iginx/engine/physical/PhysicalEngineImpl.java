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
package cn.edu.tsinghua.iginx.engine.physical;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.memory.MemoryPhysicalTaskDispatcher;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.PhysicalOptimizer;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.PhysicalOptimizerManager;
import cn.edu.tsinghua.iginx.engine.physical.storage.StorageManager;
import cn.edu.tsinghua.iginx.engine.physical.storage.execute.StoragePhysicalTaskExecutor;
import cn.edu.tsinghua.iginx.engine.physical.task.GlobalPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.*;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
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
  public void submit(PhysicalTask<?> task) {
    List<PhysicalTask<?>> bottomTasks = new ArrayList<>();
    getBottomTasks(bottomTasks, task);
    List<StoragePhysicalTask> storageTasks = new ArrayList<>();
    List<GlobalPhysicalTask> globalTasks = new ArrayList<>();
    List<MemoryPhysicalTask<?>> memoryTasks = new ArrayList<>();
    for (PhysicalTask<?> bottomTask : bottomTasks) {
      if (bottomTask instanceof StoragePhysicalTask) {
        storageTasks.add((StoragePhysicalTask) bottomTask);
      } else if (bottomTask instanceof GlobalPhysicalTask) {
        globalTasks.add((GlobalPhysicalTask) bottomTask);
      } else if (bottomTask instanceof MemoryPhysicalTask) {
        memoryTasks.add((MemoryPhysicalTask<?>) bottomTask);
      } else {
        throw new IllegalStateException("unknown task class: " + task.getClass());
      }
    }
    storageTaskExecutor.commit(storageTasks);
    for (GlobalPhysicalTask globalTask : globalTasks) {
      storageTaskExecutor.executeGlobalTask(globalTask);
    }
    memoryTasks.forEach(memoryTaskExecutor::addMemoryTask);
  }

  public static void getBottomTasks(List<PhysicalTask<?>> tasks, PhysicalTask<?> root) {
    if (root == null) {
      return;
    }

    switch (root.getType()) {
      case Storage:
      case Global:
      case SourceMemory:
        tasks.add(root);
        break;
      case BinaryMemory:
        BinaryMemoryPhysicalTask<?, ?> binaryMemoryPhysicalTask =
            (BinaryMemoryPhysicalTask<?, ?>) root;
        getBottomTasks(tasks, binaryMemoryPhysicalTask.getParentTaskA());
        getBottomTasks(tasks, binaryMemoryPhysicalTask.getParentTaskB());
        break;
      case UnaryMemory:
        UnaryMemoryPhysicalTask<?, ?> unaryMemoryPhysicalTask =
            (UnaryMemoryPhysicalTask<?, ?>) root;
        getBottomTasks(tasks, unaryMemoryPhysicalTask.getParentTask());
        break;
      case MultipleMemory:
        MultiMemoryPhysicalTask<?, ?> multipleMemoryPhysicalTask =
            (MultiMemoryPhysicalTask<?, ?>) root;
        for (PhysicalTask<?> parentTask : multipleMemoryPhysicalTask.getParentTasks()) {
          getBottomTasks(tasks, parentTask);
        }
        break;
      default:
        throw new IllegalStateException("unknown task type: " + root.getType());
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
