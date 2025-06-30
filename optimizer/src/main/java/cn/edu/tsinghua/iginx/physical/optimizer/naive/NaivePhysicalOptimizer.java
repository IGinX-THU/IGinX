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
package cn.edu.tsinghua.iginx.physical.optimizer.naive;

import cn.edu.tsinghua.iginx.engine.physical.optimizer.PhysicalOptimizer;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.ReplicaDispatcher;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.BinaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.MultiMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.UnaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;

public class NaivePhysicalOptimizer implements PhysicalOptimizer {

  public static NaivePhysicalOptimizer getInstance() {
    return NaivePhysicalOptimizerHolder.INSTANCE;
  }

  @Override
  public PhysicalTask<BatchStream> optimize(Operator root, RequestContext context) {
    if (root == null) {
      return null;
    }
    NaivePhysicalPlanner planner = new NaivePhysicalPlanner();
    PhysicalTask<?> task = planner.construct(root.copy(), context);
    PhysicalTask<BatchStream> result = planner.convert(task, context, BatchStream.class);
    setFollowerTask(result);
    return result;
  }

  @Override
  public ConstraintManager getConstraintManager() {
    return NaiveConstraintManager.getInstance();
  }

  @Override
  public ReplicaDispatcher getReplicaDispatcher() {
    return NaiveReplicaDispatcher.getInstance();
  }

  private static PhysicalTask<?> setFollowerTask(PhysicalTask<?> task) {
    switch (task.getType()) {
      case Storage:
      case Global:
      case SourceMemory:
        break;
      case UnaryMemory:
        setFollowerTask(((UnaryMemoryPhysicalTask<?, ?>) task).getParentTask())
            .setFollowerTask(task);
        break;
      case BinaryMemory:
        setFollowerTask(((BinaryMemoryPhysicalTask<?, ?>) task).getParentTaskA())
            .setFollowerTask(task);
        setFollowerTask(((BinaryMemoryPhysicalTask<?, ?>) task).getParentTaskB())
            .setFollowerTask(task);
        break;
      case MultipleMemory:
        for (PhysicalTask<?> parentTask : ((MultiMemoryPhysicalTask<?, ?>) task).getParentTasks()) {
          setFollowerTask(parentTask).setFollowerTask(task);
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported task type: " + task.getType());
    }
    return task;
  }

  private static class NaivePhysicalOptimizerHolder {

    private static final NaivePhysicalOptimizer INSTANCE = new NaivePhysicalOptimizer();

    private NaivePhysicalOptimizerHolder() {}
  }
}
