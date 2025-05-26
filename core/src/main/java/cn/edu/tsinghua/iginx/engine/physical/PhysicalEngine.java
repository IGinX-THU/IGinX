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

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.PhysicalOptimizer;
import cn.edu.tsinghua.iginx.engine.physical.storage.StorageManager;
import cn.edu.tsinghua.iginx.engine.physical.storage.execute.StoragePhysicalTaskExecutor;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskMetrics;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskResult;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.row.BatchStreamToRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.concurrent.ExecutionException;

public interface PhysicalEngine {

  void submit(PhysicalTask<?> task);

  PhysicalOptimizer getOptimizer();

  ConstraintManager getConstraintManager();

  StorageManager getStorageManager();

  StoragePhysicalTaskExecutor getStoragePhysicalTaskExecutor();

  // 为了兼容过去的接口
  default BatchStream execute(RequestContext ctx, Operator root) throws PhysicalException {
    PhysicalTask<BatchStream> task = getOptimizer().optimize(root, ctx);
    ctx.setPhysicalTree(task);
    ctx.setPhysicalEngine(this);
    submit(task);
    try (TaskResult<BatchStream> result = task.getResult().get()) {
      return result.unwrap();
    } catch (ExecutionException | InterruptedException e) {
      throw new PhysicalException(e);
    }
  }

  // 为了兼容过去的接口
  default RowStream executeAsRowStream(RequestContext ctx, Operator root) throws PhysicalException {
    return new BatchStreamToRowStreamWrapper(execute(ctx, root), TaskMetrics.NO_OP);
  }
}
