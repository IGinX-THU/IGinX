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
import cn.edu.tsinghua.iginx.engine.physical.task.GlobalPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskResult;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStreams;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Migration;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.migration.MigrationPhysicalExecutor;
import java.util.concurrent.ExecutionException;

public interface PhysicalEngine {

  void submit(PhysicalTask<?> task);

  PhysicalOptimizer getOptimizer();

  ConstraintManager getConstraintManager();

  StorageManager getStorageManager();

  StoragePhysicalTaskExecutor getStoragePhysicalTaskExecutor();

  // 为了兼容过去的接口
  default BatchStream execute(RequestContext ctx, Operator root) throws PhysicalException {

    if (OperatorType.isGlobalOperator(root.getType())) { // 全局任务临时兼容逻辑
      // 迁移任务单独处理
      if (root.getType() == OperatorType.Migration) {
        return MigrationPhysicalExecutor.getInstance()
            .execute(ctx, (Migration) root, getStoragePhysicalTaskExecutor());
      } else {
        GlobalPhysicalTask task = new GlobalPhysicalTask(root, ctx);
        getStoragePhysicalTaskExecutor().executeGlobalTask(task);
        try (TaskResult<RowStream> result = task.getResult().get()) {
          RowStream rowStream = result.unwrap();
          if (rowStream == null) {
            return null;
          }
          return BatchStreams.wrap(ctx.getAllocator(), result.unwrap(), ctx.getBatchRowCount());
        } catch (ExecutionException | InterruptedException e) {
          throw new PhysicalException(e);
        }
      }
    }

    // TODO
    //    if (!OperatorUtils.isProjectFromConstant(root)) {
    //      List<PhysicalTask> bottomTasks = new ArrayList<>();
    //      getBottomTasks(bottomTasks, task);
    //      commitBottomTasks(bottomTasks);
    //    }

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
}
