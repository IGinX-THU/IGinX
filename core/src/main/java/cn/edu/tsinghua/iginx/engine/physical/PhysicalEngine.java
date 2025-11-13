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
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskResult;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStreams;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.resource.ResourceSet;
import java.util.concurrent.ExecutionException;

public interface PhysicalEngine {

  void submit(PhysicalTask<?> task);

  PhysicalOptimizer getOptimizer();

  ConstraintManager getConstraintManager();

  StorageManager getStorageManager();

  // 为了兼容过去的接口
  default RowStream execute(RequestContext ctx, Operator root) throws PhysicalException {
    ResourceSet resourceSet = new ResourceSet(String.format("request-%d", ctx.getId()));
    try {
      ctx.setAllocator(resourceSet.getAllocator());
      ctx.setConstantPool(resourceSet.getConstantPool());
      ctx.setTaskResultMap(resourceSet.getTaskResultMap());

      PhysicalTask<RowStream> task = getOptimizer().optimize(root, ctx, RowStream.class);
      ctx.setPhysicalTree(task);
      ctx.setPhysicalEngine(this);

      submit(task);

      try (TaskResult<RowStream> result = task.getResult().get()) {
        RowStream resultStream = result.unwrap();
        RowStream resultStreamWithResource = RowStreams.closeWith(resultStream, resourceSet::close);
        resourceSet = null;
        return resultStreamWithResource;
      } catch (ExecutionException | InterruptedException e) {
        throw new PhysicalException(e);
      }
    } finally {
      if (resourceSet != null) {
        resourceSet.close();
      }
    }
  }
}
