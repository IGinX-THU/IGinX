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
package cn.edu.tsinghua.iginx.engine.physical.storage.execute.pushdown.strategy;

import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import java.util.List;

public class NoPushDownStrategy extends PushDownStrategy {

  @Override
  public TaskExecuteResult execute(
      Project project,
      List<Operator> operators,
      DataArea dataArea,
      IStorage storage,
      boolean isDummyStorageUnit,
      RequestContext requestContext) {
    TaskExecuteResult result =
        isDummyStorageUnit
            ? storage.executeProjectDummy(project, dataArea)
            : storage.executeProject(project, dataArea);

    List<Operator> remainingOperators = getRemainingOperators(operators, 1);
    executeRemainingOperators(remainingOperators, result, requestContext);
    return result;
  }
}
