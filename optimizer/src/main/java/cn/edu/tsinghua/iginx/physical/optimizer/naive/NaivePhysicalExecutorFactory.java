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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.physical.optimizer.naive;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorType;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.pipeline.PipelineExecutor;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;

public class NaivePhysicalExecutorFactory {

  public ExecutorType getExecutorType(Operator operator) {
    switch (operator.getType()) {
      case Project:
      case Rename:
      case Reorder:
      case AddSchemaPrefix:
        return ExecutorType.Pipeline;
      default:
        throw new UnsupportedOperationException("Unsupported operator type: " + operator.getType());
    }
  }

  PipelineExecutor createPipelineExecutor(UnaryOperator operator) {
    switch (operator.getType()) {
      case Project:
      case Rename:
      case Reorder:
      case AddSchemaPrefix:
        throw new UnsupportedOperationException("Not implemented yet");
        //        return new Projector();
      default:
        throw new UnsupportedOperationException("Unsupported operator type: " + operator.getType());
    }
  }
}
