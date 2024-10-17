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
package cn.edu.tsinghua.iginx.physical.optimizer.naive.initializer;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.Accumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.unary.PhysicalSum;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.sink.AggregateExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.function.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.SetTransform;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class AggregateInfoGenerator implements UnaryExecutorFactory<AggregateExecutor> {

  private final SetTransform transform;

  public AggregateInfoGenerator(SetTransform transform) {
    this.transform = Objects.requireNonNull(transform);
  }

  @Override
  public AggregateExecutor initialize(ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    List<AggregateExecutor.AggregateInfo> infos = new ArrayList<>();
    for (FunctionCall functionCall : transform.getFunctionCallList()) {
      infos.addAll(generateAggregateInfo(context, inputSchema, functionCall));
    }
    return new AggregateExecutor(context, inputSchema, infos);
  }

  private static List<AggregateExecutor.AggregateInfo> generateAggregateInfo(
      ExecutorContext context, BatchSchema inputSchema, FunctionCall call) throws ComputeException {
    Function function = call.getFunction();
    if (function.getFunctionType() != FunctionType.System) {
      throw new UnsupportedOperationException("Not implemented yet");
    }
    FunctionParams params = call.getParams();
    if (function.getMappingType() != MappingType.SetMapping) {
      throw new ComputeException("Function " + function + " is not an aggregate function");
    }
    SetMappingFunction mappingFunction = (SetMappingFunction) function;
    if (params.isDistinct()) {
      throw new UnsupportedOperationException("Not implemented yet");
    }
    if (params.getPaths().size() != 1) {
      throw new ComputeException("Function " + function + " should have exactly one parameter");
    }
    List<Integer> matchedIndexes = Schemas.matchPattern(inputSchema, params.getPaths().get(0));
    Accumulator accumulator = getAccumulator(context, mappingFunction);
    List<AggregateExecutor.AggregateInfo> result = new ArrayList<>();
    for (int index : matchedIndexes) {
      AggregateExecutor.AggregateInfo info =
          new AggregateExecutor.AggregateInfo(
              accumulator, Collections.singletonList(new FieldNode(index)));
      result.add(info);
    }
    return result;
  }

  private static Accumulator getAccumulator(ExecutorContext context, SetMappingFunction function) {
    switch (function.getIdentifier()) {
      case PhysicalSum.NAME:
        return new PhysicalSum();
      default:
        throw new UnsupportedOperationException(
            "Unsupported function: " + function.getIdentifier());
    }
  }
}
