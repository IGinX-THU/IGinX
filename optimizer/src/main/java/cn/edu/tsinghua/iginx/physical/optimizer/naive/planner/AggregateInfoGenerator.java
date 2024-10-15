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
package cn.edu.tsinghua.iginx.physical.optimizer.naive.planner;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.Accumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.sum.PhysicalSumFloat8;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorInitializer;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.sink.AggregateExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.function.*;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Sum;
import cn.edu.tsinghua.iginx.engine.shared.operator.SetTransform;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

public class AggregateInfoGenerator
    implements UnaryExecutorInitializer<List<AggregateExecutor.AggregateInfo>> {

  private final SetTransform transform;

  public AggregateInfoGenerator(SetTransform transform) {
    this.transform = Objects.requireNonNull(transform);
  }

  @Override
  public List<AggregateExecutor.AggregateInfo> initialize(
      ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    List<AggregateExecutor.AggregateInfo> temp = new ArrayList<>();
    try {
      for (FunctionCall functionCall : transform.getFunctionCallList()) {
        temp.addAll(generateAggregateInfo(context, inputSchema, functionCall));
      }
      List<AggregateExecutor.AggregateInfo> result = new ArrayList<>(temp);
      temp.clear();
      return result;
    } finally {
      temp.forEach(AggregateExecutor.AggregateInfo::close);
    }
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
    List<AggregateExecutor.AggregateInfo> temp = new ArrayList<>();
    try {
      for (int index : matchedIndexes) {
        String name = getResultName(mappingFunction, inputSchema.getName(index));
        Field field = Schemas.fieldWithName(inputSchema.getField(index), name);
        Accumulator accumulator =
            createAccumulator(context, mappingFunction, inputSchema.getMinorType(index));
        temp.add(new AggregateExecutor.AggregateInfo(field, accumulator, index));
      }
      List<AggregateExecutor.AggregateInfo> result = new ArrayList<>(temp);
      temp.clear();
      return result;
    } finally {
      temp.forEach(AggregateExecutor.AggregateInfo::close);
    }
  }

  private static Accumulator createAccumulator(
      ExecutorContext context, SetMappingFunction function, Types.MinorType type) {
    if (function instanceof Sum) {
      switch (type) {
        case FLOAT8:
          return new PhysicalSumFloat8(context);
        default:
          throw new UnsupportedOperationException("Not implemented yet: " + type);
      }
    } else {
      throw new UnsupportedOperationException("Not implemented yet: " + function);
    }
  }

  private static String getResultName(SetMappingFunction function, String name) {
    return function.getIdentifier() + "(" + name + ")";
  }
}
