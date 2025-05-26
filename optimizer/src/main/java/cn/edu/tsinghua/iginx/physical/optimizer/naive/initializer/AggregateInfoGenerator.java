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
package cn.edu.tsinghua.iginx.physical.optimizer.naive.initializer;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.expression.ExpressionAccumulation;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.expression.ExpressionAccumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.unary.*;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressionUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateful.AggregateUnaryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.utils.PhysicalExpressionPlannerUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.function.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.SetTransform;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.vector.types.pojo.Schema;

public class AggregateInfoGenerator implements UnaryExecutorFactory<AggregateUnaryExecutor> {

  private final SetTransform transform;

  public AggregateInfoGenerator(SetTransform transform) {
    this.transform = Objects.requireNonNull(transform);
  }

  @Override
  public AggregateUnaryExecutor initialize(ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {

    List<ExpressionAccumulation> accumulations =
        generateAggregateInfo(context, inputSchema, transform.getFunctionCallList());

    List<ExpressionAccumulator> accumulators = new ArrayList<>();
    for (ExpressionAccumulation accumulation : accumulations) {
      accumulators.add(accumulation.accumulate(context.getAllocator(), inputSchema.raw()));
    }
    return new AggregateUnaryExecutor(context, inputSchema.raw(), accumulators);
  }

  public static List<ExpressionAccumulation> generateAggregateInfo(
      ExecutorContext context, BatchSchema inputSchema, List<FunctionCall> calls)
      throws ComputeException {
    List<ExpressionAccumulation> result = new ArrayList<>();
    for (FunctionCall call : calls) {
      List<ExpressionAccumulation> accumulations =
          generateAggregateInfo(context, inputSchema, call);
      result.addAll(accumulations);
    }
    return result;
  }

  private static List<ExpressionAccumulation> generateAggregateInfo(
      ExecutorContext context, BatchSchema inputSchema, FunctionCall call) throws ComputeException {
    Function function = call.getFunction();
    if (function.getFunctionType() != FunctionType.System) {
      throw new UnsupportedOperationException("Not implemented yet");
    }
    FunctionParams params = call.getParams();
    if (params.getPaths().size() != 1) {
      throw new ComputeException("Function " + function + " should have exactly one parameter");
    }
    String path = params.getPaths().get(0);
    if (function.getMappingType() != MappingType.SetMapping) {
      throw new ComputeException("Function " + function + " is not an aggregate function");
    }
    UnaryAccumulation accumulation = getAccumulation(context, (SetMappingFunction) function);
    if (params.isDistinct()) {
      accumulation = new UnaryAccumulationDistinctAdapter(accumulation);
    }
    List<ScalarExpression<?>> scalarExpressions = new ArrayList<>();
    if (!call.isNeedPreRowTransform()) {
      List<Integer> matchedIndexes = Schemas.matchPatternIgnoreKey(inputSchema.raw(), path);
      for (int index : matchedIndexes) {
        scalarExpressions.add(new FieldNode(index));
      }
    } else {
      List<ScalarExpression<?>> preRowTransform = new ArrayList<>();
      for (Expression expression : params.getExpressions()) {
        preRowTransform.add(
            PhysicalExpressionPlannerUtils.getPhysicalExpression(
                context, inputSchema.raw(), expression, true));
      }
      Schema schema =
          ScalarExpressionUtils.getOutputSchema(
              context.getAllocator(), preRowTransform, inputSchema.raw());
      List<Integer> matchedIndexes = Schemas.matchPatternIgnoreKey(schema, path);
      for (int index : matchedIndexes) {
        scalarExpressions.add(preRowTransform.get(index));
      }
    }
    List<ExpressionAccumulation> result = new ArrayList<>();
    for (ScalarExpression<?> scalarExpression : scalarExpressions) {
      result.add(
          new ExpressionAccumulation(accumulation, Collections.singletonList(scalarExpression)));
    }
    return result;
  }

  private static UnaryAccumulation getAccumulation(
      ExecutorContext context, SetMappingFunction function) {
    switch (function.getIdentifier()) {
      case PhysicalCount.NAME:
        return new PhysicalCount();
      case PhysicalSum.NAME:
        return new PhysicalSum();
      case PhysicalAvg.NAME:
        return new PhysicalAvg();
      case PhysicalMax.NAME:
        return new PhysicalMax();
      case PhysicalMin.NAME:
        return new PhysicalMin();
      case PhysicalFirst.NAME:
        return new PhysicalFirst();
      case PhysicalLast.NAME:
        return new PhysicalLast();
      default:
        throw new UnsupportedOperationException(
            "Unsupported function: " + function.getIdentifier());
    }
  }
}
