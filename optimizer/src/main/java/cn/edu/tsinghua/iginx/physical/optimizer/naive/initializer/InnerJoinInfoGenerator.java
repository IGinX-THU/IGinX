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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join.JoinType;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.convert.cast.CastAsFloat8;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.CallNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.BinaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful.HashJoinExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful.StatefulBinaryExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import org.apache.arrow.vector.types.Types;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class InnerJoinInfoGenerator implements BinaryExecutorFactory<StatefulBinaryExecutor> {

  private final InnerJoin operator;

  public InnerJoinInfoGenerator(InnerJoin operator) {
    this.operator = Objects.requireNonNull(operator);
  }

  @Override
  public StatefulBinaryExecutor initialize(
      ExecutorContext context, BatchSchema leftSchema, BatchSchema rightSchema)
      throws ComputeException {

    if (operator.isNaturalJoin()) {
      throw new IllegalStateException("Natural join is not implemented yet");
    }

    if (!operator.getExtraJoinPrefix().isEmpty()) {
      throw new IllegalStateException("Extra join prefix is not implemented yet");
    }

    if (!operator.getJoinColumns().isEmpty()) {
      throw new IllegalStateException("Join columns is not implemented yet");
    }

    List<Pair<ScalarExpression<?>, ScalarExpression<?>>> on = new ArrayList<>();
    for (Pair<String, String> joinPath : getHashJoinPath()) {
      on.add(getHashJoinPath(joinPath, leftSchema, rightSchema));
    }

    int leftFieldStartIndex = leftSchema.hasKey() ? 1 : 0;
    int rightFieldStartIndex = rightSchema.hasKey() ? 1 : 0;
    List<ScalarExpression<?>> leftOutputExpressions = IntStream.range(leftFieldStartIndex, leftSchema.raw().getFields().size())
        .mapToObj(FieldNode::new)
        .collect(Collectors.toList());
    List<ScalarExpression<?>> rightOutputExpressions = IntStream.range(rightFieldStartIndex, rightSchema.raw().getFields().size())
        .mapToObj(FieldNode::new)
        .collect(Collectors.toList());

    return new HashJoinExecutor(context, leftSchema, rightSchema, JoinType.INNER, leftOutputExpressions, rightOutputExpressions, on);
  }

  private Pair<ScalarExpression<?>, ScalarExpression<?>> getHashJoinPath(Pair<String, String> joinPath, BatchSchema leftSchema, BatchSchema rightSchema) throws ComputeException {
    if (joinPath.getLeft().startsWith(operator.getPrefixA()) && joinPath.getRight().startsWith(operator.getPrefixB())) {
      List<Integer> leftIndices = Schemas.matchPattern(leftSchema.raw(), joinPath.getLeft());
      List<Integer> rightIndices = Schemas.matchPattern(rightSchema.raw(), joinPath.getRight());
      if (leftIndices.isEmpty() || rightIndices.isEmpty()) {
        throw new ComputeException("Join path is not found: " + joinPath + " in " + leftSchema + " and " + rightSchema);
      }
      if (leftIndices.size() > 1 || rightIndices.size() > 1) {
        throw new ComputeException("Join path is ambiguous: " + joinPath + " in " + leftSchema + " and " + rightSchema);
      }
      return getHashJoinOn(Pair.of(leftIndices.get(0), rightIndices.get(0)), leftSchema, rightSchema);
    } else if (joinPath.getLeft().startsWith(operator.getPrefixB()) && joinPath.getRight().startsWith(operator.getPrefixA())) {
      List<Integer> leftIndices = Schemas.matchPattern(leftSchema.raw(), joinPath.getRight());
      List<Integer> rightIndices = Schemas.matchPattern(rightSchema.raw(), joinPath.getLeft());
      if (rightIndices.isEmpty() || leftIndices.isEmpty()) {
        throw new ComputeException("Join path is not found: " + joinPath + " in " + rightSchema + " and " + leftSchema);
      }
      if (rightIndices.size() > 1 || leftIndices.size() > 1) {
        throw new ComputeException("Join path is ambiguous: " + joinPath + " in " + rightSchema + " and " + leftSchema);
      }
      return getHashJoinOn(Pair.of(rightIndices.get(0), leftIndices.get(0)), rightSchema, leftSchema);
    } else {
      throw new ComputeException("Join path is not found: " + joinPath + " in " + operator.getInfo());
    }
  }

  private Pair<ScalarExpression<?>, ScalarExpression<?>> getHashJoinOn(
      Pair<Integer, Integer> joinPath, BatchSchema leftSchema, BatchSchema rightSchema) throws ComputeException {
    Types.MinorType leftType = Types.getMinorTypeForArrowType(leftSchema.raw().getFields().get(joinPath.getLeft()).getType());
    Types.MinorType rightType = Types.getMinorTypeForArrowType(rightSchema.raw().getFields().get(joinPath.getRight()).getType());

    if (leftType != rightType) {
      if (Schemas.isNumeric(leftType) && Schemas.isNumeric(rightType)) {
        return Pair.of(
            new CallNode<>(new CastAsFloat8(), new FieldNode(joinPath.getLeft())),
            new CallNode<>(new CastAsFloat8(), new FieldNode(joinPath.getRight())));
      } else {
        throw new ComputeException("Join type " + leftType + " and " + rightType + " of " + joinPath + " with " + leftSchema + " and " + rightSchema + " is not supported");
      }
    } else {
      return Pair.of(new FieldNode(joinPath.getLeft()), new FieldNode(joinPath.getRight()));
    }
  }

  private List<Pair<String, String>> getHashJoinPath() {
    if (operator.getFilter() == null) {
      return Collections.emptyList();
    }
    List<Pair<String, String>> joinPaths = new ArrayList<>();
    operator.getFilter().accept(
        new FilterVisitor() {
          @Override
          public void visit(AndFilter filter) {
          }

          @Override
          public void visit(OrFilter filter) {
          }

          @Override
          public void visit(NotFilter filter) {
          }

          @Override
          public void visit(KeyFilter filter) {
          }

          @Override
          public void visit(ValueFilter filter) {
          }

          @Override
          public void visit(PathFilter filter) {
            joinPaths.add(Pair.of(filter.getPathA(), filter.getPathB()));
          }

          @Override
          public void visit(BoolFilter filter) {
          }

          @Override
          public void visit(ExprFilter filter) {
          }
        });
    return joinPaths;
  }
}
