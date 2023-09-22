/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import static cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils.isCanUseSetQuantifierFunction;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.UnexpectedOperatorException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutor;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.SetMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Max;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Min;
import cn.edu.tsinghua.iginx.engine.shared.operator.AddSchemaPrefix;
import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.CrossJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Downsample;
import cn.edu.tsinghua.iginx.engine.shared.operator.Except;
import cn.edu.tsinghua.iginx.engine.shared.operator.GroupBy;
import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Intersect;
import cn.edu.tsinghua.iginx.engine.shared.operator.Join;
import cn.edu.tsinghua.iginx.engine.shared.operator.Limit;
import cn.edu.tsinghua.iginx.engine.shared.operator.MappingTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.MarkJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.OuterJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.PathUnion;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Rename;
import cn.edu.tsinghua.iginx.engine.shared.operator.Reorder;
import cn.edu.tsinghua.iginx.engine.shared.operator.RowTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.SetTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.SingleJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Sort;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Union;
import cn.edu.tsinghua.iginx.engine.shared.operator.ValueToSelectedPath;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;

public class StreamOperatorMemoryExecutor implements OperatorMemoryExecutor {

  private StreamOperatorMemoryExecutor() {}

  public static StreamOperatorMemoryExecutor getInstance() {
    return StreamOperatorMemoryExecutor.StreamOperatorMemoryExecutorHolder.INSTANCE;
  }

  @Override
  public RowStream executeUnaryOperator(UnaryOperator operator, RowStream stream)
      throws PhysicalException {
    switch (operator.getType()) {
      case Project:
        return executeProject((Project) operator, stream);
      case Select:
        return executeSelect((Select) operator, stream);
      case Sort:
        return executeSort((Sort) operator, stream);
      case Limit:
        return executeLimit((Limit) operator, stream);
      case Downsample:
        return executeDownsample((Downsample) operator, stream);
      case RowTransform:
        return executeRowTransform((RowTransform) operator, stream);
      case SetTransform:
        return executeSetTransform((SetTransform) operator, stream);
      case MappingTransform:
        return executeMappingTransform((MappingTransform) operator, stream);
      case Rename:
        return executeRename((Rename) operator, stream);
      case Reorder:
        return executeReorder((Reorder) operator, stream);
      case AddSchemaPrefix:
        return executeAddSchemaPrefix((AddSchemaPrefix) operator, stream);
      case GroupBy:
        return executeGroupBy((GroupBy) operator, stream);
      case Distinct:
        return executeDistinct(stream);
      case ValueToSelectedPath:
        return executeValueToSelectedPath((ValueToSelectedPath) operator, stream);
      default:
        throw new UnexpectedOperatorException("unknown unary operator: " + operator.getType());
    }
  }

  @Override
  public RowStream executeBinaryOperator(
      BinaryOperator operator, RowStream streamA, RowStream streamB) throws PhysicalException {
    switch (operator.getType()) {
      case Join:
        return executeJoin((Join) operator, streamA, streamB);
      case CrossJoin:
        return executeCrossJoin((CrossJoin) operator, streamA, streamB);
      case InnerJoin:
        return executeInnerJoin((InnerJoin) operator, streamA, streamB);
      case OuterJoin:
        return executeOuterJoin((OuterJoin) operator, streamA, streamB);
      case SingleJoin:
        return executeSingleJoin((SingleJoin) operator, streamA, streamB);
      case MarkJoin:
        return executeMarkJoin((MarkJoin) operator, streamA, streamB);
      case PathUnion:
        return executePathUnion((PathUnion) operator, streamA, streamB);
      case Union:
        return executeUnion((Union) operator, streamA, streamB);
      case Except:
        return executeExcept((Except) operator, streamA, streamB);
      case Intersect:
        return executeIntersect((Intersect) operator, streamA, streamB);
      default:
        throw new UnexpectedOperatorException("unknown binary operator: " + operator.getType());
    }
  }

  private RowStream executeProject(Project project, RowStream stream) {
    return new ProjectLazyStream(project, stream);
  }

  private RowStream executeSelect(Select select, RowStream stream) {
    return new SelectLazyStream(select, stream);
  }

  private RowStream executeSort(Sort sort, RowStream stream) throws PhysicalException {
    return new SortLazyStream(sort, stream);
  }

  private RowStream executeLimit(Limit limit, RowStream stream) {
    return new LimitLazyStream(limit, stream);
  }

  private RowStream executeDownsample(Downsample downsample, RowStream stream)
      throws PhysicalException {
    if (!stream.getHeader().hasKey()) {
      throw new InvalidOperatorParameterException(
          "downsample operator is not support for row stream without timestamps.");
    }
    return new DownsampleLazyStream(downsample, stream);
  }

  private RowStream executeRowTransform(RowTransform rowTransform, RowStream stream) {
    return new RowTransformLazyStream(rowTransform, stream);
  }

  private RowStream executeSetTransform(SetTransform setTransform, RowStream stream) {
    SetMappingFunction function = (SetMappingFunction) setTransform.getFunctionCall().getFunction();
    FunctionParams params = setTransform.getFunctionCall().getParams();
    if (params.isDistinct()) {
      if (!isCanUseSetQuantifierFunction(function.getIdentifier())) {
        throw new IllegalArgumentException(
            "function " + function.getIdentifier() + " can't use DISTINCT");
      }
      // min和max无需去重
      if (!function.getIdentifier().equals(Max.MAX) && !function.getIdentifier().equals(Min.MIN)) {
        stream = executeDistinct(stream);
      }
    }

    return new SetTransformLazyStream(setTransform, stream);
  }

  private RowStream executeMappingTransform(MappingTransform mappingTransform, RowStream stream) {
    return new MappingTransformLazyStream(mappingTransform, stream);
  }

  private RowStream executeRename(Rename rename, RowStream stream) {
    return new RenameLazyStream(rename, stream);
  }

  private RowStream executeReorder(Reorder reorder, RowStream stream) {
    return new ReorderLazyStream(reorder, stream);
  }

  private RowStream executeAddSchemaPrefix(AddSchemaPrefix addSchemaPrefix, RowStream stream) {
    return new AddSchemaPrefixLazyStream(addSchemaPrefix, stream);
  }

  private RowStream executeGroupBy(GroupBy groupBy, RowStream stream) {
    return new GroupByLazyStream(groupBy, stream);
  }

  private RowStream executeDistinct(RowStream stream) {
    return new DistinctLazyStream(stream);
  }

  private RowStream executeValueToSelectedPath(ValueToSelectedPath operator, RowStream stream) {
    return new ValueToSelectedPathLazyStream(operator, stream);
  }

  private RowStream executeJoin(Join join, RowStream streamA, RowStream streamB)
      throws PhysicalException {
    if (!join.getJoinBy().equals(Constants.KEY) && !join.getJoinBy().equals(Constants.ORDINAL)) {
      throw new InvalidOperatorParameterException(
          "join operator is not support for field "
              + join.getJoinBy()
              + " except for "
              + Constants.KEY
              + " and "
              + Constants.ORDINAL);
    }
    return new JoinLazyStream(join, streamA, streamB);
  }

  private RowStream executeCrossJoin(CrossJoin crossJoin, RowStream streamA, RowStream streamB)
      throws PhysicalException {
    return new CrossJoinLazyStream(crossJoin, streamA, streamB);
  }

  private RowStream executeInnerJoin(InnerJoin innerJoin, RowStream streamA, RowStream streamB)
      throws PhysicalException {
    switch (innerJoin.getJoinAlgType()) {
      case NestedLoopJoin:
        return executeNestedLoopInnerJoin(innerJoin, streamA, streamB);
      case HashJoin:
        return executeHashInnerJoin(innerJoin, streamA, streamB);
      case SortedMergeJoin:
        return executeSortedMergeInnerJoin(innerJoin, streamA, streamB);
      default:
        throw new PhysicalException("Unknown join algorithm type: " + innerJoin.getJoinAlgType());
    }
  }

  private RowStream executeNestedLoopInnerJoin(
      InnerJoin innerJoin, RowStream streamA, RowStream streamB) throws PhysicalException {
    return new NestedLoopInnerJoinLazyStream(innerJoin, streamA, streamB);
  }

  private RowStream executeHashInnerJoin(InnerJoin innerJoin, RowStream streamA, RowStream streamB)
      throws PhysicalException {
    return new HashInnerJoinLazyStream(innerJoin, streamA, streamB);
  }

  private RowStream executeSortedMergeInnerJoin(
      InnerJoin innerJoin, RowStream streamA, RowStream streamB) throws PhysicalException {
    return new SortedMergeInnerJoinLazyStream(innerJoin, streamA, streamB);
  }

  private RowStream executeOuterJoin(OuterJoin outerJoin, RowStream streamA, RowStream streamB)
      throws PhysicalException {
    switch (outerJoin.getJoinAlgType()) {
      case NestedLoopJoin:
        return executeNestedLoopOuterJoin(outerJoin, streamA, streamB);
      case HashJoin:
        return executeHashOuterJoin(outerJoin, streamA, streamB);
      case SortedMergeJoin:
        return executeSortedMergeOuterJoin(outerJoin, streamA, streamB);
      default:
        throw new PhysicalException("Unknown join algorithm type: " + outerJoin.getJoinAlgType());
    }
  }

  private RowStream executeNestedLoopOuterJoin(
      OuterJoin outerJoin, RowStream streamA, RowStream streamB) throws PhysicalException {
    return new NestedLoopOuterJoinLazyStream(outerJoin, streamA, streamB);
  }

  private RowStream executeHashOuterJoin(OuterJoin outerJoin, RowStream streamA, RowStream streamB)
      throws PhysicalException {
    return new HashOuterJoinLazyStream(outerJoin, streamA, streamB);
  }

  private RowStream executeSortedMergeOuterJoin(
      OuterJoin outerJoin, RowStream streamA, RowStream streamB) throws PhysicalException {
    return new SortedMergeOuterJoinLazyStream(outerJoin, streamA, streamB);
  }

  private RowStream executeSingleJoin(SingleJoin singleJoin, RowStream streamA, RowStream streamB)
      throws PhysicalException {
    switch (singleJoin.getJoinAlgType()) {
      case NestedLoopJoin:
        return executeNestedLoopMarkJoin(singleJoin, streamA, streamB);
      case HashJoin:
        return executeHashMarkJoin(singleJoin, streamA, streamB);
      default:
        throw new PhysicalException(
            "Unsupported single join algorithm type: " + singleJoin.getJoinAlgType());
    }
  }

  private RowStream executeNestedLoopMarkJoin(
      SingleJoin singleJoin, RowStream streamA, RowStream streamB) {
    return new NestedLoopSingleJoinLazyStream(singleJoin, streamA, streamB);
  }

  private RowStream executeHashMarkJoin(
      SingleJoin singleJoin, RowStream streamA, RowStream streamB) {
    return new HashSingleJoinLazyStream(singleJoin, streamA, streamB);
  }

  private RowStream executeMarkJoin(MarkJoin markJoin, RowStream streamA, RowStream streamB)
      throws PhysicalException {
    switch (markJoin.getJoinAlgType()) {
      case NestedLoopJoin:
        return executeNestedLoopMarkJoin(markJoin, streamA, streamB);
      case HashJoin:
        return executeHashMarkJoin(markJoin, streamA, streamB);
      default:
        throw new PhysicalException(
            "Unsupported mark join algorithm type: " + markJoin.getJoinAlgType());
    }
  }

  private RowStream executeNestedLoopMarkJoin(
      MarkJoin markJoin, RowStream streamA, RowStream streamB) {
    return new NestedLoopMarkJoinLazyStream(markJoin, streamA, streamB);
  }

  private RowStream executeHashMarkJoin(MarkJoin markJoin, RowStream streamA, RowStream streamB) {
    return new HashMarkJoinLazyStream(markJoin, streamA, streamB);
  }

  private RowStream executePathUnion(PathUnion union, RowStream streamA, RowStream streamB) {
    return new PathUnionLazyStream(union, streamA, streamB);
  }

  private RowStream executeUnion(Union union, RowStream streamA, RowStream streamB) {
    Reorder reorderA = new Reorder(EmptySource.EMPTY_SOURCE, union.getLeftOrder());
    Reorder reorderB = new Reorder(EmptySource.EMPTY_SOURCE, union.getRightOrder());
    streamA = executeReorder(reorderA, streamA);
    streamB = executeReorder(reorderB, streamB);

    if (union.isDistinct()) {
      return new UnionDistinctLazyStream(streamA, streamB);
    } else {
      return new UnionAllLazyStream(streamA, streamB);
    }
  }

  private RowStream executeExcept(Except except, RowStream streamA, RowStream streamB) {
    Reorder reorderA = new Reorder(EmptySource.EMPTY_SOURCE, except.getLeftOrder());
    Reorder reorderB = new Reorder(EmptySource.EMPTY_SOURCE, except.getRightOrder());
    streamA = executeReorder(reorderA, streamA);
    streamB = executeReorder(reorderB, streamB);

    return new ExceptLazyStream(except, streamA, streamB);
  }

  private RowStream executeIntersect(Intersect intersect, RowStream streamA, RowStream streamB) {
    Reorder reorderA = new Reorder(EmptySource.EMPTY_SOURCE, intersect.getLeftOrder());
    Reorder reorderB = new Reorder(EmptySource.EMPTY_SOURCE, intersect.getRightOrder());
    streamA = executeReorder(reorderA, streamA);
    streamB = executeReorder(reorderB, streamB);

    return new IntersectLazyStream(intersect, streamA, streamB);
  }

  private static class StreamOperatorMemoryExecutorHolder {

    private static final StreamOperatorMemoryExecutor INSTANCE = new StreamOperatorMemoryExecutor();

    private StreamOperatorMemoryExecutorHolder() {}
  }

  private static class EmptySource implements Source {

    public static final EmptySource EMPTY_SOURCE = new EmptySource();

    @Override
    public SourceType getType() {
      return null;
    }

    @Override
    public Source copy() {
      return null;
    }
  }
}
