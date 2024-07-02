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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import static cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils.isCanUseSetQuantifierFunction;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.UnexpectedOperatorException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutor;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.SetMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Max;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Min;
import cn.edu.tsinghua.iginx.engine.shared.operator.AddSchemaPrefix;
import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.CrossJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Distinct;
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
import cn.edu.tsinghua.iginx.engine.shared.source.EmptySource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamOperatorMemoryExecutor implements OperatorMemoryExecutor {

  private StreamOperatorMemoryExecutor() {}

  public static StreamOperatorMemoryExecutor getInstance() {
    return StreamOperatorMemoryExecutor.StreamOperatorMemoryExecutorHolder.INSTANCE;
  }

  @Override
  public RowStream executeUnaryOperator(
      UnaryOperator operator, RowStream stream, RequestContext context) throws PhysicalException {
    RowStream result = null;
    switch (operator.getType()) {
      case Project:
        result = executeProject((Project) operator, stream);
        break;
      case Select:
        result = executeSelect((Select) operator, stream);
        break;
      case Sort:
        result = executeSort((Sort) operator, stream);
        break;
      case Limit:
        result = executeLimit((Limit) operator, stream);
        break;
      case Downsample:
        result = executeDownsample((Downsample) operator, stream);
        break;
      case RowTransform:
        result = executeRowTransform((RowTransform) operator, stream);
        break;
      case SetTransform:
        result = executeSetTransform((SetTransform) operator, stream);
        break;
      case MappingTransform:
        result = executeMappingTransform((MappingTransform) operator, stream);
        break;
      case Rename:
        result = executeRename((Rename) operator, stream);
        break;
      case Reorder:
        result = executeReorder((Reorder) operator, stream);
        break;
      case AddSchemaPrefix:
        result = executeAddSchemaPrefix((AddSchemaPrefix) operator, stream);
        break;
      case GroupBy:
        result = executeGroupBy((GroupBy) operator, stream);
        break;
      case Distinct:
        result = executeDistinct((Distinct) operator, stream);
        break;
      case ValueToSelectedPath:
        result = executeValueToSelectedPath((ValueToSelectedPath) operator, stream);
        break;
      default:
        throw new UnexpectedOperatorException("unknown unary operator: " + operator.getType());
    }
    result.setContext(context);
    return result;
  }

  @Override
  public RowStream executeBinaryOperator(
      BinaryOperator operator, RowStream streamA, RowStream streamB, RequestContext context)
      throws PhysicalException {
    RowStream result = null;
    switch (operator.getType()) {
      case Join:
        result = executeJoin((Join) operator, streamA, streamB);
        break;
      case CrossJoin:
        result = executeCrossJoin((CrossJoin) operator, streamA, streamB);
        break;
      case InnerJoin:
        result = executeInnerJoin((InnerJoin) operator, streamA, streamB);
        break;
      case OuterJoin:
        result = executeOuterJoin((OuterJoin) operator, streamA, streamB);
        break;
      case SingleJoin:
        result = executeSingleJoin((SingleJoin) operator, streamA, streamB);
        break;
      case MarkJoin:
        result = executeMarkJoin((MarkJoin) operator, streamA, streamB);
        break;
      case PathUnion:
        result = executePathUnion((PathUnion) operator, streamA, streamB);
        break;
      case Union:
        result = executeUnion((Union) operator, streamA, streamB);
        break;
      case Except:
        result = executeExcept((Except) operator, streamA, streamB);
        break;
      case Intersect:
        result = executeIntersect((Intersect) operator, streamA, streamB);
        break;
      default:
        throw new UnexpectedOperatorException("unknown binary operator: " + operator.getType());
    }
    result.setContext(context);
    return result;
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
    List<FunctionCall> functionCallList = setTransform.getFunctionCallList();
    Map<List<String>, RowStream> distinctStreamMap = new HashMap<>();
    distinctStreamMap.put(null, stream);

    // 如果有distinct,构造不同function对应的stream的Map
    for (FunctionCall functionCall : functionCallList) {
      FunctionParams params = functionCall.getParams();
      SetMappingFunction function = (SetMappingFunction) functionCall.getFunction();
      if (params.isDistinct()) {
        if (!isCanUseSetQuantifierFunction(function.getIdentifier())) {
          throw new IllegalArgumentException(
              "function " + function.getIdentifier() + " can't use DISTINCT");
        }
        // min和max无需去重
        if (!function.getIdentifier().equals(Max.MAX)
            && !function.getIdentifier().equals(Min.MIN)) {
          Distinct distinct = new Distinct(EmptySource.EMPTY_SOURCE, params.getPaths());
          distinctStreamMap.put(params.getPaths(), executeDistinct(distinct, stream));
        }
      }
    }

    return new SetTransformLazyStream(setTransform, distinctStreamMap);
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

  private RowStream executeDistinct(Distinct distinct, RowStream stream) {
    Project project = new Project(EmptySource.EMPTY_SOURCE, distinct.getPatterns(), null);
    stream = executeProject(project, stream);
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
}
