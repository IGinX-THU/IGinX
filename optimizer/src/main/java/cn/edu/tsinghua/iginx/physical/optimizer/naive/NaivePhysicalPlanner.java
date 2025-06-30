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
package cn.edu.tsinghua.iginx.physical.optimizer.naive;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalJoinUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateful.AddSequenceExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateful.LimitUnaryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateful.RemoveNullColumnExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless.ValueToSelectedPathExecutor;
import cn.edu.tsinghua.iginx.engine.physical.task.GlobalPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskType;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.*;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.parallel.FetchAsyncMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.parallel.ParallelPipelineMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.row.ArrowToRowUnaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.row.BinaryRowMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.row.RowToArrowUnaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.row.UnaryRowMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.utils.PhysicalCloseable;
import cn.edu.tsinghua.iginx.engine.physical.utils.UDFDetector;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStreams;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.system.ArithmeticExpr;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.*;
import cn.edu.tsinghua.iginx.physical.optimizer.naive.initializer.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.arrow.util.Preconditions;

public class NaivePhysicalPlanner {

  public PhysicalTask<?> construct(Operator operator, RequestContext context) {
    switch (operator.getType()) {
      case CombineNonQuery:
        return construct((CombineNonQuery) operator, context);
      case Delete:
        return construct((Delete) operator, context);
      case Insert:
        return construct((Insert) operator, context);
      case Project:
        return construct((Project) operator, context);
      case Rename:
        return construct((Rename) operator, context);
      case Reorder:
        return construct((Reorder) operator, context);
      case AddSchemaPrefix:
        return construct((AddSchemaPrefix) operator, context);
      case RowTransform:
        return construct((RowTransform) operator, context);
      case Select:
        return construct((Select) operator, context);
      case Sort:
        return construct((Sort) operator, context);
      case SetTransform:
        return construct((SetTransform) operator, context);
      case GroupBy:
        return construct((GroupBy) operator, context);
      case Limit:
        return construct((Limit) operator, context);
      case InnerJoin:
        return construct((InnerJoin) operator, context);
      case OuterJoin:
        return construct((OuterJoin) operator, context);
      case SingleJoin:
        return construct((SingleJoin) operator, context);
      case MarkJoin:
        return construct((MarkJoin) operator, context);
      case CrossJoin:
        return construct((CrossJoin) operator, context);
      case Folded:
        return construct((FoldedOperator) operator, context);
      case ShowColumns:
        return construct((ShowColumns) operator, context);
      case Migration:
        return construct((Migration) operator, context);
      case Join:
        return construct((Join) operator, context);
      case PathUnion:
        return construct((PathUnion) operator, context);
      case Union:
        return construct((Union) operator, context);
      case Except:
        return construct((Except) operator, context);
      case Intersect:
        return construct((Intersect) operator, context);
      case Downsample:
        return construct((Downsample) operator, context);
      case MappingTransform:
        return construct((MappingTransform) operator, context);
      case Distinct:
        return construct((Distinct) operator, context);
      case ValueToSelectedPath:
        return construct((ValueToSelectedPath) operator, context);
      case AddSequence:
        return construct((AddSequence) operator, context);
      case RemoveNullColumn:
        return construct((RemoveNullColumn) operator, context);
      case Unknown:
      case ProjectWaitingForPath:
      case Multiple:
      case Binary:
      case Unary:
      default:
        throw new UnsupportedOperationException("Unsupported operator type: " + operator.getType());
    }
  }

  public PhysicalTask<?> fetch(Source source, RequestContext context) {
    switch (source.getType()) {
      case Fragment:
        FragmentSource fragmentSource = (FragmentSource) source;
        return new StoragePhysicalTask(
            new ArrayList<>(), fragmentSource.getFragment(), true, false, context);
      case Operator:
        OperatorSource operatorSource = (OperatorSource) source;
        return construct(operatorSource.getOperator(), context);
      case Constant:
        ConstantSource constantSource = (ConstantSource) source;
        return construct(constantSource.getExpressionList(), context);
      default:
        throw new UnsupportedOperationException("Unsupported source type: " + source.getType());
    }
  }

  public PhysicalTask<?> construct(List<Expression> constantExpressions, RequestContext context) {
    Preconditions.checkArgument(
        constantExpressions.stream()
            .allMatch(cn.edu.tsinghua.iginx.sql.utils.ExpressionUtils::isConstantArithmeticExpr));

    PhysicalTask<BatchStream> source =
        new StreamSourceMemoryPhysicalTask(
            context,
            "Produce 1 emtpy row to calculate constant values",
            () -> BatchStreams.nonColumn(context.getBatchRowCount(), 1));

    List<FunctionCall> functionCalls = new ArrayList<>();
    for (Expression expression : constantExpressions) {
      functionCalls.add(
          new FunctionCall(ArithmeticExpr.getInstance(), new FunctionParams(expression)));
    }

    RowTransform rowTransform = new RowTransform(EmptySource.EMPTY_SOURCE, functionCalls);

    if (rowTransform.getFunctionCallList().stream()
        .anyMatch(UDFDetector::containNonSystemFunction)) {
      return new UnaryRowMemoryPhysicalTask(
          convert(source, context, RowStream.class), rowTransform, context);
    }

    return new PipelineMemoryPhysicalTask(
        source,
        Collections.singletonList(rowTransform),
        context,
        new RowTransformInfoGenerator(rowTransform));
  }

  public PhysicalTask<BatchStream> fetchAsync(Source source, RequestContext context) {
    PhysicalTask<?> sourceTask = fetch(source, context);
    // TODO: 是否所有的情况都应当使用 Exchange？ 是否存在一些情况使用 FetchAll 更好？
    // return new UnarySinkMemoryPhysicalTask(
    //     convert(sourceTask, context, BatchStream.class),
    //     Collections.emptyList(),
    //     context,
    //     (ctx, schema) -> new FetchAllUnaryExecutor(ctx, schema.raw()));
    return new FetchAsyncMemoryPhysicalTask(
        convert(sourceTask, context, BatchStream.class), context);
  }

  public PhysicalTask<BatchStream> construct(CombineNonQuery operator, RequestContext context) {
    List<PhysicalTask<RowStream>> sourceTasks = new ArrayList<>();
    for (Source source : operator.getSources()) {
      sourceTasks.add(convert(fetch(source, context), context, RowStream.class));
    }

    return new CombineNonQueryPhysicalTask(
        Collections.singletonList(operator), sourceTasks, context);
  }

  public PhysicalTask<RowStream> construct(Insert operator, RequestContext context) {
    return constructWriteTask(operator, context);
  }

  public PhysicalTask<RowStream> construct(Delete operator, RequestContext context) {
    return constructWriteTask(operator, context);
  }

  private PhysicalTask<RowStream> constructWriteTask(
      UnaryOperator operator, RequestContext context) {
    PhysicalTask<?> task = fetch(operator.getSource(), context);
    if (task.getType() != TaskType.Storage) {
      throw new IllegalStateException("Unexpected task type: " + task.getType());
    }
    return reConstruct((StoragePhysicalTask) task, context, true, operator);
  }

  private StoragePhysicalTask reConstruct(
      StoragePhysicalTask storageTask,
      RequestContext context,
      boolean needBroadcasting,
      Operator... extraOperators) {
    List<Operator> newOperators = new ArrayList<>(storageTask.getOperators());
    Collections.addAll(newOperators, extraOperators);
    return new StoragePhysicalTask(
        newOperators,
        storageTask.getTargetFragment(),
        storageTask.isSync(),
        needBroadcasting,
        context);
  }

  public PhysicalTask<RowStream> constructRow(UnaryOperator operator, RequestContext context) {
    PhysicalTask<?> sourceTask = fetch(operator.getSource(), context);
    return new UnaryRowMemoryPhysicalTask(
        convert(sourceTask, context, RowStream.class), operator, context);
  }

  public PhysicalTask<RowStream> constructRow(BinaryOperator operator, RequestContext context) {
    PhysicalTask<?> leftTask = fetch(operator.getSourceA(), context);
    PhysicalTask<?> rightTask = fetch(operator.getSourceB(), context);
    return new BinaryRowMemoryPhysicalTask(
        convert(leftTask, context, RowStream.class),
        convert(rightTask, context, RowStream.class),
        operator,
        context);
  }

  public PhysicalTask<?> construct(Project operator, RequestContext context) {
    PhysicalTask<?> sourceTask = fetch(operator.getSource(), context);
    if (sourceTask.getType() == TaskType.Storage) {
      StoragePhysicalTask sourceStorageTask = (StoragePhysicalTask) sourceTask;
      if (sourceStorageTask.getOperators().isEmpty()) {
        return reConstruct(sourceStorageTask, context, false, operator);
      }
    }
    return new PipelineMemoryPhysicalTask(
        convert(sourceTask, context, BatchStream.class),
        Collections.singletonList(operator),
        context,
        new ProjectInfoGenerator(operator));
  }

  public PhysicalTask<BatchStream> construct(Rename operator, RequestContext context) {
    PhysicalTask<?> sourceTask = fetch(operator.getSource(), context);
    return new PipelineMemoryPhysicalTask(
        convert(sourceTask, context, BatchStream.class),
        Collections.singletonList(operator),
        context,
        new RenameInfoGenerator(operator));
  }

  public PhysicalTask<BatchStream> construct(Reorder operator, RequestContext context) {
    PhysicalTask<?> sourceTask = fetch(operator.getSource(), context);
    return new PipelineMemoryPhysicalTask(
        convert(sourceTask, context, BatchStream.class),
        Collections.singletonList(operator),
        context,
        new ReorderInfoGenerator(operator));
  }

  public PhysicalTask<?> construct(AddSchemaPrefix operator, RequestContext context) {
    PhysicalTask<?> sourceTask = fetch(operator.getSource(), context);
    if (operator.getSchemaPrefix() == null) {
      return sourceTask;
    }
    return new PipelineMemoryPhysicalTask(
        convert(sourceTask, context, BatchStream.class),
        Collections.singletonList(operator),
        context,
        new AddSchemaPrefixInfoGenerator(operator));
  }

  public PhysicalTask<?> construct(RowTransform operator, RequestContext context) {
    // 有 UDF 时借用旧的基于 Row 的实现
    if (operator.getFunctionCallList().stream().anyMatch(UDFDetector::containNonSystemFunction)) {
      return constructRow(operator, context);
    }

    // 有 UDF 时并行化流水线
    if (operator.getFunctionCallList().stream().anyMatch(UDFDetector::containNonSystemFunction)) {
      PhysicalTask<BatchStream> sourceTask = fetchAsync(operator.getSource(), context);

      int pipelineParallelism = ConfigDescriptor.getInstance().getConfig().getPipelineParallelism();

      return new ParallelPipelineMemoryPhysicalTask(
          sourceTask,
          context,
          (ctx, parentTask) ->
              new PipelineMemoryPhysicalTask(
                  parentTask,
                  Collections.singletonList(operator),
                  ctx,
                  new RowTransformInfoGenerator(operator)),
          pipelineParallelism);
    }

    PhysicalTask<?> sourceTask = fetch(operator.getSource(), context);
    return new PipelineMemoryPhysicalTask(
        convert(sourceTask, context, BatchStream.class),
        Collections.singletonList(operator),
        context,
        new RowTransformInfoGenerator(operator));
  }

  public PhysicalTask<?> construct(Select operator, RequestContext context) {
    Source source = operator.getSource();
    PhysicalTask<?> sourceTask = fetch(source, context);

    StoragePhysicalTask storageTask = tryPushDownAloneWithProject(sourceTask, context, operator);
    if (storageTask != null) {
      return storageTask;
    }

    if (UDFDetector.containNonSystemFunction(operator.getFilter())) {
      return constructRow(operator, context);
    }

    if (sourceTask.getResultClass() == RowStream.class) {
      return new UnaryRowMemoryPhysicalTask(
          convert(sourceTask, context, RowStream.class), operator, context);
    }

    PhysicalTask<BatchStream> batchTask = convert(sourceTask, context, BatchStream.class);

    if (operator.getTagFilter() != null) {
      batchTask =
          new PipelineMemoryPhysicalTask(
              batchTask,
              Collections.singletonList(new Select(source, null, operator.getTagFilter())),
              context,
              new TagKVInfoGenerator(operator.getTagFilter()));
    }

    if (operator.getFilter() != null) {
      batchTask =
          new PipelineMemoryPhysicalTask(
              batchTask,
              Collections.singletonList(new Select(source, operator.getFilter(), null)),
              context,
              new FilterInfoGenerator(operator.getFilter()));
    }

    return batchTask;
  }

  @Nullable
  private StoragePhysicalTask tryPushDownAloneWithProject(
      PhysicalTask<?> sourceTask, RequestContext context, Operator operator) {

    if (!ConfigDescriptor.getInstance().getConfig().isEnablePushDown()) {
      return null;
    }

    if (sourceTask.getType() != TaskType.Storage) {
      return null;
    }
    StoragePhysicalTask storageTask = (StoragePhysicalTask) sourceTask;
    List<Operator> childOperators = sourceTask.getOperators();

    switch (childOperators.size()) {
      case 2:
        Operator secondSourceOperator = childOperators.get(1);
        if (secondSourceOperator.getType() != OperatorType.Select) {
          return null;
        }
        if (operator.getType() == OperatorType.Select) {
          return null;
        }
      case 1:
        Operator firstSourceOperator = childOperators.get(0);
        if (firstSourceOperator.getType() != OperatorType.Project) {
          return null;
        }
        Project project = (Project) firstSourceOperator;
        if (project.getTagFilter() != null) {
          return null;
        }
        break;
      default:
        return null;
    }

    return reConstruct(storageTask, context, false, operator);
  }

  public PhysicalTask<?> construct(SetTransform operator, RequestContext context) {
    PhysicalTask<?> sourceTask = fetch(operator.getSource(), context);

    StoragePhysicalTask storageTask = tryPushDownAloneWithProject(sourceTask, context, operator);
    if (storageTask != null) {
      return storageTask;
    }

    if (operator.getFunctionCallList().stream().anyMatch(UDFDetector::containNonSystemFunction)) {
      return constructRow(operator, context);
    }

    return new UnarySinkMemoryPhysicalTask(
        convert(sourceTask, context, BatchStream.class),
        Collections.singletonList(operator),
        context,
        new AggregateInfoGenerator(operator));
  }

  public PhysicalTask<?> construct(GroupBy operator, RequestContext context) {
    PhysicalTask<?> sourceTask = fetch(operator.getSource(), context);

    StoragePhysicalTask storageTask = tryPushDownAloneWithProject(sourceTask, context, operator);
    if (storageTask != null) {
      return storageTask;
    }

    if (operator.getFunctionCallList().stream().anyMatch(UDFDetector::containNonSystemFunction)) {
      return constructRow(operator, context);
    }
    if (operator.getGroupByExpressions().stream().anyMatch(UDFDetector::containNonSystemFunction)) {
      return constructRow(operator, context);
    }

    if (sourceTask.getResultClass() == RowStream.class) {
      return new UnaryRowMemoryPhysicalTask(
          convert(sourceTask, context, RowStream.class), operator, context);
    }
    PhysicalTask<BatchStream> batchTask = convert(sourceTask, context, BatchStream.class);

    if (!operator.getGroupByExpressions().isEmpty()) {
      batchTask =
          new PipelineMemoryPhysicalTask(
              batchTask,
              Collections.singletonList(operator),
              context,
              new AppendExpressionInfoGenerator(operator.getGroupByExpressions()));
    }

    return new UnarySinkMemoryPhysicalTask(
        batchTask,
        Collections.singletonList(operator),
        context,
        new GroupsAggregateInfoGenerator(operator));
  }

  public PhysicalTask<?> construct(Sort operator, RequestContext context) {
    //    PhysicalTask<?> sourceTask = fetch(operator.getSource(), context);
    //      sourceTask = new PipelineMemoryPhysicalTask(
    //          convert(sourceTask, context, BatchStream.class),
    //          Collections.singletonList(operator),
    //          context,
    //          new InnerBatchSortInfoGenerator(operator));

    return constructRow(operator, context);
  }

  public PhysicalTask<BatchStream> construct(Limit operator, RequestContext context) {
    PhysicalTask<?> sourceTask = fetch(operator.getSource(), context);

    return new UnarySinkMemoryPhysicalTask(
        convert(sourceTask, context, BatchStream.class),
        Collections.singletonList(operator),
        context,
        (ctx, schema) ->
            new LimitUnaryExecutor(ctx, schema.raw(), operator.getOffset(), operator.getLimit()));
  }

  public PhysicalTask<?> construct(InnerJoin operator, RequestContext context) {
    if ((operator.getJoinAlgType() != JoinAlgType.HashJoin
            && operator.getJoinAlgType() != JoinAlgType.NestedLoopJoin)
        || operator.isJoinByKey()
        || operator.isNaturalJoin()) {
      return constructRow(operator, context);
    }

    if (UDFDetector.containNonSystemFunction(operator.getFilter())) {
      return constructRow(operator, context);
    }

    // NOTE: The order of left and right task is reversed in InnerJoin
    // 这里以及后面交换了左右两个表的顺序，原因是在之前基于行的实现中，右表是BuildSide，左表是ProbeSide
    // 现在基于列的实现中，左表是BuildSide，右表是ProbeSide
    operator = LogicalJoinUtils.reverse(operator);

    PhysicalTask<?> leftTask = fetch(operator.getSourceA(), context);
    PhysicalTask<?> rightTask = fetchAsync(operator.getSourceB(), context);

    return new BinarySinkMemoryPhysicalTask(
        convert(leftTask, context, BatchStream.class),
        convert(rightTask, context, BatchStream.class),
        operator,
        context,
        new InnerJoinInfoGenerator(operator));
  }

  public PhysicalTask<?> construct(OuterJoin operator, RequestContext context) {
    if ((operator.getJoinAlgType() != JoinAlgType.HashJoin
            && operator.getJoinAlgType() != JoinAlgType.NestedLoopJoin)
        || operator.isJoinByKey()
        || operator.isNaturalJoin()) {
      return constructRow(operator, context);
    }

    if (UDFDetector.containNonSystemFunction(operator.getFilter())) {
      return constructRow(operator, context);
    }

    operator = LogicalJoinUtils.reverse(operator);

    PhysicalTask<?> leftTask = fetch(operator.getSourceA(), context);
    PhysicalTask<?> rightTask = fetchAsync(operator.getSourceB(), context);

    return new BinarySinkMemoryPhysicalTask(
        convert(leftTask, context, BatchStream.class),
        convert(rightTask, context, BatchStream.class),
        operator,
        context,
        new OuterJoinInfoGenerator(operator));
  }

  public PhysicalTask<?> construct(MarkJoin operator, RequestContext context) {
    if (operator.getJoinAlgType() != JoinAlgType.HashJoin
        && operator.getJoinAlgType() != JoinAlgType.NestedLoopJoin) {
      return constructRow(operator, context);
    }

    if (UDFDetector.containNonSystemFunction(operator.getFilter())) {
      return constructRow(operator, context);
    }

    operator = LogicalJoinUtils.reverse(operator);

    PhysicalTask<?> leftTask = fetch(operator.getSourceA(), context);
    PhysicalTask<?> rightTask = fetchAsync(operator.getSourceB(), context);

    return new BinarySinkMemoryPhysicalTask(
        convert(leftTask, context, BatchStream.class),
        convert(rightTask, context, BatchStream.class),
        operator,
        context,
        new MarkJoinInfoGenerator(operator));
  }

  public PhysicalTask<?> construct(SingleJoin operator, RequestContext context) {
    if (operator.getJoinAlgType() != JoinAlgType.HashJoin
        && operator.getJoinAlgType() != JoinAlgType.NestedLoopJoin) {
      return constructRow(operator, context);
    }

    if (UDFDetector.containNonSystemFunction(operator.getFilter())) {
      return constructRow(operator, context);
    }

    operator = LogicalJoinUtils.reverse(operator);

    PhysicalTask<?> leftTask = fetch(operator.getSourceA(), context);
    PhysicalTask<?> rightTask = fetchAsync(operator.getSourceB(), context);

    return new BinarySinkMemoryPhysicalTask(
        convert(leftTask, context, BatchStream.class),
        convert(rightTask, context, BatchStream.class),
        operator,
        context,
        new SingleJoinInfoGenerator(operator));
  }

  public PhysicalTask<BatchStream> construct(CrossJoin operator, RequestContext context) {
    operator = LogicalJoinUtils.reverse(operator);

    PhysicalTask<?> leftTask = fetch(operator.getSourceA(), context);
    PhysicalTask<?> rightTask = fetchAsync(operator.getSourceB(), context);

    return new BinarySinkMemoryPhysicalTask(
        convert(leftTask, context, BatchStream.class),
        convert(rightTask, context, BatchStream.class),
        operator,
        context,
        new CrossJoinInfoGenerator(operator));
  }

  public PhysicalTask<?> construct(FoldedOperator operator, RequestContext context) {
    List<PhysicalTask<BatchStream>> sourceTasks = new ArrayList<>();
    for (Source source : operator.getSources()) {
      sourceTasks.add(convert(fetch(source, context), context, BatchStream.class));
    }
    return new FoldedMemoryPhysicalTask(
        Collections.singletonList(operator), operator.getIncompleteRoot(), sourceTasks, context);
  }

  public PhysicalTask<?> construct(ShowColumns operator, RequestContext context) {
    return new GlobalPhysicalTask(operator, context);
  }

  public PhysicalTask<?> construct(Migration operator, RequestContext context) {
    return new GlobalPhysicalTask(operator, context);
  }

  public PhysicalTask<?> construct(Join operator, RequestContext context) {
    return constructRow(operator, context);
  }

  public PhysicalTask<?> construct(PathUnion operator, RequestContext context) {
    return constructRow(operator, context);
  }

  public PhysicalTask<?> construct(Union operator, RequestContext context) {
    return constructRow(operator, context);
  }

  public PhysicalTask<?> construct(Except operator, RequestContext context) {
    return constructRow(operator, context);
  }

  public PhysicalTask<?> construct(Intersect operator, RequestContext context) {
    return constructRow(operator, context);
  }

  public PhysicalTask<?> construct(Downsample operator, RequestContext context) {
    return constructRow(operator, context);
  }

  public PhysicalTask<?> construct(MappingTransform operator, RequestContext context) {
    return constructRow(operator, context);
  }

  public PhysicalTask<?> construct(Distinct operator, RequestContext context) {
    return constructRow(operator, context);
  }

  public PhysicalTask<?> construct(ValueToSelectedPath operator, RequestContext context) {
    PhysicalTask<?> sourceTask = fetch(operator.getSource(), context);
    return new PipelineMemoryPhysicalTask(
        convert(sourceTask, context, BatchStream.class),
        Collections.singletonList(operator),
        context,
        (ctx, schema) -> new ValueToSelectedPathExecutor(ctx, schema.raw(), operator.getPrefix()));
  }

  public PhysicalTask<BatchStream> construct(AddSequence operator, RequestContext context) {
    PhysicalTask<?> sourceTask = fetch(operator.getSource(), context);
    return new UnarySinkMemoryPhysicalTask(
        convert(sourceTask, context, BatchStream.class),
        Collections.singletonList(operator),
        context,
        (ctx, schema) ->
            AddSequenceExecutor.of(
                ctx,
                schema.raw(),
                operator.getStartList(),
                operator.getIncrementList(),
                operator.getColumns()));
  }

  public PhysicalTask<BatchStream> construct(RemoveNullColumn operator, RequestContext context) {
    PhysicalTask<?> sourceTask = fetch(operator.getSource(), context);
    return new UnarySinkMemoryPhysicalTask(
        convert(sourceTask, context, BatchStream.class),
        Collections.singletonList(operator),
        context,
        (ctx, schema) -> new RemoveNullColumnExecutor(ctx, schema.raw()));
  }

  @SuppressWarnings("unchecked")
  public <RESULT extends PhysicalCloseable> PhysicalTask<RESULT> convert(
      PhysicalTask<?> task, RequestContext context, Class<RESULT> destResultClass) {
    Class<?> sourceResultClass = task.getResultClass();
    if (sourceResultClass == destResultClass) {
      return (PhysicalTask<RESULT>) task;
    }
    if (sourceResultClass == RowStream.class && destResultClass == BatchStream.class) {
      return (PhysicalTask<RESULT>)
          new RowToArrowUnaryMemoryPhysicalTask((PhysicalTask<RowStream>) task, context);
    }
    if (sourceResultClass == BatchStream.class && destResultClass == RowStream.class) {
      return (PhysicalTask<RESULT>)
          new ArrowToRowUnaryMemoryPhysicalTask((PhysicalTask<BatchStream>) task, context);
    }
    throw new UnsupportedOperationException(
        "Unsupported conversion from " + sourceResultClass + " to " + destResultClass);
  }
}
