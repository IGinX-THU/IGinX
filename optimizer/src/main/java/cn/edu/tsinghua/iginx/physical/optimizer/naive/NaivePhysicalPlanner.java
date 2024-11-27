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

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateful.LimitUnaryExecutor;
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
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.physical.optimizer.naive.initializer.*;
import cn.edu.tsinghua.iginx.physical.optimizer.naive.util.Joins;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

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
      default:
        throw new UnsupportedOperationException("Unsupported source type: " + source.getType());
    }
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
        convert(sourceTask, context, RowStream.class),
        Collections.singletonList(operator),
        operator,
        context);
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
      return reConstruct((StoragePhysicalTask) sourceTask, context, false, operator);
    }
    return new PipelineMemoryPhysicalTask(
        convert(sourceTask, context, BatchStream.class),
        Collections.singletonList(operator),
        context,
        new SimpleProjectionInfoGenerator(operator));
  }

  public PhysicalTask<BatchStream> construct(Rename operator, RequestContext context) {
    PhysicalTask<?> sourceTask = fetch(operator.getSource(), context);
    return new cn.edu.tsinghua.iginx.engine.physical.task.memory.PipelineMemoryPhysicalTask(
        convert(sourceTask, context, BatchStream.class),
        Collections.singletonList(operator),
        context,
        new SimpleProjectionInfoGenerator(operator));
  }

  public PhysicalTask<BatchStream> construct(Reorder operator, RequestContext context) {
    PhysicalTask<?> sourceTask = fetch(operator.getSource(), context);
    return new cn.edu.tsinghua.iginx.engine.physical.task.memory.PipelineMemoryPhysicalTask(
        convert(sourceTask, context, BatchStream.class),
        Collections.singletonList(operator),
        context,
        new SimpleProjectionInfoGenerator(operator));
  }

  public PhysicalTask<BatchStream> construct(AddSchemaPrefix operator, RequestContext context) {
    PhysicalTask<?> sourceTask = fetch(operator.getSource(), context);
    return new cn.edu.tsinghua.iginx.engine.physical.task.memory.PipelineMemoryPhysicalTask(
        convert(sourceTask, context, BatchStream.class),
        Collections.singletonList(operator),
        context,
        new SimpleProjectionInfoGenerator(operator));
  }

  public PhysicalTask<BatchStream> construct(RowTransform operator, RequestContext context) {
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
                new TransformProjectionInfoGenerator(operator)),
        pipelineParallelism);
  }

  public PhysicalTask<?> construct(Select operator, RequestContext context) {
    Source source = operator.getSource();
    PhysicalTask<?> sourceTask = fetch(operator.getSource(), context);

    StoragePhysicalTask storageTask = tryPushDownAloneWithProject(sourceTask, context, operator);
    if (storageTask != null) {
      return storageTask;
    }

    PhysicalTask<BatchStream> batchTask = convert(sourceTask, context, BatchStream.class);

    if (operator.getTagFilter() != null) {
      batchTask =
          new PipelineMemoryPhysicalTask(
              batchTask,
              Collections.singletonList(new Select(source, null, operator.getTagFilter())),
              context,
              new TagKVProjectionInfoGenerator(operator.getTagFilter()));
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

    if (childOperators.size() != 1) {
      return null;
    }
    Operator sourceOperator = childOperators.get(0);

    if (sourceOperator.getType() != OperatorType.Project) {
      return null;
    }
    Project project = (Project) sourceOperator;

    if (project.getTagFilter() == null) {
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

    return new UnarySinkMemoryPhysicalTask(
        convert(sourceTask, context, BatchStream.class),
        Collections.singletonList(operator),
        context,
        new AggregateInfoGenerator(operator));
  }

  public PhysicalTask<BatchStream> construct(GroupBy operator, RequestContext context) {
    PhysicalTask<?> sourceTask = fetch(operator.getSource(), context);
    return new UnarySinkMemoryPhysicalTask(
        convert(sourceTask, context, BatchStream.class),
        Collections.singletonList(operator),
        context,
        new GroupsAggregateInfoGenerator(operator));
  }

  public PhysicalTask<?> construct(Sort operator, RequestContext context) {
    PhysicalTask<?> sourceTask = fetch(operator.getSource(), context);

    //      sourceTask = new PipelineMemoryPhysicalTask(
    //          convert(sourceTask, context, BatchStream.class),
    //          Collections.singletonList(operator),
    //          context,
    //          new InnerBatchSortInfoGenerator(operator));

    return new UnaryRowMemoryPhysicalTask(
        convert(sourceTask, context, RowStream.class),
        Collections.singletonList(operator),
        operator,
        context);
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
    if (operator.getJoinAlgType() != JoinAlgType.HashJoin || operator.isNaturalJoin()) {
      return constructRow(operator, context);
    }

    // NOTE: The order of left and right task is reversed in InnerJoin
    // 这里以及后面交换了左右两个表的顺序，原因是在之前基于行的实现中，右表是BuildSide，左表是ProbeSide
    // 现在基于列的实现中，左表是BuildSide，右表是ProbeSide
    operator = Joins.reverse(operator);

    PhysicalTask<BatchStream> leftTask = fetchAsync(operator.getSourceA(), context);
    PhysicalTask<BatchStream> rightTask = fetchAsync(operator.getSourceB(), context);

    return new BinarySinkMemoryPhysicalTask(
        convert(leftTask, context, BatchStream.class),
        convert(rightTask, context, BatchStream.class),
        operator,
        context,
        new InnerJoinInfoGenerator(operator));
  }

  public PhysicalTask<?> construct(OuterJoin operator, RequestContext context) {
    if (operator.getJoinAlgType() != JoinAlgType.HashJoin || operator.isNaturalJoin()) {
      return constructRow(operator, context);
    }

    operator = Joins.reverse(operator);

    PhysicalTask<BatchStream> leftTask = fetchAsync(operator.getSourceA(), context);
    PhysicalTask<BatchStream> rightTask = fetchAsync(operator.getSourceB(), context);

    return new BinarySinkMemoryPhysicalTask(
        convert(leftTask, context, BatchStream.class),
        convert(rightTask, context, BatchStream.class),
        operator,
        context,
        new OuterJoinInfoGenerator(operator));
  }

  public PhysicalTask<?> construct(MarkJoin operator, RequestContext context) {
    if (operator.getJoinAlgType() != JoinAlgType.HashJoin) {
      return constructRow(operator, context);
    }

    operator = Joins.reverse(operator);

    PhysicalTask<BatchStream> leftTask = fetchAsync(operator.getSourceA(), context);
    PhysicalTask<BatchStream> rightTask = fetchAsync(operator.getSourceB(), context);

    return new BinarySinkMemoryPhysicalTask(
        convert(leftTask, context, BatchStream.class),
        convert(rightTask, context, BatchStream.class),
        operator,
        context,
        new MarkJoinInfoGenerator(operator));
  }

  public PhysicalTask<?> construct(SingleJoin operator, RequestContext context) {
    if (operator.getJoinAlgType() != JoinAlgType.HashJoin) {
      return constructRow(operator, context);
    }

    operator = Joins.reverse(operator);

    PhysicalTask<BatchStream> leftTask = fetchAsync(operator.getSourceA(), context);
    PhysicalTask<BatchStream> rightTask = fetchAsync(operator.getSourceB(), context);

    return new BinarySinkMemoryPhysicalTask(
        convert(leftTask, context, BatchStream.class),
        convert(rightTask, context, BatchStream.class),
        operator,
        context,
        new SingleJoinInfoGenerator(operator));
  }

  public PhysicalTask<?> construct(CrossJoin operator, RequestContext context) {
    return constructRow(operator, context);
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
    throw new UnsupportedOperationException(
        "Migration is not supported in the new physical planner");
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
