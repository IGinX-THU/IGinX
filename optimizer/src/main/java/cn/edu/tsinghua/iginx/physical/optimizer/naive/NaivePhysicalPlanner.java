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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateful.FetchAllUnaryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateful.LimitUnaryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.task.*;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
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

  public PhysicalTask construct(Operator operator, RequestContext context) {
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
      case MarkJoin:
        return construct((MarkJoin) operator, context);
      default:
        throw new UnsupportedOperationException("Unsupported operator type: " + operator.getType());
    }
  }

  public PhysicalTask fetch(Source source, RequestContext context) {
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

  public PhysicalTask fetchAll(Source source, RequestContext context) {
    PhysicalTask leftTask = fetch(source, context);
    return new UnarySinkMemoryPhysicalTask(
        leftTask,
        Collections.emptyList(),
        context,
        (ctx, schema) -> new FetchAllUnaryExecutor(ctx, schema.raw()));
  }

  public PhysicalTask construct(CombineNonQuery operator, RequestContext context) {
    List<PhysicalTask> sourceTasks = new ArrayList<>();
    for (Source source : operator.getSources()) {
      sourceTasks.add(fetch(source, context));
    }

    return new MultipleMemoryPhysicalTask(
        Collections.singletonList(operator), sourceTasks, context);
  }

  public PhysicalTask construct(Insert operator, RequestContext context) {
    return constructWriteTask(operator, context);
  }

  public PhysicalTask construct(Delete operator, RequestContext context) {
    return constructWriteTask(operator, context);
  }

  private PhysicalTask constructWriteTask(UnaryOperator operator, RequestContext context) {
    PhysicalTask task = fetch(operator.getSource(), context);
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

  public PhysicalTask construct(Project operator, RequestContext context) {
    PhysicalTask sourceTask = fetch(operator.getSource(), context);
    if (sourceTask.getType() == TaskType.Storage) {
      return reConstruct((StoragePhysicalTask) sourceTask, context, false, operator);
    }
    return new PipelineMemoryPhysicalTask(
        sourceTask,
        Collections.singletonList(operator),
        context,
        new SimpleProjectionInfoGenerator(operator));
  }

  public PhysicalTask construct(Rename operator, RequestContext context) {
    PhysicalTask sourceTask = fetch(operator.getSource(), context);
    return new PipelineMemoryPhysicalTask(
        sourceTask,
        Collections.singletonList(operator),
        context,
        new SimpleProjectionInfoGenerator(operator));
  }

  public PhysicalTask construct(Reorder operator, RequestContext context) {
    PhysicalTask sourceTask = fetch(operator.getSource(), context);
    return new PipelineMemoryPhysicalTask(
        sourceTask,
        Collections.singletonList(operator),
        context,
        new SimpleProjectionInfoGenerator(operator));
  }

  public PhysicalTask construct(AddSchemaPrefix operator, RequestContext context) {
    PhysicalTask sourceTask = fetch(operator.getSource(), context);
    return new PipelineMemoryPhysicalTask(
        sourceTask,
        Collections.singletonList(operator),
        context,
        new SimpleProjectionInfoGenerator(operator));
  }

  public PhysicalTask construct(RowTransform operator, RequestContext context) {
    PhysicalTask sourceTask = fetch(operator.getSource(), context);
    return new PipelineMemoryPhysicalTask(
        sourceTask,
        Collections.singletonList(operator),
        context,
        new TransformProjectionInfoGenerator(operator));
  }

  public PhysicalTask construct(Select operator, RequestContext context) {
    Source source = operator.getSource();
    PhysicalTask sourceTask = fetch(operator.getSource(), context);

    StoragePhysicalTask storageTask = tryPushDownAloneWithProject(sourceTask, context, operator);
    if (storageTask != null) {
      return storageTask;
    }

    if (operator.getTagFilter() != null) {
      sourceTask =
          new PipelineMemoryPhysicalTask(
              sourceTask,
              Collections.singletonList(new Select(source, null, operator.getTagFilter())),
              context,
              new TagKVProjectionInfoGenerator(operator.getTagFilter()));
    }

    if (operator.getFilter() != null) {
      sourceTask =
          new PipelineMemoryPhysicalTask(
              sourceTask,
              Collections.singletonList(new Select(source, operator.getFilter(), null)),
              context,
              new FilterInfoGenerator(operator.getFilter()));
    }

    return sourceTask;
  }

  @Nullable
  private StoragePhysicalTask tryPushDownAloneWithProject(
      PhysicalTask sourceTask, RequestContext context, Operator operator) {
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

  public PhysicalTask construct(SetTransform operator, RequestContext context) {
    PhysicalTask sourceTask = fetch(operator.getSource(), context);
    StoragePhysicalTask storageTask = tryPushDownAloneWithProject(sourceTask, context, operator);
    if (storageTask != null) {
      return storageTask;
    }

    return new UnarySinkMemoryPhysicalTask(
        sourceTask,
        Collections.singletonList(operator),
        context,
        new AggregateInfoGenerator(operator));
  }

  public PhysicalTask construct(GroupBy operator, RequestContext context) {
    PhysicalTask sourceTask = fetch(operator.getSource(), context);
    return new UnarySinkMemoryPhysicalTask(
        sourceTask,
        Collections.singletonList(operator),
        context,
        new GroupsAggregateInfoGenerator(operator));
  }

  public PhysicalTask construct(Sort operator, RequestContext context) {
    PhysicalTask sourceTask = fetch(operator.getSource(), context);

    PipelineMemoryPhysicalTask sortBatchTask =
        new PipelineMemoryPhysicalTask(
            sourceTask,
            Collections.singletonList(operator),
            context,
            new InnerBatchSortInfoGenerator(operator));

    return new UnarySinkMemoryPhysicalTask(
        sortBatchTask,
        Collections.singletonList(operator),
        context,
        new MergeSortedBatchInfoGenerator(operator));
  }

  public PhysicalTask construct(Limit operator, RequestContext context) {
    PhysicalTask sourceTask = fetch(operator.getSource(), context);

    return new UnarySinkMemoryPhysicalTask(
        sourceTask,
        Collections.singletonList(operator),
        context,
        (ctx, schema) ->
            new LimitUnaryExecutor(ctx, schema.raw(), operator.getOffset(), operator.getLimit()));
  }

  public PhysicalTask construct(InnerJoin operator, RequestContext context) {
    // NOTE: The order of left and right task is reversed in InnerJoin
    // 这里以及后面交换了左右两个表的顺序，原因是在之前基于行的实现中，右表是BuildSide，左表是ProbeSide
    // 现在基于列的实现中，左表是BuildSide，右表是ProbeSide
    operator = Joins.reverse(operator);

    PhysicalTask leftTask = fetchAll(operator.getSourceA(), context);
    PhysicalTask rightTask = fetchAll(operator.getSourceB(), context);

    return new BinarySinkMemoryPhysicalTask(
        leftTask,
        rightTask,
        Collections.singletonList(operator),
        context,
        new InnerJoinInfoGenerator(operator));
  }

  public PhysicalTask construct(OuterJoin operator, RequestContext context) {
    operator = Joins.reverse(operator);

    PhysicalTask leftTask = fetchAll(operator.getSourceA(), context);
    PhysicalTask rightTask = fetchAll(operator.getSourceB(), context);

    return new BinarySinkMemoryPhysicalTask(
        leftTask,
        rightTask,
        Collections.singletonList(operator),
        context,
        new OuterJoinInfoGenerator(operator));
  }

  public PhysicalTask construct(MarkJoin operator, RequestContext context) {
    operator = Joins.reverse(operator);

    PhysicalTask leftTask = fetchAll(operator.getSourceA(), context);
    PhysicalTask rightTask = fetchAll(operator.getSourceB(), context);

    return new BinarySinkMemoryPhysicalTask(
        leftTask,
        rightTask,
        Collections.singletonList(operator),
        context,
        new MarkJoinInfoGenerator(operator));
  }
}
