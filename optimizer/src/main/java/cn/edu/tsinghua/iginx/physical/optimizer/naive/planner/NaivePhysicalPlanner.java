package cn.edu.tsinghua.iginx.physical.optimizer.naive.planner;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.pipeline.FilterExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.pipeline.ProjectionExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.sink.AggregateExecutor;
import cn.edu.tsinghua.iginx.engine.physical.task.*;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NaivePhysicalPlanner {

  public PhysicalTask construct(Operator operator, RequestContext context) {
    switch (operator.getType()) {
      case Insert:
        return construct((Insert) operator, context);
      case Delete:
        return construct((Delete) operator, context);
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
      case SetTransform:
        return construct((SetTransform) operator, context);
      default:
        throw new UnsupportedOperationException("Unsupported operator type: " + operator.getType());
    }
  }

  public PhysicalTask construct(Source source, RequestContext context) {
    switch (source.getType()) {
      case Fragment:
        FragmentSource fragmentSource = (FragmentSource) source;
        return new StoragePhysicalTask(new ArrayList<>(), fragmentSource.getFragment(), true, false, context);
      case Operator:
        OperatorSource operatorSource = (OperatorSource) source;
        return construct(operatorSource.getOperator(), context);
      default:
        throw new UnsupportedOperationException("Unsupported source type: " + source.getType());
    }
  }

  public PhysicalTask construct(Insert operator, RequestContext context) {
    return constructWriteTask(operator, context);
  }

  public PhysicalTask construct(Delete operator, RequestContext context) {
    return constructWriteTask(operator, context);
  }

  private PhysicalTask constructWriteTask(UnaryOperator operator, RequestContext context) {
    PhysicalTask task = construct(operator.getSource(), context);
    if (task.getType() != TaskType.Storage) {
      throw new IllegalStateException("Unexpected task type: " + task.getType());
    }
    return reConstruct((StoragePhysicalTask) task, context, true, operator);
  }

  private StoragePhysicalTask reConstruct(StoragePhysicalTask storageTask, RequestContext context, boolean needBroadcasting, Operator... extraOperators) {
    List<Operator> newOperators = new ArrayList<>(storageTask.getOperators());
    Collections.addAll(newOperators, extraOperators);
    return new StoragePhysicalTask(newOperators, storageTask.getTargetFragment(), storageTask.isSync(), needBroadcasting, context);
  }

  private void checkIsMemoryTask(PhysicalTask task) {
    if (!TaskType.isMemoryTask(task.getType())) {
      throw new IllegalStateException("Unexpected task type: " + task.getType());
    }
  }

  public PhysicalTask construct(Project operator, RequestContext context) {
    PhysicalTask sourceTask = construct(operator.getSource(), context);
    if (sourceTask.getType() == TaskType.Storage) {
      return reConstruct((StoragePhysicalTask) sourceTask, context, false, operator);
    }
    checkIsMemoryTask(sourceTask);
    return new PipelineMemoryPhysicalTask(
        sourceTask,
        Collections.singletonList(operator),
        context,
        () -> new ProjectionExecutor(new SimpleProjectionInfoGenerator(operator))
    );
  }

  public PhysicalTask construct(Rename operator, RequestContext context) {
    PhysicalTask sourceTask = construct(operator.getSource(), context);
    checkIsMemoryTask(sourceTask);
    return new PipelineMemoryPhysicalTask(
        sourceTask,
        Collections.singletonList(operator),
        context,
        () -> new ProjectionExecutor(new SimpleProjectionInfoGenerator(operator))
    );
  }

  public PhysicalTask construct(Reorder operator, RequestContext context) {
    PhysicalTask sourceTask = construct(operator.getSource(), context);
    checkIsMemoryTask(sourceTask);
    return new PipelineMemoryPhysicalTask(
        sourceTask,
        Collections.singletonList(operator),
        context,
        () -> new ProjectionExecutor(new SimpleProjectionInfoGenerator(operator))
    );
  }

  public PhysicalTask construct(AddSchemaPrefix operator, RequestContext context) {
    PhysicalTask sourceTask = construct(operator.getSource(), context);
    checkIsMemoryTask(sourceTask);
    return new PipelineMemoryPhysicalTask(
        sourceTask,
        Collections.singletonList(operator),
        context,
        () -> new ProjectionExecutor(new SimpleProjectionInfoGenerator(operator))
    );
  }

  public PhysicalTask construct(RowTransform operator, RequestContext context) {
    PhysicalTask sourceTask = construct(operator.getSource(), context);
    checkIsMemoryTask(sourceTask);
    return new PipelineMemoryPhysicalTask(
        sourceTask,
        Collections.singletonList(operator),
        context,
        () -> new ProjectionExecutor(new TransformProjectionInfoGenerator(operator))
    );
  }

  public PhysicalTask construct(Select operator, RequestContext context) {
    Source source = operator.getSource();
    PhysicalTask sourceTask = construct(operator.getSource(), context);

    StoragePhysicalTask storageTask = tryPushDownAloneWithProject(sourceTask, context, operator);
    if (storageTask != null) {
      return storageTask;
    }

    checkIsMemoryTask(sourceTask);

    if (operator.getTagFilter() != null) {
      sourceTask = new PipelineMemoryPhysicalTask(
          sourceTask,
          Collections.singletonList(new Select(source, null, operator.getTagFilter())),
          context,
          () -> new ProjectionExecutor(new TagKVProjectionInfoGenerator(operator.getTagFilter()))
      );
    }

    if (operator.getFilter() != null) {
      sourceTask = new PipelineMemoryPhysicalTask(
          sourceTask,
          Collections.singletonList(new Select(source, operator.getFilter(), null)),
          context,
          () -> new FilterExecutor(new FilterInfoGenerator(operator.getFilter()))
      );
    }

    return sourceTask;
  }

  @Nullable
  private StoragePhysicalTask tryPushDownAloneWithProject(PhysicalTask sourceTask, RequestContext context, Operator operator) {
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
    PhysicalTask sourceTask = construct(operator.getSource(), context);
    checkIsMemoryTask(sourceTask);

    StoragePhysicalTask storageTask = tryPushDownAloneWithProject(sourceTask, context, operator);
    if (storageTask != null) {
      return storageTask;
    }

    checkIsMemoryTask(sourceTask);
    return new UnarySinkMemoryPhysicalTask(
        sourceTask,
        Collections.singletonList(operator),
        context,
        () -> new AggregateExecutor(new AggregateInfoGenerator(operator))
    );
  }

}
