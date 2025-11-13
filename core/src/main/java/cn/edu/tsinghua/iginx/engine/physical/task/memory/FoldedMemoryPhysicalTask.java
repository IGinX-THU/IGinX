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
package cn.edu.tsinghua.iginx.engine.physical.task.memory;

import static cn.edu.tsinghua.iginx.engine.logical.utils.MetaUtils.getFragmentsByColumnsInterval;
import static cn.edu.tsinghua.iginx.engine.logical.utils.MetaUtils.mergeRawData;
import static cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType.isUnaryOperator;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.logical.constraint.ConstraintChecker;
import cn.edu.tsinghua.iginx.engine.logical.constraint.ConstraintCheckerManager;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless.ValueToSelectedPathExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.PhysicalOptimizer;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskResult;
import cn.edu.tsinghua.iginx.engine.physical.task.utils.PhysicalCloseable;
import cn.edu.tsinghua.iginx.engine.physical.task.utils.PhysicalCloseables;
import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskVisitor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStreams;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.SortUtils;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.annotation.WillClose;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class FoldedMemoryPhysicalTask extends MultiMemoryPhysicalTask<BatchStream, BatchStream> {

  private final Operator foldedRoot;
  private PhysicalTask<BatchStream> reConstructedTask;

  public FoldedMemoryPhysicalTask(
      List<Operator> operators,
      Operator foldedRoot,
      List<PhysicalTask<BatchStream>> parentTasks,
      RequestContext context) {
    super(operators, context, parentTasks);
    this.foldedRoot = Objects.requireNonNull(foldedRoot);
  }

  @Override
  public Class<BatchStream> getResultClass() {
    return BatchStream.class;
  }

  @Override
  public String getInfo() {
    return "FoldedTask";
  }

  @Override
  public void accept(TaskVisitor visitor) {
    visitor.enter();
    visitor.visit(this);

    List<PhysicalTask<BatchStream>> tasks = getParentTasks();
    for (PhysicalTask<?> task : tasks) {
      if (task != null) {
        task.accept(visitor);
      }
    }
    if (reConstructedTask != null) {
      reConstructedTask.accept(visitor);
    }
    visitor.leave();
  }

  @Override
  public TaskResult<BatchStream> execute() {
    List<BatchStream> streams = new ArrayList<>();
    try (PhysicalCloseable ignored = () -> PhysicalCloseables.close(streams)) {
      for (PhysicalTask<BatchStream> parentTask : getParentTasks()) {
        streams.add(parentTask.getResult().get().unwrap());
      }
      PhysicalTask<BatchStream> task = reSubmitQuery(streams);
      reConstructedTask = task;
      return new TaskResultProxy(task);
    } catch (PhysicalException e) {
      return new TaskResult<>(e);
    } catch (InterruptedException | ExecutionException e) {
      return new TaskResult<>(new PhysicalException(e));
    }
  }

  private PhysicalTask<BatchStream> reSubmitQuery(@WillClose List<BatchStream> streams)
      throws PhysicalException {
    // 根据运行时结果生成最终操作树
    Operator finalRoot = reGenerateRoot(foldedRoot, streams);
    PhysicalEngine physicalEngine = getContext().getPhysicalEngine();
    ConstraintManager constraintManager = physicalEngine.getOptimizer().getConstraintManager();
    ConstraintChecker checker =
        ConstraintCheckerManager.getInstance()
            .getChecker(ConfigDescriptor.getInstance().getConfig().getConstraintChecker());
    if (!constraintManager.check(finalRoot) || !checker.check(finalRoot)) {
      throw new PhysicalException(
          "Execute Error: can not reconstruct this folded operator to a legal logical tree.");
    }

    PhysicalOptimizer optimizer = physicalEngine.getOptimizer();
    PhysicalTask<BatchStream> task = optimizer.optimize(finalRoot, getContext(), BatchStream.class);

    task.setFollowerTask(getFollowerTask());
    setFollowerTask(null);

    physicalEngine.submit(task);
    return task;
  }

  private Operator reGenerateRoot(Operator root, List<BatchStream> streams)
      throws PhysicalException {
    Set<String> set = new HashSet<>();
    List<String> selectedPaths = new ArrayList<>();
    for (BatchStream stream : streams) {
      while (stream.hasNext()) {
        try (Batch batch = stream.getNext()) {
          try (VectorSchemaRoot columns = batch.flattened(getContext().getAllocator())) {
            VarBinaryVector selectedPathVector =
                (VarBinaryVector)
                    columns.getVector(ValueToSelectedPathExecutor.SELECTED_PATH_FIELD);
            for (byte[] bytes : selectedPathVector.getValueIterable()) {
              if (bytes == null) {
                continue;
              }
              String path = new String(bytes);
              if (!set.contains(path)) {
                set.add(path);
                selectedPaths.add(path);
              }
            }
          }
        }
      }
    }

    return fillRootWithPath(root, selectedPaths);
  }

  private Operator fillRootWithPath(Operator operator, List<String> selectedPaths) {
    switch (operator.getType()) {
      case Reorder:
        Reorder reorder = (Reorder) operator;
        reorder.setSource(
            new OperatorSource(
                fillRootWithPath(
                    ((OperatorSource) reorder.getSource()).getOperator(), selectedPaths)));
        if (reorder.isNeedSelectedPath()) {
          reorder.getPatterns().addAll(selectedPaths);
          selectedPaths.forEach(p -> reorder.getIsPyUDF().add(false));
          reorder.setNeedSelectedPath(false);
        }
        return reorder;
      case Project:
        Project project = (Project) operator;
        project.setSource(
            new OperatorSource(
                fillRootWithPath(
                    ((OperatorSource) project.getSource()).getOperator(), selectedPaths)));
        if (project.isNeedSelectedPath()) {
          project.getPatterns().addAll(selectedPaths);
          project.setNeedSelectedPath(false);
        }
        return project;
      case ProjectWaitingForPath:
        return unfoldProjectWaitingForPath((ProjectWaitingForPath) operator, selectedPaths);
      default:
        if (isUnaryOperator(operator.getType())) {
          UnaryOperator unaryOperator = (UnaryOperator) operator;
          unaryOperator.setSource(
              new OperatorSource(
                  fillRootWithPath(
                      ((OperatorSource) unaryOperator.getSource()).getOperator(), selectedPaths)));
          return unaryOperator;
        } else {
          throw new RuntimeException(
              "unexpected operator type when unfolding root with path: " + operator.getType());
        }
    }
  }

  private Operator unfoldProjectWaitingForPath(
      ProjectWaitingForPath project, List<String> selectedPaths) {
    List<String> pathList =
        SortUtils.mergeAndSortPaths(selectedPaths.stream().distinct().collect(Collectors.toList()));
    TagFilter tagFilter = project.getIncompleteStatement().getTagFilter();

    ColumnsInterval columnsInterval =
        new ColumnsInterval(pathList.get(0), pathList.get(pathList.size() - 1));
    Pair<Map<KeyInterval, List<FragmentMeta>>, List<FragmentMeta>> pair =
        getFragmentsByColumnsInterval(project.getIncompleteStatement(), columnsInterval);
    Map<KeyInterval, List<FragmentMeta>> fragments = pair.k;
    List<FragmentMeta> dummyFragments = pair.v;

    return mergeRawData(fragments, dummyFragments, pathList, tagFilter);
  }

  private static class TaskResultProxy extends TaskResult<BatchStream> {
    private final PhysicalTask<BatchStream> task;

    public TaskResultProxy(PhysicalTask<BatchStream> task) {
      super(BatchStreams.empty());
      this.task = Objects.requireNonNull(task);
    }

    @Override
    public BatchStream unwrap() throws PhysicalException {
      try {
        return task.getResult().get().unwrap();
      } catch (InterruptedException | ExecutionException e) {
        throw new PhysicalException(e);
      }
    }

    @Override
    public void close() {}
  }
}
