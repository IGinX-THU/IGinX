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
package cn.edu.tsinghua.iginx.engine.physical.task;

import static cn.edu.tsinghua.iginx.engine.logical.utils.MetaUtils.getFragmentsByColumnsInterval;
import static cn.edu.tsinghua.iginx.engine.logical.utils.MetaUtils.mergeRawData;
import static cn.edu.tsinghua.iginx.engine.physical.task.utils.TaskUtils.getBottomTasks;
import static cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType.isUnaryOperator;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.logical.constraint.ConstraintChecker;
import cn.edu.tsinghua.iginx.engine.logical.constraint.ConstraintCheckerManager;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.PhysicalOptimizer;
import cn.edu.tsinghua.iginx.engine.physical.storage.execute.StoragePhysicalTaskExecutor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.ProjectWaitingForPath;
import cn.edu.tsinghua.iginx.engine.shared.operator.Reorder;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.SortUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FoldedMemoryPhysicalTask extends MultipleMemoryPhysicalTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(FoldedMemoryPhysicalTask.class);

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final PhysicalEngine engine = PhysicalEngineImpl.getInstance();

  private static final ConstraintChecker checker =
      ConstraintCheckerManager.getInstance().getChecker(config.getConstraintChecker());

  private static final ConstraintManager constraintManager = engine.getConstraintManager();

  private static final PhysicalOptimizer optimizer = engine.getOptimizer();

  private static final StoragePhysicalTaskExecutor storageTaskExecutor =
      engine.getStoragePhysicalTaskExecutor();

  private final Operator foldedRoot;

  public FoldedMemoryPhysicalTask(
      List<Operator> operators,
      Operator foldedRoot,
      List<PhysicalTask> parentTasks,
      RequestContext context) {
    super(operators, parentTasks, context);
    this.foldedRoot = foldedRoot;
  }

  public Operator getFoldedRoot() {
    return foldedRoot;
  }

  @Override
  public TaskExecuteResult execute() {
    List<RowStream> streams = new ArrayList<>();
    for (PhysicalTask parentTask : getParentTasks()) {
      TaskExecuteResult parentResult = parentTask.getResult();
      if (parentResult == null) {
        return new TaskExecuteResult(
            new PhysicalException("unexpected parent task execute result for " + this + ": null"));
      }
      if (parentResult.getException() != null) {
        return parentResult;
      }
      streams.add(parentResult.getRowStream());
    }

    // 根据运行时结果生成最终操作树
    Operator finalRoot = reGenerateRoot(foldedRoot, streams);
    if (!constraintManager.check(finalRoot) || !checker.check(finalRoot)) {
      throw new RuntimeException(
          "Execute Error: can not reconstruct this folded operator to a legal logical tree.");
    }

    PhysicalTask task = optimizer.optimize(finalRoot, getContext());

    PhysicalTask originFollowTask = getFollowerTask();
    task.setFollowerTask(originFollowTask);
    if (originFollowTask instanceof CompletedFoldedPhysicalTask) {
      ((CompletedFoldedPhysicalTask) originFollowTask).setParentTask(task);
    } else {
      throw new RuntimeException(
          "The follow task of a FoldedMemoryPhysicalTask is expected a CompletedFoldedPhysicalTask.");
    }
    setFollowerTask(null);

    List<PhysicalTask> bottomTasks = new ArrayList<>();
    getBottomTasks(bottomTasks, task);
    commitBottomTasks(bottomTasks);

    return null;
  }

  private void commitBottomTasks(List<PhysicalTask> bottomTasks) {
    List<StoragePhysicalTask> storageTasks = new ArrayList<>();
    List<GlobalPhysicalTask> globalTasks = new ArrayList<>();
    for (PhysicalTask task : bottomTasks) {
      if (task.getType().equals(TaskType.Storage)) {
        storageTasks.add((StoragePhysicalTask) task);
      } else if (task.getType().equals(TaskType.Global)) {
        globalTasks.add((GlobalPhysicalTask) task);
      }
    }

    storageTaskExecutor.commit(storageTasks);
    for (GlobalPhysicalTask globalTask : globalTasks) {
      storageTaskExecutor.executeGlobalTask(globalTask);
    }
  }

  private Operator reGenerateRoot(Operator root, List<RowStream> streams) {
    Set<String> set = new HashSet<>();
    List<String> selectedPaths = new ArrayList<>();
    streams.forEach(
        stream -> {
          try {
            int index = stream.getHeader().indexOf("SelectedPath");
            if (index == -1) {
              return;
            }
            while (stream.hasNext()) {
              Row row = stream.next();
              Value value = row.getAsValue(index);
              if (value.isNull()) {
                continue;
              }
              String path = value.getBinaryVAsString();
              if (!set.contains(path)) {
                set.add(path);
                selectedPaths.add(path);
              }
            }
          } catch (PhysicalException e) {
            LOGGER.error("encounter error when execute operator in memory: ", e);
            throw new RuntimeException(e);
          }
        });

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
}
