package cn.edu.tsinghua.iginx.engine.physical.task;

import static cn.edu.tsinghua.iginx.engine.logical.utils.MetaUtils.getFragmentsByColumnsInterval;
import static cn.edu.tsinghua.iginx.engine.logical.utils.MetaUtils.mergeRawData;
import static cn.edu.tsinghua.iginx.engine.physical.task.utils.TaskUtils.getStorageTasks;
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

public class FoldedMemoryPhysicalTask extends MemoryPhysicalTask {

  private static final Logger logger = LoggerFactory.getLogger(FoldedMemoryPhysicalTask.class);

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final PhysicalEngine engine = PhysicalEngineImpl.getInstance();

  private static final ConstraintChecker checker =
      ConstraintCheckerManager.getInstance().getChecker(config.getConstraintChecker());

  private static final ConstraintManager constraintManager = engine.getConstraintManager();

  private static final PhysicalOptimizer optimizer = engine.getOptimizer();

  private static final StoragePhysicalTaskExecutor storageTaskExecutor =
      engine.getStoragePhysicalTaskExecutor();

  private final Operator foldedRoot;

  private final List<PhysicalTask> parentTasks;

  public FoldedMemoryPhysicalTask(
      List<Operator> operators, Operator foldedRoot, List<PhysicalTask> parentTasks) {
    super(TaskType.Folded, operators);
    this.foldedRoot = foldedRoot;
    this.parentTasks = parentTasks;
  }

  public Operator getFoldedRoot() {
    return foldedRoot;
  }

  public List<PhysicalTask> getParentTasks() {
    return parentTasks;
  }

  @Override
  public TaskExecuteResult execute() {
    List<RowStream> streams = new ArrayList<>();
    for (PhysicalTask parentTask : parentTasks) {
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

    PhysicalTask task = optimizer.optimize(finalRoot);

    PhysicalTask originFollowTask = getFollowerTask();
    task.setFollowerTask(originFollowTask);
    if (originFollowTask.getType().equals(TaskType.CompletedFolded)) {
      ((CompletedFoldedPhysicalTask) originFollowTask).setParentTask(task);
    } else {
      throw new RuntimeException(
          "The follow task of a FoldedMemoryPhysicalTask is expected a CompletedFoldedPhysicalTask.");
    }
    setFollowerTask(null);

    List<StoragePhysicalTask> storageTasks = new ArrayList<>();
    getStorageTasks(storageTasks, task);
    storageTaskExecutor.commit(storageTasks);

    return null;
  }

  private Operator reGenerateRoot(Operator root, List<RowStream> streams) {
    Set<String> selectedPaths = new HashSet<>();
    streams.forEach(
        stream -> {
          try {
            int index = stream.getHeader().indexOf("SelectedPath");
            if (index != -1) {
              while (stream.hasNext()) {
                Row row = stream.next();
                Value value = row.getAsValue(index);
                if (!value.isNull()) {
                  selectedPaths.add(value.getBinaryVAsString());
                }
              }
            }
          } catch (PhysicalException e) {
            logger.error("encounter error when execute operator in memory: ", e);
            throw new RuntimeException(e);
          }
        });

    return fillRootWithPath(root, new ArrayList<>(selectedPaths));
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

  @Override
  public boolean notifyParentReady() {
    return parentReadyCount.incrementAndGet() == parentTasks.size();
  }
}
