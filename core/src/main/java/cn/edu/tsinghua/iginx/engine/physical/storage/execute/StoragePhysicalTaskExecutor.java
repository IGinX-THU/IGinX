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
package cn.edu.tsinghua.iginx.engine.physical.storage.execute;

import cn.edu.tsinghua.iginx.auth.SessionManager;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.exception.NonExecutablePhysicalTaskException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.TooManyPhysicalTasksException;
import cn.edu.tsinghua.iginx.engine.physical.exception.UnexpectedOperatorException;
import cn.edu.tsinghua.iginx.engine.physical.memory.MemoryPhysicalTaskDispatcher;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.ReplicaDispatcher;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.StorageManager;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.storage.queue.StoragePhysicalTaskQueue;
import cn.edu.tsinghua.iginx.engine.physical.task.GlobalPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.MemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.SetTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.ShowColumns;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.StorageUnitHook;
import cn.edu.tsinghua.iginx.monitor.HotSpotMonitor;
import cn.edu.tsinghua.iginx.monitor.RequestsMonitor;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoragePhysicalTaskExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(StoragePhysicalTaskExecutor.class);

  private static final StoragePhysicalTaskExecutor INSTANCE = new StoragePhysicalTaskExecutor();

  private final IMetaManager metaManager = DefaultMetaManager.getInstance();

  private final StorageManager storageManager =
      new StorageManager(metaManager.getStorageEngineList());

  private final Map<String, StoragePhysicalTaskQueue> storageTaskQueues = new ConcurrentHashMap<>();

  private final Map<String, ExecutorService> dispatchers = new ConcurrentHashMap<>();

  private ReplicaDispatcher replicaDispatcher;

  private MemoryPhysicalTaskDispatcher memoryTaskExecutor;

  private final int maxCachedPhysicalTaskPerStorage =
      ConfigDescriptor.getInstance().getConfig().getMaxCachedPhysicalTaskPerStorage();

  private StoragePhysicalTaskExecutor() {
    StorageUnitHook storageUnitHook =
        (before, after) -> {
          if (before == null && after != null) { // 新增加 du，处理这种事件，其他事件暂时不处理
            LOGGER.info("new storage unit {} come!", after.getId());
            String id = after.getId();
            boolean isDummy = after.isDummy();
            if (storageTaskQueues.containsKey(id)) {
              return;
            }
            storageTaskQueues.put(id, new StoragePhysicalTaskQueue());
            // 为拥有该分片的存储创建一个调度线程，用于调度任务执行
            ExecutorService dispatcher = Executors.newSingleThreadExecutor();
            long storageId = after.getStorageEngineId();
            dispatchers.put(id, dispatcher);
            dispatcher.submit(
                () -> {
                  try {
                    StoragePhysicalTaskQueue taskQueue = storageTaskQueues.get(id);
                    Pair<IStorage, ThreadPoolExecutor> p = storageManager.getStorage(storageId);
                    while (p == null) {
                      p = storageManager.getStorage(storageId);
                      LOGGER.info("spinning for IStorage!");
                      try {
                        Thread.sleep(5);
                      } catch (InterruptedException e) {
                        LOGGER.error("encounter error when spinning: ", e);
                      }
                    }
                    Pair<IStorage, ThreadPoolExecutor> pair = p;
                    while (true) {
                      StoragePhysicalTask task = taskQueue.getTask();
                      task.setStorageUnit(id);
                      task.setDummyStorageUnit(isDummy);
                      if (pair.v.getQueue().size() > maxCachedPhysicalTaskPerStorage) {
                        task.setResult(
                            new TaskExecuteResult(new TooManyPhysicalTasksException(storageId)));
                        continue;
                      }
                      if (isCancelled(task.getSessionId())) {
                        LOGGER.warn(
                            "StoragePhysicalTask[sessionId={}] is cancelled.", task.getSessionId());
                        continue;
                      }
                      pair.v.submit(
                          () -> {
                            TaskExecuteResult result = null;
                            long taskId = System.nanoTime();
                            long startTime = System.currentTimeMillis();
                            try {
                              List<Operator> operators = task.getOperators();
                              if (operators.size() < 1) {
                                result =
                                    new TaskExecuteResult(
                                        new NonExecutablePhysicalTaskException(
                                            "storage physical task should have one more operators"));
                                return;
                              }

                              Operator op = operators.get(0);
                              String storageUnit = task.getStorageUnit();
                              FragmentMeta fragmentMeta = task.getTargetFragment();
                              boolean isDummyStorageUnit = task.isDummyStorageUnit();
                              DataArea dataArea =
                                  new DataArea(storageUnit, fragmentMeta.getKeyInterval());

                              switch (op.getType()) {
                                case Project:
                                  boolean needSelectPushDown =
                                      pair.k.isSupportProjectWithSelect()
                                          && operators.size() == 2
                                          && operators.get(1).getType() == OperatorType.Select;
                                  boolean needSetTransformPushDown =
                                      operators.size() == 2
                                          && operators.get(1).getType()
                                              == OperatorType.SetTransform;
                                  boolean canSetTransformPushDown =
                                      needSetTransformPushDown
                                          && pair.k.isSupportProjectWithSetTransform(
                                              (SetTransform) operators.get(1), dataArea);
                                  if (isDummyStorageUnit) {
                                    if (needSelectPushDown) {
                                      result =
                                          pair.k.executeProjectDummyWithSelect(
                                              (Project) op, (Select) operators.get(1), dataArea);
                                    } else if (needSetTransformPushDown) {
                                      throw new IllegalStateException();
                                    } else {
                                      result = pair.k.executeProjectDummy((Project) op, dataArea);
                                    }
                                  } else {
                                    if (needSelectPushDown) {
                                      result =
                                          pair.k.executeProjectWithSelect(
                                              (Project) op, (Select) operators.get(1), dataArea);
                                    } else if (needSetTransformPushDown) {
                                      if (canSetTransformPushDown) {
                                        result =
                                            pair.k.executeProjectWithSetTransform(
                                                (Project) op,
                                                (SetTransform) operators.get(1),
                                                dataArea);
                                      } else {
                                        TaskExecuteResult tempResult =
                                            pair.k.executeProject((Project) op, dataArea);
                                        if (tempResult.getException() != null) {
                                          result = tempResult;
                                        } else {
                                          // set transform push down is not supported, execute set
                                          // transform in memory
                                          OperatorMemoryExecutor executor =
                                              OperatorMemoryExecutorFactory.getInstance()
                                                  .getMemoryExecutor();
                                          try {
                                            RowStream rowStream =
                                                executor.executeUnaryOperator(
                                                    (SetTransform) operators.get(1),
                                                    tempResult.getRowStream(),
                                                    task.getContext());
                                            result = new TaskExecuteResult(rowStream);
                                          } catch (PhysicalException e) {
                                            result = new TaskExecuteResult(e);
                                          }
                                        }
                                      }
                                    } else {
                                      result = pair.k.executeProject((Project) op, dataArea);
                                    }
                                  }
                                  break;
                                case Insert:
                                  result = pair.k.executeInsert((Insert) op, dataArea);
                                  break;
                                case Delete:
                                  result = pair.k.executeDelete((Delete) op, dataArea);
                                  break;
                                default:
                                  result =
                                      new TaskExecuteResult(
                                          new NonExecutablePhysicalTaskException(
                                              "unsupported physical task"));
                              }
                            } catch (Exception e) {
                              LOGGER.error("execute task error: ", e);
                              result = new TaskExecuteResult(new PhysicalException(e));
                            }
                            try {
                              HotSpotMonitor.getInstance()
                                  .recordAfter(
                                      taskId,
                                      task.getTargetFragment(),
                                      task.getOperators().get(0).getType());
                              RequestsMonitor.getInstance()
                                  .record(task.getTargetFragment(), task.getOperators().get(0));
                            } catch (Exception e) {
                              LOGGER.error("Monitor catch error:", e);
                            }
                            long span = System.currentTimeMillis() - startTime;
                            task.setSpan(span);
                            task.setResult(result);
                            if (task.getFollowerTask() != null
                                && task.isSync()) { // 只有同步任务才会影响后续任务的执行
                              MemoryPhysicalTask followerTask =
                                  (MemoryPhysicalTask) task.getFollowerTask();
                              boolean isFollowerTaskReady = followerTask.notifyParentReady();
                              if (isFollowerTaskReady) {
                                memoryTaskExecutor.addMemoryTask(followerTask);
                              }
                            }
                            if (task.isNeedBroadcasting()) { // 需要传播
                              if (result.getException() != null) {
                                LOGGER.error(
                                    "task "
                                        + task
                                        + " will not broadcasting to replicas for the sake of exception: "
                                        + result.getException());
                                task.setResult(new TaskExecuteResult(result.getException()));
                              } else {
                                StorageUnitMeta masterStorageUnit =
                                    task.getTargetFragment().getMasterStorageUnit();
                                List<String> replicaIds =
                                    masterStorageUnit.getReplicas().stream()
                                        .map(StorageUnitMeta::getId)
                                        .collect(Collectors.toList());
                                replicaIds.add(masterStorageUnit.getId());
                                for (String replicaId : replicaIds) {
                                  if (replicaId.equals(task.getStorageUnit())) {
                                    continue;
                                  }
                                  StoragePhysicalTask replicaTask =
                                      new StoragePhysicalTask(
                                          task.getOperators(), false, false, task.getContext());
                                  storageTaskQueues.get(replicaId).addTask(replicaTask);
                                  LOGGER.info("broadcasting task {} to {}", task, replicaId);
                                }
                              }
                            }
                          });
                    }
                  } catch (Exception e) {
                    LOGGER.error(
                        "unexpected exception during dispatcher storage task, please contact developer to check: ",
                        e);
                  }
                });
            LOGGER.info("process for new storage unit finished!");
          }
        };
    StorageEngineChangeHook storageEngineChangeHook =
        (before, after) -> {
          if (before == null && after != null) { // 新增加存储，处理这种事件，其他事件暂时不处理
            if (after.getCreatedBy() != metaManager.getIginxId()) {
              storageManager.addStorage(after);
            }
          }
        };
    metaManager.registerStorageEngineChangeHook(storageEngineChangeHook);
    metaManager.registerStorageUnitHook(storageUnitHook);
    List<StorageEngineMeta> storages = metaManager.getStorageEngineList();
    for (StorageEngineMeta storage : storages) {
      if (storage.isHasData()) {
        storageUnitHook.onChange(null, storage.getDummyStorageUnit());
      }
    }
  }

  private boolean isCancelled(long sessionId) {
    if (sessionId == 0) { // empty ctx
      return false;
    }
    return SessionManager.getInstance().isSessionClosed(sessionId);
  }

  public static StoragePhysicalTaskExecutor getInstance() {
    return INSTANCE;
  }

  public void commit(StoragePhysicalTask task) {
    commit(Collections.singletonList(task));
  }

  public void commitWithTargetStorageUnitId(StoragePhysicalTask task, String storageUnitId) {
    storageTaskQueues.get(storageUnitId).addTask(task);
  }

  public TaskExecuteResult executeGlobalTask(GlobalPhysicalTask task) {
    switch (task.getOperator().getType()) {
      case ShowColumns:
        long startTime = System.currentTimeMillis();
        TaskExecuteResult result = executeShowColumns((ShowColumns) task.getOperator());
        long span = System.currentTimeMillis() - startTime;
        task.setSpan(span);
        task.setResult(result);
        if (task.getFollowerTask() != null) {
          MemoryPhysicalTask followerTask = (MemoryPhysicalTask) task.getFollowerTask();
          boolean isFollowerTaskReady = followerTask.notifyParentReady();
          if (isFollowerTaskReady) {
            memoryTaskExecutor.addMemoryTask(followerTask);
          }
        }
        return result;
      default:
        return new TaskExecuteResult(
            new UnexpectedOperatorException("unknown op: " + task.getOperator().getType()));
    }
  }

  public TaskExecuteResult executeShowColumns(ShowColumns showColumns) {
    List<StorageEngineMeta> storageList = metaManager.getStorageEngineList();
    TreeSet<Column> columnSetAfterFilter =
        new TreeSet<>(Comparator.comparing(Column::getPhysicalPath));
    for (StorageEngineMeta storage : storageList) {
      long id = storage.getId();
      Pair<IStorage, ThreadPoolExecutor> pair = storageManager.getStorage(id);
      if (pair == null) {
        continue;
      }
      try {
        Set<String> patterns = showColumns.getPathRegexSet();
        TagFilter tagFilter = showColumns.getTagFilter();

        String schemaPrefix = storage.getSchemaPrefix();
        // 不下推dataPrefix的原因：iotdb中，非dummy的数据库中如果有原始数据，会在show columns时被查询到并造成误解
        String dataPrefixRegex =
            storage.getDataPrefix() == null
                ? null
                : StringUtils.reformatPath(storage.getDataPrefix() + ".*");

        // schemaPrefix是在IGinX中定义的，数据源的路径中没有该前缀，因此需要剪掉前缀是schemaPrefix的部分
        Set<String> patternsCutSchemaPrefix = cutSchemaPrefix(schemaPrefix, patterns);
        if (patternsCutSchemaPrefix.isEmpty()) {
          continue;
        }
        if (patternsCutSchemaPrefix.contains("*")) {
          patternsCutSchemaPrefix = Collections.emptySet();
        }
        List<Column> columnList = pair.k.getColumns(patternsCutSchemaPrefix, tagFilter);

        if (tagFilter != null) {
          columnSetAfterFilter.addAll(columnList);
          continue;
        }
        for (Column column : columnList) {
          if (!column.isDummy()) {
            columnSetAfterFilter.add(column);
            continue;
          }
          if (dataPrefixRegex == null || Pattern.matches(dataPrefixRegex, column.getPath())) {
            if (schemaPrefix == null) {
              columnSetAfterFilter.add(column);
              continue;
            }
            column.setPath(schemaPrefix + "." + column.getPath());
            boolean isMatch = patterns.isEmpty();
            for (String pathRegex : patterns) {
              if (Pattern.matches(StringUtils.reformatPath(pathRegex), column.getPath())) {
                isMatch = true;
                break;
              }
            }
            if (isMatch) {
              columnSetAfterFilter.add(column);
            }
          }
        }
      } catch (PhysicalException e) {
        return new TaskExecuteResult(e);
      }
    }

    int limit = showColumns.getLimit();
    int offset = showColumns.getOffset();
    if (limit == Integer.MAX_VALUE && offset == 0) {
      return new TaskExecuteResult(Column.toRowStream(columnSetAfterFilter));
    } else {
      // only need part of data.
      List<Column> tsList = new ArrayList<>();
      int cur = 0, size = columnSetAfterFilter.size();
      for (Iterator<Column> iter = columnSetAfterFilter.iterator(); iter.hasNext(); cur++) {
        if (cur >= size || cur - offset >= limit) {
          break;
        }
        Column ts = iter.next();
        if (cur >= offset) {
          tsList.add(ts);
        }
      }
      return new TaskExecuteResult(Column.toRowStream(tsList));
    }
  }

  private static Set<String> cutSchemaPrefix(String schemaPrefix, Set<String> patterns) {
    if (schemaPrefix == null) {
      if (patterns.isEmpty()) {
        return Collections.singleton("*");
      } else {
        return patterns;
      }
    }
    if (patterns.isEmpty()) {
      return Collections.singleton("*");
    }

    Set<String> patternsCutSchemaPrefix = new HashSet<>();
    for (String pattern : patterns) {
      Set<String> tmp = cutSchemaPrefix(schemaPrefix, pattern);
      if (tmp.contains("*")) {
        return Collections.singleton("*");
      }
      patternsCutSchemaPrefix.addAll(tmp);
    }
    return patternsCutSchemaPrefix;
  }

  private static Set<String> cutSchemaPrefix(String schemaPrefix, String pattern) {
    String[] prefixSplit = schemaPrefix.split("\\.");
    String[] patternSplit = pattern.split("\\.");
    int minLen = Math.min(prefixSplit.length, patternSplit.length);
    int index = 0;
    while (index < minLen && prefixSplit[index].equals(patternSplit[index])) {
      index++;
    }

    if (index == patternSplit.length) {
      return Collections.emptySet();
    }

    if (index == prefixSplit.length) {
      return Collections.singleton(joinWithDot(patternSplit, index));
    }

    if (!patternSplit[index].equals("*")) {
      return Collections.emptySet();
    }

    Set<String> target = new HashSet<>();

    // 将pattern的'*'视为部分匹配该前缀，即把'*'下推到数据源
    target.add(joinWithDot(patternSplit, index));

    if (index + 1 < patternSplit.length) {
      // 将pattern的'*'视为完全匹配该前缀，即不把'*'下推到数据源
      String patternRemain = joinWithDot(patternSplit, index + 1);
      target.add(patternRemain);

      for (int i = index + 1; i < prefixSplit.length; i++) {
        String prefixRemain = joinWithDot(prefixSplit, i);
        target.addAll(cutSchemaPrefix(prefixRemain, patternRemain));
      }
    }

    return target;
  }

  private static String joinWithDot(String[] strings, int begin) {
    if (begin >= strings.length) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = begin; i < strings.length; i++) {
      sb.append(strings[i]).append(".");
    }
    sb.setLength(sb.length() - 1);
    return sb.toString();
  }

  public void commit(List<StoragePhysicalTask> tasks) {
    for (StoragePhysicalTask task : tasks) {
      if (replicaDispatcher == null) {
        storageTaskQueues
            .get(task.getTargetFragment().getMasterStorageUnitId())
            .addTask(task); // 默认情况下，异步写备，查询只查主
      } else {
        storageTaskQueues
            .get(replicaDispatcher.chooseReplica(task))
            .addTask(task); // 在优化策略提供了选择器的情况下，利用选择器提供的结果
      }
    }
  }

  public void init(
      MemoryPhysicalTaskDispatcher memoryTaskExecutor, ReplicaDispatcher replicaDispatcher) {
    this.memoryTaskExecutor = memoryTaskExecutor;
    this.replicaDispatcher = replicaDispatcher;
  }

  public StorageManager getStorageManager() {
    return storageManager;
  }
}
