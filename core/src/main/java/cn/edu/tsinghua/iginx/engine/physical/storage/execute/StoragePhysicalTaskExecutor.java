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
package cn.edu.tsinghua.iginx.engine.physical.storage.execute;

import cn.edu.tsinghua.iginx.auth.SessionManager;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.exception.NonExecutablePhysicalTaskException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.TooManyPhysicalTasksException;
import cn.edu.tsinghua.iginx.engine.physical.exception.UnconnectedStorageException;
import cn.edu.tsinghua.iginx.engine.physical.exception.UnsupportedPhysicalTaskException;
import cn.edu.tsinghua.iginx.engine.physical.memory.MemoryPhysicalTaskDispatcher;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream.EmptyRowStream;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.ReplicaDispatcher;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.StorageManager;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.storage.execute.pushdown.strategy.PushDownStrategy;
import cn.edu.tsinghua.iginx.engine.physical.storage.execute.pushdown.strategy.PushDownStrategyFactory;
import cn.edu.tsinghua.iginx.engine.physical.storage.execute.stream.ShowColumnsRowStream;
import cn.edu.tsinghua.iginx.engine.physical.storage.queue.StoragePhysicalTaskQueue;
import cn.edu.tsinghua.iginx.engine.physical.task.*;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.MemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.shared.data.read.FetchMetricsRowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStreams;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.StorageUnitHook;
import cn.edu.tsinghua.iginx.migration.MigrationPhysicalExecutor;
import cn.edu.tsinghua.iginx.monitor.HotSpotMonitor;
import cn.edu.tsinghua.iginx.monitor.RequestsMonitor;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.*;
import java.util.concurrent.*;
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
            dispatcher.submit(() -> runDispatcherTask(id, storageId, isDummy));
            LOGGER.info("process for new storage unit finished!");
          }
        };
    StorageEngineChangeHook storageEngineChangeHook =
        (before, after) -> {
          if (before == null && after != null) { // 新增加存储，处理这种事件，其他事件暂时不处理
            if (after.getCreatedBy() != metaManager.getIginxId()) {
              storageManager.addStorage(after);
              metaManager.addStorageConnection(Collections.singletonList(after));
            }
          } else if (before != null && after == null) { // 删除引擎时，需要release（目前仅支持dummy & read only）
            try {
              if (!storageManager.releaseStorage(before)) {
                LOGGER.error(
                    "Fail to release deleted storage engine. Please look into server log.");
              }
              LOGGER.info("Release storage with id={} succeeded.", before.getId());
            } catch (PhysicalException e) {
              LOGGER.error(
                  "unexpected exception during in releasing storage engine, please contact developer to check: ",
                  e);
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

  private void runDispatcherTask(String id, long storageId, boolean isDummy) {
    try {
      StoragePhysicalTaskQueue taskQueue = storageTaskQueues.get(id);
      Pair<IStorage, ThreadPoolExecutor> p = storageManager.getStorage(storageId);
      boolean hasInitialized = p != null;
      while (true) {
        StoragePhysicalTask task = taskQueue.getTask();
        task.setStorageUnit(id);
        task.setDummyStorageUnit(isDummy);

        List<Operator> operators = task.getOperators();
        if (operators.isEmpty()) {
          setResult(task, new TaskExecuteResult(new NonExecutablePhysicalTaskException()));
          continue;
        }
        Operator op = operators.get(0);

        String storageUnit = task.getStorageUnit();
        StorageUnitMeta masterStorageUnit = task.getTargetFragment().getMasterStorageUnit();
        List<StorageUnitMeta> replicas = new ArrayList<>(masterStorageUnit.getReplicas());
        replicas.add(masterStorageUnit);
        switch (op.getType()) {
          case Project:
            if (!hasInitialized) {
              p = reconnectStorage(storageId);
              hasInitialized = p != null;
            }

            // 重连失败，尝试更换为副本中的数据
            if (!hasInitialized) {
              boolean foundAvailableStorage = false;
              for (StorageUnitMeta replica : replicas) {
                if (replica.getId().equals(storageUnit)) {
                  continue;
                }
                long replicaId = replica.getStorageEngineId();
                if (metaManager.isStorageEngineInConnection(replicaId)
                    || reconnectStorage(replicaId) != null) {
                  foundAvailableStorage = true;
                  storageTaskQueues.get(replica.getId()).addTask(task);
                  break;
                }
              }

              // 未找到可用的存储单元，查询失败
              if (!foundAvailableStorage) {
                List<Long> storageIds =
                    replicas.stream()
                        .map(StorageUnitMeta::getStorageEngineId)
                        .collect(Collectors.toList());
                setResult(task, new TaskExecuteResult(new UnconnectedStorageException(storageIds)));
                executeParentTaskIfNeed(task);
              }
              continue;
            }

            break;
          case Insert:
          case Delete:
            if (!hasInitialized) {
              p = reconnectStorage(storageId);
              if (p == null) { // 重连失败，则写入/删除失败
                setResult(
                    task,
                    new TaskExecuteResult(
                        new UnconnectedStorageException(op.getType(), storageId)));
                failToExecuteInsertOrDelete(task, replicas);
                continue;
              }
              hasInitialized = true;
            }

            boolean existUnconnectedStorage =
                replicas.stream()
                    .anyMatch(
                        replica -> {
                          long replicaId = replica.getStorageEngineId();
                          // 写入/删除涉及的副本里有无法连接的存储且重连失败，则写入/删除失败
                          boolean unconnected =
                              !metaManager.isStorageEngineInConnection(replicaId)
                                  && reconnectStorage(replicaId) == null;
                          if (unconnected) {
                            setResult(
                                task,
                                new TaskExecuteResult(
                                    new UnconnectedStorageException(op.getType(), replicaId)));
                          }
                          return unconnected;
                        });
            if (existUnconnectedStorage) {
              failToExecuteInsertOrDelete(task, replicas);
              continue;
            }

            break;
          default:
            setResult(task, new TaskExecuteResult(new UnsupportedPhysicalTaskException()));
            continue;
        }

        if (p.v.getQueue().size() > maxCachedPhysicalTaskPerStorage) {
          setResult(task, new TaskExecuteResult(new TooManyPhysicalTasksException(storageId)));
          continue;
        }

        if (isCancelled(task.getContext().getSessionId())) {
          LOGGER.warn(
              "StoragePhysicalTask[sessionId={}] is cancelled.", task.getContext().getSessionId());
          continue;
        }

        Pair<IStorage, ThreadPoolExecutor> pair = p;
        pair.v.submit(() -> executeStorageTask(task, storageUnit, op, operators, pair));
      }
    } catch (Exception e) {
      LOGGER.error(
          "unexpected exception during dispatcher storage task, please contact developer to check: ",
          e);
    }
  }

  private void executeStorageTask(
      StoragePhysicalTask task,
      String storageUnit,
      Operator op,
      List<Operator> operators,
      Pair<IStorage, ThreadPoolExecutor> pair) {
    TaskExecuteResult result = null;
    long taskId = System.nanoTime();
    long startTime = System.nanoTime();
    try {
      FragmentMeta fragmentMeta = task.getTargetFragment();
      boolean isDummyStorageUnit = task.isDummyStorageUnit();
      DataArea dataArea = new DataArea(storageUnit, fragmentMeta.getKeyInterval());
      switch (op.getType()) {
        case Project:
          Project project = (Project) op;
          if (project.getPatterns().isEmpty()) {
            result = new TaskExecuteResult(new EmptyRowStream());
            break;
          }
          PushDownStrategy strategy =
              PushDownStrategyFactory.getStrategy(operators, pair.k, dataArea, isDummyStorageUnit);
          result =
              strategy.execute(
                  project, operators, dataArea, pair.k, isDummyStorageUnit, task.getContext());
          break;
        case Insert:
          result = pair.k.executeInsert((Insert) op, dataArea);
          break;
        case Delete:
          result = pair.k.executeDelete((Delete) op, dataArea);
          break;
        default:
          result = new TaskExecuteResult(new UnsupportedPhysicalTaskException());
      }
    } catch (Exception e) {
      LOGGER.error("execute task error: ", e);
      result = new TaskExecuteResult(new PhysicalException(e));
    }

    try {
      HotSpotMonitor.getInstance()
          .recordAfter(taskId, task.getTargetFragment(), task.getOperators().get(0).getType());
      RequestsMonitor.getInstance().record(task.getTargetFragment(), task.getOperators().get(0));
    } catch (Exception e) {
      LOGGER.error("Monitor catch error:", e);
    }

    long span = System.nanoTime() - startTime;
    task.getMetrics().accumulateCpuTime(span);
    setResult(task, result);

    executeParentTaskIfNeed(task);

    if (task.isNeedBroadcasting()) { // 需要传播
      PhysicalException exception = result.getException();
      if (exception != null) {
        LOGGER.error(
            "task {} will not broadcasting to replicas for the sake of exception", task, exception);
        setResult(task, new TaskExecuteResult(exception));
      } else {
        StorageUnitMeta masterStorageUnit = task.getTargetFragment().getMasterStorageUnit();
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
              new StoragePhysicalTask(task.getOperators(), false, false, task.getContext());
          storageTaskQueues.get(replicaId).addTask(replicaTask);
          LOGGER.info("broadcasting task {} to {}", task, replicaId);
        }
      }
    }
  }

  private MemoryPhysicalTask executeParentTaskIfNeed(StoragePhysicalTask task) {
    if (task.getFollowerTask() != null && task.isSync()) { // 只有同步任务才会影响后续任务的执行
      MemoryPhysicalTask followerTask = (MemoryPhysicalTask) task.getFollowerTask();
      boolean isFollowerTaskReady = followerTask.notifyParentReady();
      if (isFollowerTaskReady) {
        memoryTaskExecutor.addMemoryTask(followerTask);
      }
      return followerTask;
    }
    return null;
  }

  private void failToExecuteInsertOrDelete(
      StoragePhysicalTask task, List<StorageUnitMeta> replicas) {
    MemoryPhysicalTask followerTask = executeParentTaskIfNeed(task);
    for (StorageUnitMeta replica : replicas) {
      if (replica.getId().equals(task.getStorageUnit())) {
        continue;
      }
      followerTask.notifyParentReady();
    }
  }

  private Pair<IStorage, ThreadPoolExecutor> reconnectStorage(long id) {
    if (metaManager.isStorageEngineInConnection(id)) {
      return storageManager.getStorage(id);
    }

    StorageEngineMeta meta = metaManager.getStorageEngine(id);
    IStorage storage = StorageManager.initStorageInstance(meta);
    if (storage != null && storageManager.addStorage(meta, storage)) {
      metaManager.addStorageConnection(Collections.singletonList(meta));
      LOGGER.info("Successfully reconnect to storage {}.", meta);
      return storageManager.getStorage(id);
    }
    return null;
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

  public void executeGlobalTask(GlobalPhysicalTask task) {
    switch (task.getOperator().getType()) {
      case ShowColumns:
        long startTime = System.nanoTime();
        TaskExecuteResult result = null;
        try {
          result = executeShowColumns((ShowColumns) task.getOperator());
        } catch (PhysicalException e) {
          LOGGER.error("unexpected exception during execute show columns", e);
        }
        long span = System.nanoTime() - startTime;
        task.getMetrics().accumulateCpuTime(span);
        setResult(task, result);
        break;
      case Migration:
        try {
          RowStream stream =
              MigrationPhysicalExecutor.getInstance()
                  .execute(task.getContext(), (Migration) task.getOperator(), this);
          setResult(task, new TaskExecuteResult(stream));
        } catch (PhysicalException e) {
          setResult(task, new TaskExecuteResult(e));
        }
        break;
      default:
        throw new UnsupportedOperationException("unknown op: " + task.getOperator().getType());
    }
    if (task.getFollowerTask() != null) {
      MemoryPhysicalTask<?> followerTask = (MemoryPhysicalTask<?>) task.getFollowerTask();
      boolean isFollowerTaskReady = followerTask.notifyParentReady();
      if (isFollowerTaskReady) {
        memoryTaskExecutor.addMemoryTask(followerTask);
      }
    }
  }

  public TaskExecuteResult executeShowColumns(ShowColumns showColumns) throws PhysicalException {
    List<StorageEngineMeta> storageEngineList = metaManager.getStorageEngineList();
    List<Flowable<Column>> allStreams = new ArrayList<>();

    for (StorageEngineMeta meta : storageEngineList) {
      Pair<IStorage, ThreadPoolExecutor> pair = storageManager.getStorage(meta.getId());
      if (pair == null || pair.k == null) {
        continue;
      }
      IStorage storage = pair.k;
      ThreadPoolExecutor executor = pair.v;

      Flowable<Column> stream =
          getColumnsFromStorage(showColumns, meta, storage).subscribeOn(Schedulers.from(executor));

      allStreams.add(stream);
    }

    Flowable<Column> columnStream =
        Flowable.merge(allStreams, 20).distinct(Column::getPhysicalPath);

    int limit = showColumns.getLimit();
    int offset = showColumns.getOffset();
    if (offset > 0) {
      columnStream = columnStream.skip(offset);
    }
    if (limit < Integer.MAX_VALUE) {
      columnStream = columnStream.take(limit);
    }
    RowStream stream = new ShowColumnsRowStream(columnStream.blockingIterable().iterator());
    return new TaskExecuteResult(stream);
  }

  private Flowable<Column> getColumnsFromStorage(
      ShowColumns showColumns, StorageEngineMeta meta, IStorage storage) throws PhysicalException {
    Set<String> patterns = showColumns.getPathRegexSet();
    String schemaPrefix = meta.getSchemaPrefix();
    // schemaPrefix是在IGinX中定义的，数据源的路径中没有该前缀，因此需要剪掉patterns中前缀是schemaPrefix的部分
    patterns = StringUtils.cutSchemaPrefix(schemaPrefix, patterns);
    if (patterns.isEmpty()) {
      return Flowable.empty();
    }
    // 求patterns与dataPrefix的交集
    patterns = StringUtils.intersectDataPrefix(meta.getDataPrefix(), patterns);
    if (patterns.isEmpty()) {
      return Flowable.empty();
    }
    if (patterns.contains("*")) {
      patterns = Collections.emptySet();
    }
    Flowable<Column> stream = storage.getColumns(patterns, showColumns.getTagFilter());

    // 列名前加上schemaPrefix
    if (schemaPrefix != null) {
      return stream.map(
          col -> {
            col.setPath(schemaPrefix + "." + col.getPath());
            return col;
          });
    } else {
      return stream;
    }
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

  private static void setResult(PhysicalTask<RowStream> task, TaskExecuteResult result) {
    if (result != null) {
      if (result.getException() != null) {
        task.setResult(new TaskResult<>(result.getException()));
        return;
      }
      RowStream sourceRowStream = result.getRowStream();
      if (sourceRowStream != null) {
        RowStream rowStream =
            new FetchMetricsRowStream(
                sourceRowStream, task.getMetrics(), task.getContext().getBatchRowCount());
        task.setResult(new TaskResult<>(rowStream));
        return;
      }
    }
    task.setResult(new TaskResult<>(RowStreams.empty()));
  }
}
