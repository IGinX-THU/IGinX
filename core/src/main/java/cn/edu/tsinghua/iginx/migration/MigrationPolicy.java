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
package cn.edu.tsinghua.iginx.migration;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Migration;
import cn.edu.tsinghua.iginx.engine.shared.operator.ShowColumns;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.metadata.exception.MetaStorageException;
import cn.edu.tsinghua.iginx.migration.recover.MigrationExecuteTask;
import cn.edu.tsinghua.iginx.migration.recover.MigrationExecuteType;
import cn.edu.tsinghua.iginx.migration.recover.MigrationLogger;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;

public abstract class MigrationPolicy {

  protected ExecutorService executor;

  protected static final Config config = ConfigDescriptor.getInstance().getConfig();

  private Logger logger;

  private final IPolicy policy =
      PolicyManager.getInstance()
          .getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName());
  private final int maxReshardFragmentsNum = config.getMaxReshardFragmentsNum();
  private static final double maxTimeseriesLoadBalanceThreshold =
      ConfigDescriptor.getInstance().getConfig().getMaxTimeseriesLoadBalanceThreshold();

  private MigrationLogger migrationLogger;

  private static final PhysicalEngine physicalEngine = PhysicalEngineImpl.getInstance();

  public MigrationPolicy(Logger logger) {
    this.logger = logger;
  }

  public void setMigrationLogger(MigrationLogger migrationLogger) {
    this.migrationLogger = migrationLogger;
  }

  public abstract void migrate(
      List<MigrationTask> migrationTasks,
      Map<Long, List<FragmentMeta>> nodeFragmentMap,
      Map<FragmentMeta, Long> fragmentWriteLoadMap,
      Map<FragmentMeta, Long> fragmentReadLoadMap);

  /** 可定制化副本 */
  public void reshardByCustomizableReplica(
      FragmentMeta fragmentMeta,
      Map<String, Long> timeseriesLoadMap,
      Set<String> overLoadTimeseries,
      long totalLoad,
      long points,
      Map<Long, Long> storageHeat)
      throws MetaStorageException {
    try {
      migrationLogger.logMigrationExecuteTaskStart(
          new MigrationExecuteTask(
              fragmentMeta,
              fragmentMeta.getMasterStorageUnitId(),
              0L,
              0L,
              MigrationExecuteType.RESHARD_TIME_SERIES));

      List<String> timeseries = new ArrayList<>(timeseriesLoadMap.keySet());
      String currStartTimeseries = fragmentMeta.getColumnsInterval().getStartColumn();
      long currLoad = 0L;
      String endTimeseries = fragmentMeta.getColumnsInterval().getEndColumn();
      long startTime = fragmentMeta.getKeyInterval().getStartKey();
      long endTime = fragmentMeta.getKeyInterval().getEndKey();
      StorageUnitMeta storageUnitMeta = fragmentMeta.getMasterStorageUnit();
      List<FragmentMeta> fakedFragmentMetas = new ArrayList<>();
      List<Long> fakedFragmentMetaLoads = new ArrayList<>();
      // 按超负载序列进行分片
      for (int i = 0; i < timeseries.size(); i++) {
        if (overLoadTimeseries.contains(timeseries.get(i))) {
          fakedFragmentMetas.add(
              new FragmentMeta(
                  currStartTimeseries, timeseries.get(i), startTime, endTime, storageUnitMeta));
          fakedFragmentMetaLoads.add(currLoad);
          currLoad = 0;
          if (i != (timeseries.size() - 1)) {
            fakedFragmentMetas.add(
                new FragmentMeta(
                    timeseries.get(i), timeseries.get(i + 1), startTime, endTime, storageUnitMeta));
            fakedFragmentMetaLoads.add(timeseriesLoadMap.get(timeseries.get(i)));
            currStartTimeseries = timeseries.get(i + 1);
          } else {
            currStartTimeseries = timeseries.get(i);
            currLoad = timeseriesLoadMap.get(timeseries.get(i));
          }
        }
        currLoad += timeseriesLoadMap.get(timeseries.get(i));
      }
      fakedFragmentMetas.add(
          new FragmentMeta(
              currStartTimeseries, endTimeseries, startTime, endTime, storageUnitMeta));
      fakedFragmentMetaLoads.add(currLoad);

      // 模拟进行时间序列分片
      while (fakedFragmentMetas.size() > maxReshardFragmentsNum) {
        double currAverageLoad = totalLoad * 1.0 / fakedFragmentMetaLoads.size();
        boolean canMergeFragments = false;
        for (int i = 0; i < fakedFragmentMetaLoads.size(); i++) {
          FragmentMeta currFragmentMeta = fakedFragmentMetas.get(i);
          // 合并时间序列分片
          if (fakedFragmentMetaLoads.get(i)
                  <= currAverageLoad * (1 + maxTimeseriesLoadBalanceThreshold)
              && currFragmentMeta
                  .getColumnsInterval()
                  .getStartColumn()
                  .equals(currFragmentMeta.getColumnsInterval().getEndColumn())) {

            // 与他最近的负载最低的时间分区进行合并
            if (i == (fakedFragmentMetaLoads.size() - 1)
                || fakedFragmentMetaLoads.get(i + 1) > fakedFragmentMetaLoads.get(i - 1)) {
              FragmentMeta toMergeFragmentMeta = fakedFragmentMetas.get(i - 1);
              toMergeFragmentMeta
                  .getColumnsInterval()
                  .setEndColumn(fakedFragmentMetas.get(i).getColumnsInterval().getEndColumn());
              fakedFragmentMetas.remove(i);
              fakedFragmentMetaLoads.set(
                  i - 1, fakedFragmentMetaLoads.get(i - 1) + fakedFragmentMetaLoads.get(i));
              fakedFragmentMetaLoads.remove(i);
            } else if (fakedFragmentMetaLoads.get(i + 1) <= fakedFragmentMetaLoads.get(i - 1)) {
              FragmentMeta toMergeFragmentMeta = fakedFragmentMetas.get(i);
              toMergeFragmentMeta
                  .getColumnsInterval()
                  .setEndColumn(fakedFragmentMetas.get(i + 1).getColumnsInterval().getEndColumn());
              fakedFragmentMetas.remove(i + 1);
              fakedFragmentMetaLoads.set(
                  i, fakedFragmentMetaLoads.get(i) + fakedFragmentMetaLoads.get(i + 1));
              fakedFragmentMetaLoads.remove(i + 1);
            }

            // 需要合并
            canMergeFragments = true;
          }
        }
        // 合并最小分片
        if (canMergeFragments) {
          long maxTwoFragmentLoads = 0L;
          int startIndex = 0;
          for (int i = 0; i < fakedFragmentMetaLoads.size(); i++) {
            if (i < fakedFragmentMetaLoads.size() - 1) {
              long currTwoFragmentLoad =
                  fakedFragmentMetaLoads.get(i) + fakedFragmentMetaLoads.get(i + 1);
              if (currTwoFragmentLoad > maxTwoFragmentLoads) {
                maxTwoFragmentLoads = currTwoFragmentLoad;
                startIndex = i;
              }
            }
          }
          FragmentMeta toMergeFragmentMeta = fakedFragmentMetas.get(startIndex);
          toMergeFragmentMeta
              .getColumnsInterval()
              .setEndColumn(
                  fakedFragmentMetas.get(startIndex + 1).getColumnsInterval().getEndColumn());
          fakedFragmentMetas.remove(startIndex + 1);
          fakedFragmentMetaLoads.set(
              startIndex,
              fakedFragmentMetaLoads.get(startIndex) + fakedFragmentMetaLoads.get(startIndex + 1));
          fakedFragmentMetaLoads.remove(startIndex + 1);
        }
      }

      // 给每个节点负载做排序以方便后续迁移
      // 去掉本身节点
      storageHeat.remove(fragmentMeta.getMasterStorageUnit().getStorageEngineId());
      List<Entry<Long, Long>> storageHeatEntryList = new ArrayList<>(storageHeat.entrySet());
      storageHeatEntryList.sort(Entry.comparingByValue());

      // 开始实际切分片
      double currAverageLoad = totalLoad * 1.0 / fakedFragmentMetaLoads.size();
      ColumnsInterval sourceColumnsInterval =
          new ColumnsInterval(
              fragmentMeta.getColumnsInterval().getStartColumn(),
              fragmentMeta.getColumnsInterval().getEndColumn());
      for (int i = 0; i < fakedFragmentMetas.size(); i++) {
        FragmentMeta targetFragmentMeta = fakedFragmentMetas.get(i);
        if (i == 0) {
          DefaultMetaManager.getInstance()
              .endFragmentByColumnsInterval(
                  fragmentMeta, targetFragmentMeta.getColumnsInterval().getEndColumn());
          DefaultMetaManager.getInstance()
              .updateFragmentByColumnsInterval(sourceColumnsInterval, fragmentMeta);
        } else {
          FragmentMeta newFragment =
              new FragmentMeta(
                  targetFragmentMeta.getColumnsInterval().getStartColumn(),
                  targetFragmentMeta.getColumnsInterval().getEndColumn(),
                  fragmentMeta.getKeyInterval().getStartKey(),
                  fragmentMeta.getKeyInterval().getEndKey(),
                  fragmentMeta.getMasterStorageUnit());
          DefaultMetaManager.getInstance().addFragment(newFragment);
        }
        // 开始拷贝副本，有一个先决条件，时间分区必须为闭区间，即只有查询请求，在之后不会被再写入数据，也不会被拆分读写
        if (fakedFragmentMetaLoads.get(i)
            >= currAverageLoad * (1 + maxTimeseriesLoadBalanceThreshold)) {
          int replicas = (int) (fakedFragmentMetaLoads.get(i) / currAverageLoad);
          for (int num = 1; num < replicas; num++) {
            long targetStorageId =
                storageHeatEntryList.get(num % storageHeatEntryList.size()).getKey();
            StorageUnitMeta newStorageUnitMeta =
                DefaultMetaManager.getInstance()
                    .generateNewStorageUnitMetaByFragment(fragmentMeta, targetStorageId);
            FragmentMeta newFragment =
                new FragmentMeta(
                    targetFragmentMeta.getColumnsInterval().getStartColumn(),
                    targetFragmentMeta.getColumnsInterval().getEndColumn(),
                    fragmentMeta.getKeyInterval().getStartKey(),
                    fragmentMeta.getKeyInterval().getEndKey(),
                    newStorageUnitMeta);
            DefaultMetaManager.getInstance().addFragment(newFragment);
          }
        }
      }

    } finally {
      migrationLogger.logMigrationExecuteTaskEnd();
    }
  }

  /** 在时间序列层面将分片在同一个du下分为两块（未知时间序列, 写入场景） */
  public void reshardWriteByTimeseries(FragmentMeta fragmentMeta, long points)
      throws PhysicalException {
    // 分区不存在直接返回
    //    if (!DefaultMetaManager.getInstance()
    //        .checkFragmentExistenceByTimeInterval(fragmentMeta.getTsInterval())) {
    //      return;
    //    }
    try {
      logger.info("start to reshard timeseries by write");
      migrationLogger.logMigrationExecuteTaskStart(
          new MigrationExecuteTask(
              fragmentMeta,
              fragmentMeta.getMasterStorageUnitId(),
              0L,
              0L,
              MigrationExecuteType.RESHARD_TIME_SERIES));

      Set<String> pathRegexSet = new HashSet<>();
      pathRegexSet.add(fragmentMeta.getMasterStorageUnitId());
      ShowColumns showColumns =
          new ShowColumns(new GlobalSource(), pathRegexSet, null, Integer.MAX_VALUE, 0);
      RowStream rowStream = physicalEngine.executeAsRowStream(new RequestContext(), showColumns);
      SortedSet<String> pathSet = new TreeSet<>();
      while (rowStream.hasNext()) {
        Row row = rowStream.next();
        String timeSeries = new String((byte[]) row.getValue(0));
        if (fragmentMeta.getColumnsInterval().isContain(timeSeries)) {
          pathSet.add(timeSeries);
        }
      }
      logger.info("start to add new fragment");
      String middleTimeseries = new ArrayList<>(pathSet).get(pathSet.size() / 2);
      logger.info("timeseries split middleTimeseries={}", middleTimeseries);
      ColumnsInterval sourceColumnsInterval =
          new ColumnsInterval(
              fragmentMeta.getColumnsInterval().getStartColumn(),
              fragmentMeta.getColumnsInterval().getEndColumn());
      FragmentMeta newFragment =
          new FragmentMeta(
              middleTimeseries,
              sourceColumnsInterval.getEndColumn(),
              fragmentMeta.getKeyInterval().getStartKey(),
              fragmentMeta.getKeyInterval().getEndKey(),
              fragmentMeta.getMasterStorageUnit());
      logger.info("timeseries split new fragment={}", newFragment);
      DefaultMetaManager.getInstance().addFragment(newFragment);
      logger.info("start to add old fragment");
      DefaultMetaManager.getInstance().endFragmentByColumnsInterval(fragmentMeta, middleTimeseries);
    } finally {
      migrationLogger.logMigrationExecuteTaskEnd();
    }
  }

  /** 在时间序列层面将分片在同一个du下分为两块（已知时间序列） */
  public void reshardQueryByTimeseries(
      FragmentMeta fragmentMeta, Map<String, Long> timeseriesLoadMap) {
    try {
      migrationLogger.logMigrationExecuteTaskStart(
          new MigrationExecuteTask(
              fragmentMeta,
              fragmentMeta.getMasterStorageUnitId(),
              0L,
              0L,
              MigrationExecuteType.RESHARD_TIME_SERIES));
      long totalLoad = 0L;
      for (Entry<String, Long> timeseriesLoadEntry : timeseriesLoadMap.entrySet()) {
        totalLoad += timeseriesLoadEntry.getValue();
      }
      String middleTimeseries = null;
      long currLoad = 0L;
      for (Entry<String, Long> timeseriesLoadEntry : timeseriesLoadMap.entrySet()) {
        currLoad += timeseriesLoadEntry.getValue();
        if (currLoad >= totalLoad / 2) {
          middleTimeseries = timeseriesLoadEntry.getKey();
          break;
        }
      }

      ColumnsInterval sourceColumnsInterval =
          new ColumnsInterval(
              fragmentMeta.getColumnsInterval().getStartColumn(),
              fragmentMeta.getColumnsInterval().getEndColumn());
      FragmentMeta newFragment =
          new FragmentMeta(
              middleTimeseries,
              sourceColumnsInterval.getEndColumn(),
              fragmentMeta.getKeyInterval().getStartKey(),
              fragmentMeta.getKeyInterval().getEndKey(),
              fragmentMeta.getMasterStorageUnit());
      DefaultMetaManager.getInstance().addFragment(newFragment);
      DefaultMetaManager.getInstance().endFragmentByColumnsInterval(fragmentMeta, middleTimeseries);
      DefaultMetaManager.getInstance()
          .updateFragmentByColumnsInterval(sourceColumnsInterval, fragmentMeta);
    } finally {
      migrationLogger.logMigrationExecuteTaskEnd();
    }
  }

  public void interrupt() {
    executor.shutdown();
  }

  protected boolean canExecuteTargetMigrationTask(
      MigrationTask migrationTask, Map<Long, Long> nodeLoadMap) {
    //    long currTargetNodeLoad = nodeLoadMap.getOrDefault(migrationTask.getTargetStorageId(),
    // 0L);
    //    LOGGER.error("currTargetNodeLoad = {}", currTargetNodeLoad);
    //    LOGGER.error("migrationTask.getLoad() = {}", migrationTask.getLoad());
    //    LOGGER.error("config.getMaxLoadThreshold() = {}", config.getMaxLoadThreshold());
    return true;
  }

  protected boolean isAllQueueEmpty(List<Queue<MigrationTask>> migrationTaskQueueList) {
    for (Queue<MigrationTask> migrationTaskQueue : migrationTaskQueueList) {
      if (!migrationTaskQueue.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  protected void sortQueueListByFirstItem(List<Queue<MigrationTask>> migrationTaskQueue) {
    migrationTaskQueue.sort(
        (o1, o2) -> {
          MigrationTask migrationTask1 = o1.peek();
          MigrationTask migrationTask2 = o2.peek();
          if (migrationTask1 == null || migrationTask2 == null) {
            return 1;
          } else {
            return (int) (migrationTask2.getPriorityScore() - migrationTask1.getPriorityScore());
          }
        });
  }

  protected Map<Long, Long> calculateNodeLoadMap(
      Map<Long, List<FragmentMeta>> nodeFragmentMap,
      Map<FragmentMeta, Long> fragmentWriteLoadMap,
      Map<FragmentMeta, Long> fragmentReadLoadMap) {
    Map<Long, Long> nodeLoadMap = new HashMap<>();
    for (Entry<Long, List<FragmentMeta>> nodeFragmentEntry : nodeFragmentMap.entrySet()) {
      List<FragmentMeta> fragmentMetas = nodeFragmentEntry.getValue();
      for (FragmentMeta fragmentMeta : fragmentMetas) {
        nodeLoadMap.put(
            nodeFragmentEntry.getKey(),
            fragmentWriteLoadMap.getOrDefault(fragmentMeta, 0L)
                + fragmentReadLoadMap.getOrDefault(fragmentMeta, 0L));
      }
    }
    return nodeLoadMap;
  }

  protected synchronized void executeOneRoundMigration(
      List<Queue<MigrationTask>> migrationTaskQueueList, Map<Long, Long> nodeLoadMap) {
    for (Queue<MigrationTask> migrationTaskQueue : migrationTaskQueueList) {
      MigrationTask migrationTask = migrationTaskQueue.peek();
      // 根据负载判断是否能进行该任务
      if (migrationTask != null && canExecuteTargetMigrationTask(migrationTask, nodeLoadMap)) {
        migrationTaskQueue.poll();
        this.executor.submit(
            () -> {
              this.logger.info("start migration: {}", migrationTask);
              // 异步执行耗时的操作
              if (migrationTask.getMigrationType() == MigrationType.QUERY) {
                // 如果之前没切过分区，需要优先切一下分区
                if (migrationTask.getFragmentMeta().getKeyInterval().getEndKey()
                    == Long.MAX_VALUE) {
                  this.logger.error("start to reshard query data: {}", migrationTask);
                  FragmentMeta fragmentMeta =
                      reshardFragment(
                          migrationTask.getSourceStorageId(),
                          migrationTask.getTargetStorageId(),
                          migrationTask.getFragmentMeta());
                  migrationTask.setFragmentMeta(fragmentMeta);
                }
                this.logger.error("start to migrate data: {}", migrationTask);
                migrateData(
                    migrationTask.getSourceStorageId(),
                    migrationTask.getTargetStorageId(),
                    migrationTask.getFragmentMeta());
              } else {
                this.logger.error("start to migrate write data: {}", migrationTask);
                reshardFragment(
                    migrationTask.getSourceStorageId(),
                    migrationTask.getTargetStorageId(),
                    migrationTask.getFragmentMeta());
              }
              this.logger.error(
                  "complete one migration task from {} to {} with load: {}, size: {}, type: {}",
                  migrationTask.getSourceStorageId(),
                  migrationTask.getTargetStorageId(),
                  migrationTask.getLoad(),
                  migrationTask.getSize(),
                  migrationTask.getMigrationType());
              // 执行下一轮判断
              while (!isAllQueueEmpty(migrationTaskQueueList)) {
                executeOneRoundMigration(migrationTaskQueueList, nodeLoadMap);
              }
            });
      }
    }
    sortQueueListByFirstItem(migrationTaskQueueList);
  }

  private void migrateData(long sourceStorageId, long targetStorageId, FragmentMeta fragmentMeta) {
    try {
      // 在目标节点创建新du
      StorageUnitMeta storageUnitMeta;
      try {
        storageUnitMeta =
            DefaultMetaManager.getInstance()
                .generateNewStorageUnitMetaByFragment(fragmentMeta, targetStorageId);
      } catch (MetaStorageException e) {
        logger.error("cannot create storage unit in target storage engine", e);
        throw new PhysicalException(e);
      }
      migrationLogger.logMigrationExecuteTaskStart(
          new MigrationExecuteTask(
              fragmentMeta,
              storageUnitMeta.getId(),
              sourceStorageId,
              targetStorageId,
              MigrationExecuteType.MIGRATION));

      Set<String> pathRegexSet = new HashSet<>();
      pathRegexSet.add(fragmentMeta.getMasterStorageUnitId());
      ShowColumns showColumns =
          new ShowColumns(new GlobalSource(), pathRegexSet, null, Integer.MAX_VALUE, 0);
      RowStream rowStream = physicalEngine.executeAsRowStream(new RequestContext(), showColumns);
      SortedSet<String> pathSet = new TreeSet<>();
      rowStream
          .getHeader()
          .getFields()
          .forEach(
              field -> {
                String timeSeries = field.getName();
                if (fragmentMeta.getColumnsInterval().isContain(timeSeries)) {
                  pathSet.add(timeSeries);
                }
              });
      // 开始迁移数据
      Migration migration =
          new Migration(
              new GlobalSource(), fragmentMeta, new ArrayList<>(pathSet), storageUnitMeta);
      physicalEngine.execute(new RequestContext(), migration);
      // 迁移完开始删除原数据

      List<String> paths = new ArrayList<>();
      paths.add(fragmentMeta.getMasterStorageUnitId() + "*");
      List<KeyRange> keyRanges = new ArrayList<>();
      keyRanges.add(
          new KeyRange(
              fragmentMeta.getKeyInterval().getStartKey(),
              true,
              fragmentMeta.getKeyInterval().getEndKey(),
              false));
      Delete delete = new Delete(new FragmentSource(fragmentMeta), keyRanges, paths, null);
      physicalEngine.execute(new RequestContext(), delete);
    } catch (Exception e) {
      logger.error(
          "encounter error when migrate data from {} to {} ", sourceStorageId, targetStorageId, e);
    } finally {
      migrationLogger.logMigrationExecuteTaskEnd();
    }
  }

  public boolean migrationData(String sourceStorageUnitId, String targetStorageUnitId) {
    try {
      List<FragmentMeta> fragmentMetas =
          DefaultMetaManager.getInstance().getFragmentsByStorageUnit(sourceStorageUnitId);

      Set<String> pathRegexSet = new HashSet<>();
      ShowColumns showColumns =
          new ShowColumns(new GlobalSource(), pathRegexSet, null, Integer.MAX_VALUE, 0);
      RowStream rowStream = physicalEngine.executeAsRowStream(new RequestContext(), showColumns);
      SortedSet<String> pathSet = new TreeSet<>();
      while (rowStream.hasNext()) {
        Row row = rowStream.next();
        String timeSeries = new String((byte[]) row.getValue(0));
        if (timeSeries.contains("{") && timeSeries.contains("}")) {
          timeSeries = timeSeries.split("\\{")[0];
        }
        logger.info("[migrationData] need migration path: {}", timeSeries);
        for (FragmentMeta fragmentMeta : fragmentMetas) {
          if (fragmentMeta.getColumnsInterval().isContain(timeSeries)) {
            pathSet.add(timeSeries);
            logger.info("[migrationData] path {} belong to {}", timeSeries, fragmentMeta);
          }
        }
      }
      StorageUnitMeta targetStorageUnit =
          DefaultMetaManager.getInstance().getStorageUnit(targetStorageUnitId);
      // 开始迁移数据
      for (FragmentMeta fragmentMeta : fragmentMetas) {
        Migration migration =
            new Migration(
                new GlobalSource(), fragmentMeta, new ArrayList<>(pathSet), targetStorageUnit);
        physicalEngine.execute(new RequestContext(), migration);
      }
      return true;
    } catch (Exception e) {
      logger.error(
          "encounter error when migrate data from {} to {} ",
          sourceStorageUnitId,
          targetStorageUnitId,
          e);
    }
    return false;
  }

  private FragmentMeta reshardFragment(
      long sourceStorageId, long targetStorageId, FragmentMeta fragmentMeta) {
    try {
      migrationLogger.logMigrationExecuteTaskStart(
          new MigrationExecuteTask(
              fragmentMeta,
              fragmentMeta.getMasterStorageUnitId(),
              sourceStorageId,
              targetStorageId,
              MigrationExecuteType.RESHARD_TIME));
      // [startTime, +∞) & (startPath, endPath)
      ColumnsInterval columnsInterval = fragmentMeta.getColumnsInterval();
      KeyInterval keyInterval = fragmentMeta.getKeyInterval();
      List<Long> storageEngineList = new ArrayList<>();
      storageEngineList.add(targetStorageId);

      // 排除乱序写入问题
      if (keyInterval.getEndKey() == Long.MAX_VALUE) {
        operateTaskAndRequest(sourceStorageId, targetStorageId, fragmentMeta);
        Pair<FragmentMeta, StorageUnitMeta> fragmentMetaStorageUnitMetaPair =
            policy.generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
                columnsInterval.getStartColumn(),
                columnsInterval.getEndColumn(),
                DefaultMetaManager.getInstance().getMaxActiveEndKey(),
                Long.MAX_VALUE,
                storageEngineList);
        logger.info("start to splitFragmentAndStorageUnit");
        return DefaultMetaManager.getInstance()
            .splitFragmentAndStorageUnit(
                fragmentMetaStorageUnitMetaPair.getV(),
                fragmentMetaStorageUnitMetaPair.getK(),
                fragmentMeta);
      }
      return null;
    } finally {
      migrationLogger.logMigrationExecuteTaskEnd();
    }
  }

  private void operateTaskAndRequest(
      long sourceStorageId, long targetStorageId, FragmentMeta fragmentMeta) {
    // TODO 暂时先不管迁移过程中的请求问题
  }
}
