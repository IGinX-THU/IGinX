package cn.edu.tsinghua.iginx.policy.range;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.policy.AbstractPolicy;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangePolicy extends AbstractPolicy implements IPolicy {

  private static final Logger LOGGER = LoggerFactory.getLogger(RangePolicy.class);

  protected AtomicBoolean needReAllocate = new AtomicBoolean(false);

  private IMetaManager iMetaManager;

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  @Override
  public void notify(DataStatement statement) {}

  @Override
  public void init(IMetaManager iMetaManager) {
    this.iMetaManager = iMetaManager;
    StorageEngineChangeHook hook = getStorageEngineChangeHook();
    if (hook != null) {
      iMetaManager.registerStorageEngineChangeHook(hook);
    }
  }

  @Override
  public StorageEngineChangeHook getStorageEngineChangeHook() {
    return (before, after) -> {
      // 哪台机器加了分片，哪台机器初始化，并且在批量添加的时候只有最后一个存储引擎才会导致扩容发生
      if (before == null
          && after != null
          && after.getCreatedBy() == iMetaManager.getIginxId()
          && after.isNeedReAllocate()) {
        needReAllocate.set(true);
        LOGGER.info("新的可写节点进入集群，集群需要重新分片");
      }
      // TODO: 针对节点退出的情况缩容
    };
  }

  @Override
  public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateInitialFragmentsAndStorageUnits(
      DataStatement statement) {
    List<FragmentMeta> fragmentList = new ArrayList<>();
    List<StorageUnitMeta> storageUnitList = new ArrayList<>();

    List<KeyInterval> keyIntervalList = new ArrayList<>();
    Map<Long, List<ColumnsInterval>> storageEngineIdToColumnsIntervalList = new HashMap<>();

    List<StorageEngineMeta> storageEngineList = iMetaManager.getStorageEngineList();
    int storageEngineNum = storageEngineList.size();
    int replicaNum =
        Math.min(1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum(), storageEngineNum);

    int n = 20; // 每个存储引擎负责n个存储单元
    int m = 2; // 每个存储单元负责m个分片

    String[] keyIntervals = config.getKeyIntervalsForRangePolicy().split(";");
    for (String keyInterval : keyIntervals) {
      String[] keys = keyInterval.split(",");
      if (keys.length != 2) {
        throw new IllegalArgumentException(
            String.format(
                "keyIntervalsForRangePolicy %s is illegal!",
                config.getKeyIntervalsForRangePolicy()));
      }
      keyIntervalList.add(new KeyInterval(Long.parseLong(keys[0]), Long.parseLong(keys[1])));
    }

    File columnsFile = new File(config.getColumnsForRangePolicy());
    List<String> chosenColumns = new ArrayList<>();
    if (columnsFile.exists() && columnsFile.isFile()) {
      List<String> columns = new ArrayList<>();
      try (BufferedReader reader = new BufferedReader(new FileReader(columnsFile))) {
        String line;
        while ((line = reader.readLine()) != null) {
          columns.add(line);
        }
      } catch (IOException e) {
        throw new RuntimeException(
            String.format("read columns file %s failed!", config.getColumnsForRangePolicy()));
      }
      Collections.sort(columns);
      if (columns.size() < storageEngineNum - 1) {
        throw new IllegalArgumentException("#column < #storageEngine - 1");
      } else {
        chosenColumns.add(null);
        for (int i = 0; i < n * m * storageEngineNum - 1; i++) {
          chosenColumns.add(columns.get((i + 1) * columns.size() / (n * m * storageEngineNum)));
        }
        chosenColumns.add(null);
      }
    } else {
      throw new RuntimeException(
          String.format("read columns file %s failed!", config.getColumnsForRangePolicy()));
    }

    // 先创建存储单元
    StorageUnitMeta[][] storageUnits = new StorageUnitMeta[storageEngineNum][n];
    for (int i = 0; i < storageEngineNum; i++) {
      long storageEngineId = storageEngineList.get(i).getId();
      for (int j = 0; j < n; j++) {
        String storageUnitId = RandomStringUtils.randomAlphanumeric(16);
        StorageUnitMeta storageUnit = new StorageUnitMeta(storageUnitId, storageEngineId, true);
        storageUnits[i][j] = storageUnit;
      }
    }

    // 再创建分片
    int cnt = 0;
    int[] columnIndexes = new int[storageEngineNum];
    for (int i = 0; i < storageEngineNum; i++) {
      int rowIndex = (i + 1) % storageEngineNum;
      for (int j = 0; j < n; j++) {
        for (int k = 0; k < m; k++) {
          for (KeyInterval interval : keyIntervalList) {
            // 主本
            String masterId = RandomStringUtils.randomAlphanumeric(16);
            FragmentMeta masterFragment =
                new FragmentMeta(
                    masterId,
                    true,
                    masterId,
                    chosenColumns.get(cnt),
                    chosenColumns.get(cnt + 1),
                    interval.getStartKey(),
                    interval.getEndKey(),
                    storageUnits[i][j].getId());
            // 副本
            for (int l = 0; l < replicaNum - 1; l++) {
              String replicaId = RandomStringUtils.randomAlphanumeric(16);
              FragmentMeta replicaFragment =
                  new FragmentMeta(
                      replicaId,
                      false,
                      masterId,
                      chosenColumns.get(cnt),
                      chosenColumns.get(cnt + 1),
                      interval.getStartKey(),
                      interval.getEndKey(),
                      storageUnits[rowIndex][columnIndexes[rowIndex]].getId());
              masterFragment.addReplica(replicaFragment);
              // 更新列索引
              columnIndexes[rowIndex] = (columnIndexes[rowIndex] + 1) % n;
              // 更新行索引
              rowIndex = (rowIndex + 1) % storageEngineNum;
              if (rowIndex == i) {
                rowIndex = (rowIndex + 1) % storageEngineNum;
              }
            }
            fragmentList.add(masterFragment);
          }
          cnt++;
        }
      }
    }

    for (int i = 0; i < storageEngineNum; i++) {
      storageUnitList.addAll(Arrays.asList(storageUnits[i]));
    }

    return new Pair<>(fragmentList, storageUnitList);
  }

  //  @Override
  //  public Pair<List<FragmentMeta>, List<StorageUnitMeta>>
  // generateInitialFragmentsAndStorageUnits(
  //      DataStatement statement) {
  //    List<FragmentMeta> fragmentList = new ArrayList<>();
  //    List<StorageUnitMeta> storageUnitList = new ArrayList<>();
  //
  //    List<KeyInterval> keyIntervalList = new ArrayList<>();
  //    Map<Long, List<ColumnsInterval>> storageEngineIdToColumnsIntervalList = new HashMap<>();
  //
  //    List<StorageEngineMeta> storageEngineList = iMetaManager.getStorageEngineList();
  //    int storageEngineNum = storageEngineList.size();
  //    int replicaNum =
  //        Math.min(1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum(),
  // storageEngineNum);
  //
  //    String[] keyIntervals = config.getKeyIntervalsForRangePolicy().split(";");
  //    for (String keyInterval : keyIntervals) {
  //      String[] keys = keyInterval.split(",");
  //      if (keys.length != 2) {
  //        throw new IllegalArgumentException(
  //            String.format(
  //                "keyIntervalsForRangePolicy %s is illegal!",
  //                config.getKeyIntervalsForRangePolicy()));
  //      }
  //      keyIntervalList.add(new KeyInterval(Long.parseLong(keys[0]), Long.parseLong(keys[1])));
  //    }
  //
  //    File columnsFile = new File(config.getColumnsForRangePolicy());
  //    if (columnsFile.exists() && columnsFile.isFile()) {
  //      List<String> columns = new ArrayList<>();
  //      try (BufferedReader reader = new BufferedReader(new FileReader(columnsFile))) {
  //        String line;
  //        while ((line = reader.readLine()) != null) {
  //          columns.add(line);
  //        }
  //      } catch (IOException e) {
  //        throw new RuntimeException(
  //            String.format("read columns file %s failed!", config.getColumnsForRangePolicy()));
  //      }
  //      Collections.sort(columns);
  //      if (columns.size() < storageEngineNum - 1) {
  //        throw new IllegalArgumentException("#column < #storageEngine - 1");
  //      } else {
  //        storageEngineIdToColumnsIntervalList.put(
  //            storageEngineList.get(0).getId(),
  //            Collections.singletonList(
  //                new ColumnsInterval(null, columns.get(columns.size() / storageEngineNum))));
  //        for (int i = 1; i < storageEngineNum - 1; i++) {
  //          storageEngineIdToColumnsIntervalList.put(
  //              storageEngineList.get(i).getId(),
  //              Collections.singletonList(
  //                  new ColumnsInterval(
  //                      columns.get(columns.size() * i / storageEngineNum),
  //                      columns.get(columns.size() * (i + 1) / storageEngineNum))));
  //        }
  //        storageEngineIdToColumnsIntervalList.put(
  //            storageEngineList.get(storageEngineNum - 1).getId(),
  //            Collections.singletonList(
  //                new ColumnsInterval(
  //                    columns.get(columns.size() * (storageEngineNum - 1) / storageEngineNum),
  //                    null)));
  //      }
  //    } else {
  //      File columnsIntervalsFile = new File(config.getColumnsIntervalsForRangePolicy());
  //      if (columnsIntervalsFile.exists() && columnsIntervalsFile.isFile()) {
  //        try (BufferedReader reader = new BufferedReader(new FileReader(columnsIntervalsFile))) {
  //          String line;
  //          while ((line = reader.readLine()) != null) {
  //            String[] parts = line.split(":");
  //            if (parts.length != 2) {
  //              throw new IllegalArgumentException(String.format("%s is illegal!", line));
  //            }
  //            String finalLine = line;
  //            long storageEngineId =
  //                storageEngineList.stream()
  //                    .filter(x -> x.getIp().equals(parts[0]))
  //                    .map(StorageEngineMeta::getId)
  //                    .findAny()
  //                    .orElseThrow(
  //                        () ->
  //                            new IllegalArgumentException(
  //                                String.format("%s is illegal!", finalLine)));
  //            for (String columnsInterval : parts[1].split(";")) {
  //              String[] columns = columnsInterval.split(",");
  //              if (columns.length != 2) {
  //                throw new IllegalArgumentException(String.format("%s is illegal!", line));
  //              }
  //              List<ColumnsInterval> columnsIntervalList =
  //                  storageEngineIdToColumnsIntervalList.computeIfAbsent(
  //                      storageEngineId, x -> new ArrayList<>());
  //              columnsIntervalList.add(
  //                  new ColumnsInterval(
  //                      columns[0].equalsIgnoreCase("null") ? null : columns[0],
  //                      columns[1].equalsIgnoreCase("null") ? null : columns[1]));
  //            }
  //          }
  //        } catch (IOException e) {
  //          throw new RuntimeException(
  //              String.format("read columns file %s failed!", config.getColumnsForRangePolicy()));
  //        }
  //      } else {
  //        throw new IllegalArgumentException(
  //            String.format(
  //                "both columnsForRangePolicy %s and columnIntervalsForRangePolicy %s are
  // illegal!",
  //                config.getColumnsForRangePolicy(), config.getColumnsIntervalsForRangePolicy()));
  //      }
  //    }
  //
  //    for (Map.Entry<Long, List<ColumnsInterval>> entry :
  //        storageEngineIdToColumnsIntervalList.entrySet()) {
  //      long storageEngineId = entry.getKey();
  //      String masterId;
  //      StorageUnitMeta storageUnit;
  //      int index = 0;
  //      for (StorageEngineMeta storageEngine : storageEngineList) {
  //        if (storageEngine.getId() == storageEngineId) {
  //          break;
  //        }
  //        index++;
  //      }
  //      for (ColumnsInterval columnsInterval : entry.getValue()) {
  //        for (KeyInterval keyInterval : keyIntervalList) {
  //          masterId = RandomStringUtils.randomAlphanumeric(16);
  //          storageUnit = new StorageUnitMeta(masterId, storageEngineId, masterId, true);
  //          for (int i = 1; i < replicaNum; i++) {
  //            storageUnit.addReplica(
  //                new StorageUnitMeta(
  //                    RandomStringUtils.randomAlphanumeric(16),
  //                    storageEngineList.get((i + index) % storageEngineNum).getId(),
  //                    masterId,
  //                    false));
  //          }
  //          storageUnitList.add(storageUnit);
  //          fragmentList.add(new FragmentMeta(columnsInterval, keyInterval, masterId));
  //        }
  //      }
  //    }
  //
  //    return new Pair<>(fragmentList, storageUnitList);
  //  }

  @Override
  public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateFragmentsAndStorageUnits(
      DataStatement statement) {
    return new Pair<>(new ArrayList<>(), new ArrayList<>());
  }

  @Override
  public Pair<FragmentMeta, StorageUnitMeta>
      generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
          String startPath,
          String endPath,
          long startKey,
          long endKey,
          List<Long> storageEngineList) {
    return null;
  }

  @Override
  public boolean isNeedReAllocate() {
    return needReAllocate.getAndSet(false);
  }

  @Override
  public void setNeedReAllocate(boolean needReAllocate) {
    this.needReAllocate.set(needReAllocate);
  }
}
