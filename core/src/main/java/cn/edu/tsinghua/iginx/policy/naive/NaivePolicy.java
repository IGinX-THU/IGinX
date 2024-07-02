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
package cn.edu.tsinghua.iginx.policy.naive;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.policy.AbstractPolicy;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.Utils;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NaivePolicy extends AbstractPolicy implements IPolicy {

  private static final Logger LOGGER = LoggerFactory.getLogger(NaivePolicy.class);

  private Sampler sampler;

  @Override
  public void notify(DataStatement statement) {
    List<String> pathList = Utils.getPathListFromStatement(statement);
    if (pathList != null && !pathList.isEmpty()) {
      sampler.updatePrefix(
          new ArrayList<>(Arrays.asList(pathList.get(0), pathList.get(pathList.size() - 1))));
    }
  }

  @Override
  public void init(IMetaManager iMetaManager) {
    this.iMetaManager = iMetaManager;
    this.sampler = Sampler.getInstance();
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
    List<String> paths = Utils.getNonWildCardPaths(Utils.getPathListFromStatement(statement));
    KeyInterval keyInterval = Utils.getKeyIntervalFromDataStatement(statement);

    if (ConfigDescriptor.getInstance().getConfig().getClients().indexOf(",") > 0) {
      Pair<Map<ColumnsInterval, List<FragmentMeta>>, List<StorageUnitMeta>> pair =
          generateInitialFragmentsAndStorageUnitsByClients(paths, keyInterval);
      return new Pair<>(
          pair.k.values().stream().flatMap(List::stream).collect(Collectors.toList()), pair.v);
    } else {
      return generateInitialFragmentsAndStorageUnitsDefault(paths, keyInterval);
    }
  }

  /**
   * This storage unit initialization method is used when no information about workloads is provided
   */
  private Pair<List<FragmentMeta>, List<StorageUnitMeta>>
      generateInitialFragmentsAndStorageUnitsDefault(List<String> paths, KeyInterval keyInterval) {
    List<FragmentMeta> fragmentList = new ArrayList<>();
    List<StorageUnitMeta> storageUnitList = new ArrayList<>();

    int storageEngineNum = iMetaManager.getStorageEngineNum();
    int replicaNum =
        Math.min(1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum(), storageEngineNum);
    List<Long> storageEngineIdList;
    Pair<FragmentMeta, StorageUnitMeta> pair;
    int index = 0;

    // [0, startKey) & (-∞, +∞)
    // 一般情况下该范围内几乎无数据，因此作为一个分片处理
    // TODO 考虑大规模插入历史数据的情况
    if (keyInterval.getStartKey() != 0) {
      storageEngineIdList = generateStorageEngineIdList(index++, replicaNum);
      pair =
          generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
              null, null, 0, keyInterval.getStartKey(), storageEngineIdList);
      fragmentList.add(pair.k);
      storageUnitList.add(pair.v);
    }

    // [startKey, +∞) & (-∞, +∞)
    // 在初始查询/删除等语句中没有具体路径，只有通配符的情况下创建初始分片
    if (paths == null || paths.isEmpty()) {
      storageEngineIdList = generateStorageEngineIdList(index++, replicaNum);
      pair =
          generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
              null, null, keyInterval.getStartKey(), Long.MAX_VALUE, storageEngineIdList);
      fragmentList.add(pair.k);
      storageUnitList.add(pair.v);
      return new Pair<>(fragmentList, storageUnitList);
    }

    // [startKey, +∞) & [startPath, endPath)
    int splitNum = Math.max(Math.min(storageEngineNum, paths.size() - 1), 0);
    for (int i = 0; i < splitNum; i++) {
      storageEngineIdList = generateStorageEngineIdList(index++, replicaNum);
      pair =
          generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
              paths.get(i * (paths.size() - 1) / splitNum),
              paths.get((i + 1) * (paths.size() - 1) / splitNum),
              keyInterval.getStartKey(),
              Long.MAX_VALUE,
              storageEngineIdList);
      fragmentList.add(pair.k);
      storageUnitList.add(pair.v);
    }

    // [startKey, +∞) & [endPath, null)
    storageEngineIdList = generateStorageEngineIdList(index++, replicaNum);
    pair =
        generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
            paths.get(paths.size() - 1),
            null,
            keyInterval.getStartKey(),
            Long.MAX_VALUE,
            storageEngineIdList);
    fragmentList.add(pair.k);
    storageUnitList.add(pair.v);

    // [startKey, +∞) & (null, startPath)
    storageEngineIdList = generateStorageEngineIdList(index, replicaNum);
    pair =
        generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
            null, paths.get(0), keyInterval.getStartKey(), Long.MAX_VALUE, storageEngineIdList);
    fragmentList.add(pair.k);
    storageUnitList.add(pair.v);

    return new Pair<>(fragmentList, storageUnitList);
  }

  /**
   * This storage unit initialization method is used when clients are provided, such as in TPCx-IoT
   * tests
   */
  private Pair<Map<ColumnsInterval, List<FragmentMeta>>, List<StorageUnitMeta>>
      generateInitialFragmentsAndStorageUnitsByClients(
          List<String> paths, KeyInterval keyInterval) {
    Map<ColumnsInterval, List<FragmentMeta>> fragmentMap = new HashMap<>();
    List<StorageUnitMeta> storageUnitList = new ArrayList<>();

    List<StorageEngineMeta> storageEngineList = iMetaManager.getWritableStorageEngineList();
    int storageEngineNum = storageEngineList.size();

    String[] clients = ConfigDescriptor.getInstance().getConfig().getClients().split(",");
    int instancesNumPerClient =
        ConfigDescriptor.getInstance().getConfig().getInstancesNumPerClient() - 1;
    int replicaNum =
        Math.min(1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum(), storageEngineNum);
    String[] prefixes = new String[clients.length * instancesNumPerClient];
    for (int i = 0; i < clients.length; i++) {
      for (int j = 0; j < instancesNumPerClient; j++) {
        prefixes[i * instancesNumPerClient + j] = clients[i] + (j + 2);
      }
    }
    Arrays.sort(prefixes);

    List<FragmentMeta> fragmentMetaList;
    String masterId;
    StorageUnitMeta storageUnit;
    for (int i = 0; i < clients.length * instancesNumPerClient - 1; i++) {
      fragmentMetaList = new ArrayList<>();
      masterId = RandomStringUtils.randomAlphanumeric(16);
      storageUnit =
          new StorageUnitMeta(
              masterId, storageEngineList.get(i % storageEngineNum).getId(), masterId, true);
      //            storageUnit = new StorageUnitMeta(masterId, getStorageEngineList().get(i *
      // 2 % getStorageEngineList().size()).getId(), masterId, true);
      for (int j = i + 1; j < i + replicaNum; j++) {
        storageUnit.addReplica(
            new StorageUnitMeta(
                RandomStringUtils.randomAlphanumeric(16),
                storageEngineList.get(j % storageEngineNum).getId(),
                masterId,
                false));
        //                storageUnit.addReplica(new
        // StorageUnitMeta(RandomStringUtils.randomAlphanumeric(16),
        // getStorageEngineList().get((i * 2 + 1) % getStorageEngineList().size()).getId(),
        // masterId, false));
      }
      storageUnitList.add(storageUnit);
      fragmentMetaList.add(
          new FragmentMeta(prefixes[i], prefixes[i + 1], 0, Long.MAX_VALUE, masterId));
      fragmentMap.put(new ColumnsInterval(prefixes[i], prefixes[i + 1]), fragmentMetaList);
    }

    fragmentMetaList = new ArrayList<>();
    masterId = RandomStringUtils.randomAlphanumeric(16);
    storageUnit = new StorageUnitMeta(masterId, storageEngineList.get(0).getId(), masterId, true);
    for (int i = 1; i < replicaNum; i++) {
      storageUnit.addReplica(
          new StorageUnitMeta(
              RandomStringUtils.randomAlphanumeric(16),
              storageEngineList.get(i).getId(),
              masterId,
              false));
    }
    storageUnitList.add(storageUnit);
    fragmentMetaList.add(new FragmentMeta(null, prefixes[0], 0, Long.MAX_VALUE, masterId));
    fragmentMap.put(new ColumnsInterval(null, prefixes[0]), fragmentMetaList);

    fragmentMetaList = new ArrayList<>();
    masterId = RandomStringUtils.randomAlphanumeric(16);
    storageUnit =
        new StorageUnitMeta(
            masterId, storageEngineList.get(storageEngineNum - 1).getId(), masterId, true);
    for (int i = 1; i < replicaNum; i++) {
      storageUnit.addReplica(
          new StorageUnitMeta(
              RandomStringUtils.randomAlphanumeric(16),
              storageEngineList.get(storageEngineNum - 1 - i).getId(),
              masterId,
              false));
    }
    storageUnitList.add(storageUnit);
    fragmentMetaList.add(
        new FragmentMeta(
            prefixes[clients.length * instancesNumPerClient - 1],
            null,
            0,
            Long.MAX_VALUE,
            masterId));
    fragmentMap.put(
        new ColumnsInterval(prefixes[clients.length * instancesNumPerClient - 1], null),
        fragmentMetaList);

    return new Pair<>(fragmentMap, storageUnitList);
  }

  private Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateFragmentsAndStorageUnits(
      List<String> prefixList, long startKey) {
    List<FragmentMeta> fragmentList = new ArrayList<>();
    List<StorageUnitMeta> storageUnitList = new ArrayList<>();

    int storageEngineNum = iMetaManager.getStorageEngineNum();
    int replicaNum =
        Math.min(1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum(), storageEngineNum);
    List<Long> storageEngineIdList;
    Pair<FragmentMeta, StorageUnitMeta> pair;
    int index = 0;

    // [startKey, +∞) & [startPath, endPath)
    int splitNum = Math.max(Math.min(storageEngineNum, prefixList.size() - 1), 0);
    for (int i = 0; i < splitNum; i++) {
      storageEngineIdList = generateStorageEngineIdList(index++, replicaNum);
      pair =
          generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
              prefixList.get(i),
              prefixList.get(i + 1),
              startKey,
              Long.MAX_VALUE,
              storageEngineIdList);
      fragmentList.add(pair.k);
      storageUnitList.add(pair.v);
    }

    // [startKey, +∞) & [endPath, null)
    storageEngineIdList = generateStorageEngineIdList(index++, replicaNum);
    pair =
        generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
            prefixList.get(prefixList.size() - 1),
            null,
            startKey,
            Long.MAX_VALUE,
            storageEngineIdList);
    fragmentList.add(pair.k);
    storageUnitList.add(pair.v);

    // [startKey, +∞) & (null, startPath)
    storageEngineIdList = generateStorageEngineIdList(index, replicaNum);
    pair =
        generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
            null, prefixList.get(0), startKey, Long.MAX_VALUE, storageEngineIdList);
    fragmentList.add(pair.k);
    storageUnitList.add(pair.v);

    return new Pair<>(fragmentList, storageUnitList);
  }

  @Override
  public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateFragmentsAndStorageUnits(
      DataStatement statement) {
    long startKey;
    if (statement.getType() == StatementType.INSERT) {
      startKey =
          ((InsertStatement) statement).getEndKey()
              + TimeUnit.SECONDS.toMillis(
                      ConfigDescriptor.getInstance().getConfig().getDisorderMargin())
                  * 2
              + 1;
    } else {
      throw new IllegalArgumentException(
          "function generateFragmentsAndStorageUnits only use insert statement for now.");
    }

    List<FragmentMeta> fragmentList = new ArrayList<>();
    List<StorageUnitMeta> storageUnitList = new ArrayList<>();
    int storageEngineNum = iMetaManager.getWritableStorageEngineList().size();
    int replicaNum =
        Math.min(1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum(), storageEngineNum);
    List<Long> storageEngineIdList;
    Pair<FragmentMeta, StorageUnitMeta> pair;

    if (storageEngineNum == 0) {
      // 系统中无可写存储引擎
      throw new IllegalArgumentException("there are no writable storage engines!");
    } else if (storageEngineNum == 1) {
      // 系统中只有一个可写存储引擎
      // [startKey, +∞) & (null, null)
      storageEngineIdList =
          Collections.singletonList(iMetaManager.getWritableStorageEngineList().get(0).getId());
      pair =
          generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
              null, null, startKey, Long.MAX_VALUE, storageEngineIdList);
      fragmentList.add(pair.k);
      storageUnitList.add(pair.v);
    } else {
      // 系统中有多个可写存储引擎
      List<String> prefixList = sampler.samplePrefix(storageEngineNum - 1);

      int index = 0;

      // [startKey, +∞) & [startPath, endPath)
      int splitNum = Math.max(Math.min(storageEngineNum, prefixList.size() - 1), 0);
      for (int i = 0; i < splitNum; i++) {
        storageEngineIdList = generateStorageEngineIdList(index++, replicaNum);
        pair =
            generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
                prefixList.get(i),
                prefixList.get(i + 1),
                startKey,
                Long.MAX_VALUE,
                storageEngineIdList);
        fragmentList.add(pair.k);
        storageUnitList.add(pair.v);
      }

      // [startKey, +∞) & [endPath, null)
      storageEngineIdList = generateStorageEngineIdList(index++, replicaNum);
      pair =
          generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
              prefixList.get(prefixList.size() - 1),
              null,
              startKey,
              Long.MAX_VALUE,
              storageEngineIdList);
      fragmentList.add(pair.k);
      storageUnitList.add(pair.v);

      // [startKey, +∞) & (null, startPath)
      storageEngineIdList = generateStorageEngineIdList(index, replicaNum);
      pair =
          generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
              null, prefixList.get(0), startKey, Long.MAX_VALUE, storageEngineIdList);
      fragmentList.add(pair.k);
      storageUnitList.add(pair.v);
    }

    return new Pair<>(fragmentList, storageUnitList);
  }

  public Pair<FragmentMeta, StorageUnitMeta>
      generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
          String startPath,
          String endPath,
          long startKey,
          long endKey,
          List<Long> storageEngineList) {
    String masterId = RandomStringUtils.randomAlphanumeric(16);
    StorageUnitMeta storageUnit =
        new StorageUnitMeta(masterId, storageEngineList.get(0), masterId, true);
    FragmentMeta fragment = new FragmentMeta(startPath, endPath, startKey, endKey, masterId);
    for (int i = 1; i < storageEngineList.size(); i++) {
      storageUnit.addReplica(
          new StorageUnitMeta(
              RandomStringUtils.randomAlphanumeric(16), storageEngineList.get(i), masterId, false));
    }
    return new Pair<>(fragment, storageUnit);
  }
}
