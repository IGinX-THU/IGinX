package cn.edu.tsinghua.iginx.policy.historical;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.policy.AbstractPolicy;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.Utils;
import cn.edu.tsinghua.iginx.policy.naive.Sampler;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// 若要使用历史数据分片策略，须在config.properties中添加以下配置：
// # 历史数据中常见的路径前缀列表，使用','分隔
// historicalPrefixList=000,001,002,AAA
// # 预期存储单元数量
// expectedStorageUnitNum=50
public class HistoricalPolicy extends AbstractPolicy implements IPolicy {

  private static final Logger LOGGER = LoggerFactory.getLogger(HistoricalPolicy.class);

  private Sampler sampler;
  private List<String> suffixList = new ArrayList<>();

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

    for (int i = 0; i <= 9; i++) {
      suffixList.add(String.valueOf(i));
    }
    for (char c = 'A'; c <= 'Z'; c++) {
      suffixList.add(String.valueOf(c));
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

    KeyInterval keyInterval = Utils.getKeyIntervalFromDataStatement(statement);
    List<StorageEngineMeta> storageEngineList = iMetaManager.getStorageEngineList();
    int storageEngineNum = storageEngineList.size();
    int replicaNum =
        Math.min(1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum(), storageEngineNum);

    String[] historicalPrefixList =
        ConfigDescriptor.getInstance().getConfig().getHistoricalPrefixList().split(",");
    Arrays.sort(historicalPrefixList);
    int expectedStorageUnitNum =
        ConfigDescriptor.getInstance().getConfig().getExpectedStorageUnitNum();

    List<String> prefixList = new ArrayList<>();
    List<ColumnsInterval> columnsIntervalList = new ArrayList<>();
    for (String historicalPrefix : historicalPrefixList) {
      for (String suffix : suffixList) {
        if (!prefixList.contains(historicalPrefix + suffix)) {
          prefixList.add(historicalPrefix + suffix);
        }
      }
    }
    Collections.sort(prefixList);
    int prefixNum = prefixList.size();
    prefixList.add(null);
    columnsIntervalList.add(new ColumnsInterval(null, prefixList.get(0)));
    for (int i = 0; i < expectedStorageUnitNum; i++) {
      columnsIntervalList.add(
          new ColumnsInterval(
              prefixList.get(i * prefixNum / expectedStorageUnitNum),
              prefixList.get((i + 1) * prefixNum / expectedStorageUnitNum)));
    }

    String masterId;
    StorageUnitMeta storageUnit;
    for (int i = 0; i < columnsIntervalList.size(); i++) {
      masterId = RandomStringUtils.randomAlphanumeric(16);
      storageUnit =
          new StorageUnitMeta(
              masterId, storageEngineList.get(i % storageEngineNum).getId(), masterId, true);
      for (int j = i + 1; j < i + replicaNum; j++) {
        storageUnit.addReplica(
            new StorageUnitMeta(
                RandomStringUtils.randomAlphanumeric(16),
                storageEngineList.get(j % storageEngineNum).getId(),
                masterId,
                false));
      }
      storageUnitList.add(storageUnit);
      fragmentList.add(new FragmentMeta(columnsIntervalList.get(i), keyInterval, masterId));
    }

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
