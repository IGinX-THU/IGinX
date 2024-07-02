package cn.edu.tsinghua.iginx.policy.simple;

import cn.edu.tsinghua.iginx.conf.Config;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimplePolicy extends AbstractPolicy implements IPolicy {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimplePolicy.class);

  protected AtomicBoolean needReAllocate = new AtomicBoolean(false);
  private IMetaManager iMetaManager;
  private FragmentCreator fragmentCreator;
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  @Override
  public void notify(DataStatement statement) {
    if (statement.getType() == StatementType.INSERT) {
      iMetaManager.saveColumnsData((InsertStatement) statement);
    }
  }

  @Override
  public void init(IMetaManager iMetaManager) {
    this.iMetaManager = iMetaManager;
    this.fragmentCreator = new FragmentCreator(this, this.iMetaManager);
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
   * This storage unit initialization method is used when clients are provided, such as in TPCx-IoT
   * tests
   */
  public Pair<Map<ColumnsInterval, List<FragmentMeta>>, List<StorageUnitMeta>>
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
      for (int j = i + 1; j < i + replicaNum; j++) {
        storageUnit.addReplica(
            new StorageUnitMeta(
                RandomStringUtils.randomAlphanumeric(16),
                storageEngineList.get(j % storageEngineNum).getId(),
                masterId,
                false));
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

  /**
   * This storage unit initialization method is used when no information about workloads is provided
   */
  public Pair<List<FragmentMeta>, List<StorageUnitMeta>>
      generateInitialFragmentsAndStorageUnitsDefault(
          List<String> inspaths, KeyInterval keyInterval) {
    List<FragmentMeta> fragmentList = new ArrayList<>();
    List<StorageUnitMeta> storageUnitList = new ArrayList<>();
    List<String> paths = new ArrayList<>();
    if (inspaths.size() > 0) {
      paths.add(inspaths.get(0));
    }

    if (inspaths.size() > 1) {
      paths.add(inspaths.get(inspaths.size() - 1));
    }

    int storageEngineNum = iMetaManager.getStorageEngineNum();
    int replicaNum =
        Math.min(1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum(), storageEngineNum);
    List<Long> storageEngineIdList;
    Pair<FragmentMeta, StorageUnitMeta> pair;
    int index = 0;

    // [0, startKey) & (-∞, +∞)
    // 一般情况下该范围内几乎无数据，因此作为一个分片处理
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
    if (paths.isEmpty()) {
      storageEngineIdList = generateStorageEngineIdList(index++, replicaNum);
      pair =
          generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
              null, null, keyInterval.getStartKey(), Long.MAX_VALUE, storageEngineIdList);
      fragmentList.add(pair.k);
      storageUnitList.add(pair.v);
      return new Pair<>(fragmentList, storageUnitList);
    }

    // [startKey, +∞) & (null, startPath)
    storageEngineIdList = generateStorageEngineIdList(index++, replicaNum);
    pair =
        generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
            null, paths.get(0), keyInterval.getStartKey(), Long.MAX_VALUE, storageEngineIdList);
    fragmentList.add(pair.k);
    storageUnitList.add(pair.v);

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
    storageEngineIdList = generateStorageEngineIdList(index, replicaNum);
    pair =
        generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
            paths.get(paths.size() - 1),
            null,
            keyInterval.getStartKey(),
            Long.MAX_VALUE,
            storageEngineIdList);
    fragmentList.add(pair.k);
    storageUnitList.add(pair.v);

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
        new StorageUnitMeta(masterId, storageEngineList.get(0), masterId, true, false);
    FragmentMeta fragment = new FragmentMeta(startPath, endPath, startKey, endKey, masterId);
    for (int i = 1; i < storageEngineList.size(); i++) {
      storageUnit.addReplica(
          new StorageUnitMeta(
              RandomStringUtils.randomAlphanumeric(16),
              storageEngineList.get(i),
              masterId,
              false,
              false));
    }
    return new Pair<>(fragment, storageUnit);
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

    Map<String, Double> data = iMetaManager.getColumnsData();
    List<String> prefixList = getNewFragment(data);

    List<FragmentMeta> fragmentList = new ArrayList<>();
    List<StorageUnitMeta> storageUnitList = new ArrayList<>();

    int storageEngineNum = iMetaManager.getStorageEngineNum();
    int replicaNum =
        Math.min(1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum(), storageEngineNum);
    List<Long> storageEngineIdList;
    Pair<FragmentMeta, StorageUnitMeta> pair;
    int index = 0;

    // [startKey, +∞) & (null, startPath)
    storageEngineIdList = generateStorageEngineIdList(index++, replicaNum);
    pair =
        generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
            null, prefixList.get(0), startKey, Long.MAX_VALUE, storageEngineIdList);
    fragmentList.add(pair.k);
    storageUnitList.add(pair.v);

    // [startKey, +∞) & [startPath, endPath)
    int splitNum = Math.max(prefixList.size() - 1, 0);
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
    storageEngineIdList = generateStorageEngineIdList(index, replicaNum);
    pair =
        generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
            prefixList.get(prefixList.size() - 1),
            null,
            startKey,
            Long.MAX_VALUE,
            storageEngineIdList);
    fragmentList.add(pair.k);
    storageUnitList.add(pair.v);

    return new Pair<>(fragmentList, storageUnitList);
  }

  private List<String> getNewFragment(Map<String, Double> data) {
    List<String> ret = new ArrayList<>();
    int n = data.size();
    int m = iMetaManager.getStorageEngineNum() * config.getFragmentPerEngine();
    if (config.isEnableStorageGroupValueLimit()) {
      double totalValue = data.values().stream().mapToDouble(Double::doubleValue).sum();
      m =
          new Double(
                      Math.ceil(
                          totalValue
                              / config.getStorageGroupValueLimit()
                              / iMetaManager.getStorageEngineNum()))
                  .intValue()
              * iMetaManager.getStorageEngineNum();
    }
    List<Pair<String, Double>> tmp =
        data.entrySet().stream()
            .map(entry -> new Pair<>(entry.getKey(), entry.getValue()))
            .sorted(Comparator.comparing(Pair::getK))
            .collect(Collectors.toList());
    List<Double> sum = new ArrayList<>();
    sum.add(0.0);
    for (int i = 0; i < n; i++) {
      sum.add(sum.get(i) + tmp.get(i).v);
    }
    double[][] dp = new double[n + 1][m + 1];
    int[][] last = new int[n + 1][m + 1];
    for (int i = 0; i <= n; i++) {
      for (int j = 0; j <= m; j++) {
        dp[i][j] = Double.MAX_VALUE / 10;
        last[i][j] = -1;
      }
    }
    dp[0][0] = 0;
    for (int i = 1; i <= n; i++) {
      for (int j = 1; j <= m; j++) {
        for (int k = 0; k < i; k++) {
          double tmpValue = dp[k][j - 1] + Math.pow(sum.get(i) - sum.get(k), 2);
          if (tmpValue < dp[i][j]) {
            dp[i][j] = tmpValue;
            last[i][j] = k;
          }
        }
      }
    }
    int tmpn = n, tmpm = m;
    while (last[tmpn][tmpm] > 0) {
      tmpn = last[tmpn][tmpm];
      tmpm--;
      ret.add(tmp.get(tmpn - 1).k);
    }
    Collections.reverse(ret);
    return ret;
  }

  public boolean checkSuccess(Map<String, Double> columnsData) {
    Map<ColumnsInterval, FragmentMeta> latestFragments = iMetaManager.getLatestFragmentMap();
    Map<ColumnsInterval, Double> fragmentValue =
        latestFragments.keySet().stream()
            .collect(Collectors.toMap(Function.identity(), e1 -> 0.0, (e1, e2) -> e1));
    columnsData.forEach(
        (key, value) -> {
          for (Map.Entry<ColumnsInterval, Double> entry : fragmentValue.entrySet()) {
            if (entry.getKey().isContain(key)) {
              fragmentValue.put(entry.getKey(), value + entry.getValue());
            }
          }
        });
    List<Double> value = fragmentValue.values().stream().sorted().collect(Collectors.toList());
    int num = 0;
    for (Double v : value) {
      LOGGER.info("fragment value num : {}, value : {}", num++, v);
    }
    if (value.size() > 0) {
      return !(value.get(new Double(Math.ceil(value.size() - 1) * 0.9).intValue())
          > config.getStorageGroupValueLimit() * 3);
    } else {
      return true;
    }
  }
}
