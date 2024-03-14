package cn.edu.tsinghua.iginx.policy;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.policy.naive.Sampler;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractPolicy implements IPolicy{
  protected AtomicBoolean needReAllocate = new AtomicBoolean(false);
  protected IMetaManager iMetaManager;
  private Sampler sampler;
//  @Override
//  public void notify(DataStatement statement) {
//    List<String> pathList = Utils.getPathListFromStatement(statement);
//    if (pathList != null && !pathList.isEmpty()) {
//      sampler.updatePrefix(
//              new ArrayList<>(Arrays.asList(pathList.get(0), pathList.get(pathList.size() - 1))));
//    }
//  }


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

  protected List<Long> generateStorageEngineIdList(int startIndex, int num) {
    List<Long> storageEngineIdList = new ArrayList<>();
    List<StorageEngineMeta> storageEngines = iMetaManager.getWritableStorageEngineList();
    for (int i = startIndex; i < startIndex + num; i++) {
      storageEngineIdList.add(storageEngines.get(i % storageEngines.size()).getId());
    }
    return storageEngineIdList;
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

  @Override
  public boolean isNeedReAllocate() {
    return needReAllocate.getAndSet(false);
  }

  @Override
  public void setNeedReAllocate(boolean needReAllocate) {
    this.needReAllocate.set(needReAllocate);
  }
}
