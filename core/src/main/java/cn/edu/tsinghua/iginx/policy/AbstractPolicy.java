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
