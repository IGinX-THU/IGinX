/** 该Policy是为了测试而写的，不要在生产环境中使用 该Policy会生成KeyRange不同的Fragment,用于测试。 */
package cn.edu.tsinghua.iginx.policy.test;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.policy.naive.NaivePolicy;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyRangeTestPolicy extends NaivePolicy {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyRangeTestPolicy.class);

  @Override
  public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateInitialFragmentsAndStorageUnits(
      DataStatement statement) {
    Pair<List<FragmentMeta>, List<StorageUnitMeta>> pair =
        super.generateInitialFragmentsAndStorageUnits(statement);
    List<FragmentMeta> fragmentMetaList = pair.k;
    List<StorageUnitMeta> storageUnitMetaList = pair.v;

    List<StorageUnitMeta> newSuList = new ArrayList<>();
    List<FragmentMeta> newFMList = new ArrayList<>();
    for (int i = 0; i < fragmentMetaList.size(); i++) {
      StorageUnitMeta su = storageUnitMetaList.get(i);
      String masterId1 = RandomStringUtils.randomAlphanumeric(16);
      String masterId2 = RandomStringUtils.randomAlphanumeric(16);
      StorageUnitMeta newSu1 =
          new StorageUnitMeta(masterId1, su.getStorageEngineId(), masterId1, su.isMaster());
      StorageUnitMeta newSu2 =
          new StorageUnitMeta(masterId2, su.getStorageEngineId(), masterId2, su.isMaster());
      newSuList.add(newSu1);
      newSuList.add(newSu2);

      FragmentMeta fm = fragmentMetaList.get(i);
      KeyInterval fmKeyInterval = fm.getKeyInterval();
      long midKey = (fmKeyInterval.getEndKey() - fmKeyInterval.getStartKey()) / 2;
      FragmentMeta newFm1 =
          new FragmentMeta(
              fm.getColumnsInterval(),
              new KeyInterval(fmKeyInterval.getStartKey(), midKey),
              masterId1);
      FragmentMeta newFm2 =
          new FragmentMeta(
              fm.getColumnsInterval(),
              new KeyInterval(midKey + 1, fmKeyInterval.getEndKey()),
              masterId2);
      newFMList.add(newFm1);
      newFMList.add(newFm2);
    }

    return new Pair<>(newFMList, newSuList);
  }
}
