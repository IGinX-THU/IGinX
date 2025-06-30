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
package cn.edu.tsinghua.iginx.policy.single;

import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.policy.AbstractPolicy;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinglePolicy extends AbstractPolicy {

  private static final Logger LOGGER = LoggerFactory.getLogger(SinglePolicy.class);

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
      if (before == null
          && after != null
          && after.getCreatedBy() == iMetaManager.getIginxId()
          && after.isNeedReAllocate()) {
        needReAllocate.set(true);
        LOGGER.info("新的可写节点进入集群，集群需要重新分片");
      }
    };
  }

  @Override
  public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateInitialFragmentsAndStorageUnits(
      DataStatement statement) {
    return generateFragmentsAndStorageUnits(statement);
  }

  @Override
  public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateFragmentsAndStorageUnits(
      DataStatement statement) {
    KeyInterval keyInterval = KeyInterval.getDefaultKeyInterval();

    List<Long> storageEngineList =
        iMetaManager.getWritableStorageEngineList().stream()
            .map(StorageEngineMeta::getId)
            .collect(Collectors.toList());

    String masterId = "master";
    StorageUnitMeta master =
        new StorageUnitMeta(masterId, storageEngineList.get(0), masterId, true);
    for (int i = 1; i < storageEngineList.size(); i++) {
      String replicaId = "replica" + RandomStringUtils.randomAlphanumeric(16);
      StorageUnitMeta replica =
          new StorageUnitMeta(replicaId, storageEngineList.get(i), masterId, false);
      master.addReplica(replica);
    }

    FragmentMeta fragment =
        new FragmentMeta(null, null, keyInterval.getStartKey(), keyInterval.getEndKey(), masterId);

    return new Pair<>(Collections.singletonList(fragment), Collections.singletonList(master));
  }

  public Pair<FragmentMeta, StorageUnitMeta>
      generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
          String startPath,
          String endPath,
          long startKey,
          long endKey,
          List<Long> storageEngineList) {
    throw new UnsupportedOperationException("SinglePolicy does not support this method");
  }
}
