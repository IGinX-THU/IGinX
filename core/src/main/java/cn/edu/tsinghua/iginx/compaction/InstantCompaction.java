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
package cn.edu.tsinghua.iginx.compaction;

import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 即时对所有分片进行合并，仅用于测试 */
public class InstantCompaction extends Compaction {

  private static final Logger LOGGER = LoggerFactory.getLogger(InstantCompaction.class);
  private List<List<FragmentMeta>> toCompactFragmentGroups;

  public InstantCompaction(PhysicalEngine physicalEngine, IMetaManager metaManager) {
    super(physicalEngine, metaManager);
  }

  @Override
  public boolean needCompaction() {
    List<FragmentMeta> fragmentMetas = DefaultMetaManager.getInstance().getFragments();
    toCompactFragmentGroups = packFragmentsByGroup(fragmentMetas);
    return !toCompactFragmentGroups.isEmpty();
  }

  @Override
  public void compact() throws Exception {
    LOGGER.info("start to compact all fragments");
    for (List<FragmentMeta> fragmentGroup : toCompactFragmentGroups) {
      if (fragmentGroup.size() > 1) {
        StorageUnitMeta maxStorageUnitMeta = fragmentGroup.get(0).getMasterStorageUnit();
        compactFragmentGroupToTargetStorageUnit(fragmentGroup, maxStorageUnitMeta, 0L);
      }
    }
  }
}
