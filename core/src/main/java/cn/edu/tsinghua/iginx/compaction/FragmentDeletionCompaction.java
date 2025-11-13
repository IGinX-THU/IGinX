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
package cn.edu.tsinghua.iginx.compaction;

import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FragmentDeletionCompaction extends Compaction {

  private static final Logger LOGGER = LoggerFactory.getLogger(FragmentDeletionCompaction.class);
  private List<FragmentMeta> toDeletionFragments = new ArrayList<>();

  public FragmentDeletionCompaction(PhysicalEngine physicalEngine, IMetaManager metaManager) {
    super(physicalEngine, metaManager);
  }

  @Override
  public boolean needCompaction() throws Exception {
    // 集中信息（初版主要是统计分区热度）
    Pair<Map<FragmentMeta, Long>, Map<FragmentMeta, Long>> fragmentHeatPair =
        metaManager.loadFragmentHeat();
    Map<FragmentMeta, Long> fragmentHeatWriteMap = fragmentHeatPair.getK();
    Map<FragmentMeta, Long> fragmentHeatReadMap = fragmentHeatPair.getV();
    if (fragmentHeatWriteMap == null) {
      fragmentHeatWriteMap = new HashMap<>();
    }
    if (fragmentHeatReadMap == null) {
      fragmentHeatReadMap = new HashMap<>();
    }

    long totalHeats = 0;
    for (Map.Entry<FragmentMeta, Long> fragmentHeatReadEntry : fragmentHeatReadMap.entrySet()) {
      totalHeats += fragmentHeatReadEntry.getValue();
    }
    double limitReadHeats = totalHeats * 1.0 / fragmentHeatReadMap.size();

    // 判断是否要删除可定制化副本生成的冗余分片
    // TODO

    return !toDeletionFragments.isEmpty();
  }

  @Override
  public void compact() throws PhysicalException {
    for (FragmentMeta fragmentMeta : toDeletionFragments) {
      // 删除可定制化副本分片元数据
      // TODO

      // 删除节点数据
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
      try (RowStream ignored = physicalEngine.execute(new RequestContext(), delete)) {}
    }
  }
}
