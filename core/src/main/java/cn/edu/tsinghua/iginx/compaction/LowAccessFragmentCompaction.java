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

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.resource.exception.ResourceException;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LowAccessFragmentCompaction extends Compaction {

  private static final Logger LOGGER = LoggerFactory.getLogger(LowAccessFragmentCompaction.class);

  private List<List<FragmentMeta>> toCompactFragmentGroups;

  public LowAccessFragmentCompaction(PhysicalEngine physicalEngine, IMetaManager metaManager) {
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

    List<FragmentMeta> fragmentMetaSet = metaManager.getFragments();
    toCompactFragmentGroups =
        judgeCompaction(fragmentMetaSet, fragmentHeatWriteMap, fragmentHeatReadMap);
    return !toCompactFragmentGroups.isEmpty();
  }

  public List<List<FragmentMeta>> judgeCompaction(
      List<FragmentMeta> fragmentMetaSet,
      Map<FragmentMeta, Long> fragmentHeatWriteMap,
      Map<FragmentMeta, Long> fragmentHeatReadMap) {
    List<FragmentMeta> candidateFragments = new ArrayList<>();
    // 判断是否要合并不再被写入的的历史分片
    for (FragmentMeta fragmentMeta : fragmentMetaSet) {
      long writeLoad = fragmentHeatWriteMap.getOrDefault(fragmentMeta, 0L);
      long readLoad = fragmentHeatReadMap.getOrDefault(fragmentMeta, 0L);
      if (fragmentMeta.getKeyInterval().getEndKey() != Long.MAX_VALUE
          && writeLoad
              < ConfigDescriptor.getInstance().getConfig().getFragmentCompactionWriteThreshold()
          && readLoad
              <= ConfigDescriptor.getInstance().getConfig().getFragmentCompactionReadThreshold()) {
        candidateFragments.add(fragmentMeta);
      }
    }

    return packFragmentsByGroup(candidateFragments);
  }

  @Override
  public void compact() throws Exception {
    LOGGER.info("start to compact low access fragments");
    Map<FragmentMeta, Long> fragmentMetaPointsMap = metaManager.loadFragmentPoints();
    executeCompaction(toCompactFragmentGroups, fragmentMetaPointsMap);
  }

  public void executeCompaction(
      List<List<FragmentMeta>> toCompactFragmentGroups,
      Map<FragmentMeta, Long> fragmentMetaPointsMap)
      throws PhysicalException {
    // 优先存储到点数最少的节点上（剩余磁盘空间较大）
    Map<Long, Long> storageEnginePointsMap = new HashMap<>();
    for (Map.Entry<FragmentMeta, Long> fragmentMetaPointsEntry : fragmentMetaPointsMap.entrySet()) {
      FragmentMeta fragmentMeta = fragmentMetaPointsEntry.getKey();
      long storageEngineId = fragmentMeta.getMasterStorageUnit().getStorageEngineId();
      long points = fragmentMetaPointsEntry.getValue();
      long allPoints = storageEnginePointsMap.getOrDefault(storageEngineId, 0L);
      allPoints += points;
      storageEnginePointsMap.put(storageEngineId, allPoints);
    }
    long minPoints = Long.MAX_VALUE;
    long minStorageEngineId = 0;
    for (Map.Entry<Long, Long> storageEnginePointsEntry : storageEnginePointsMap.entrySet()) {
      if (minPoints > storageEnginePointsEntry.getValue()) {
        minStorageEngineId = storageEnginePointsEntry.getKey();
        minPoints = storageEnginePointsEntry.getValue();
      }
    }

    for (List<FragmentMeta> fragmentGroup : toCompactFragmentGroups) {
      if (fragmentGroup.size() > 1) {
        // 分别计算每个du的数据量，取其中数据量最多的du作为目标合并du
        StorageUnitMeta maxStorageUnitMeta = fragmentGroup.get(0).getMasterStorageUnit();
        long maxStorageUnitPoint = 0;
        long totalPoints = 0;
        Map<String, Long> storageUnitPointsMap = new HashMap<>();
        for (FragmentMeta fragmentMeta : fragmentGroup) {
          // 优先按照节点当前存储的点数最小做选择
          if (fragmentMeta.getMasterStorageUnit().getStorageEngineId() == minStorageEngineId) {
            long pointsNum =
                storageUnitPointsMap.getOrDefault(fragmentMeta.getMasterStorageUnitId(), 0L);
            pointsNum += fragmentMetaPointsMap.getOrDefault(fragmentMeta, 0L);
            if (pointsNum > maxStorageUnitPoint) {
              maxStorageUnitMeta = fragmentMeta.getMasterStorageUnit();
            }
            storageUnitPointsMap.put(fragmentMeta.getMasterStorageUnitId(), pointsNum);
          }
          totalPoints += fragmentMetaPointsMap.getOrDefault(fragmentMeta, 0L);
        }

        try {
          compactFragmentGroupToTargetStorageUnit(fragmentGroup, maxStorageUnitMeta, totalPoints);
        } catch (ResourceException e) {
          throw new PhysicalException(e);
        }
      } else {
        LOGGER.info("fragmentGroup size = {}", fragmentGroup.size());
      }
    }
  }
}
