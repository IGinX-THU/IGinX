package cn.edu.tsinghua.iginx.compaction;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LowWriteFragmentCompaction extends Compaction {

  private static final Logger LOGGER = LoggerFactory.getLogger(LowWriteFragmentCompaction.class);

  private List<List<FragmentMeta>> toCompactFragmentGroups;

  public LowWriteFragmentCompaction(PhysicalEngine physicalEngine, IMetaManager metaManager) {
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
    long totalHeats = 0;
    for (Map.Entry<FragmentMeta, Long> fragmentHeatReadEntry : fragmentHeatReadMap.entrySet()) {
      totalHeats += fragmentHeatReadEntry.getValue();
    }
    double averageReadHeats = totalHeats * 1.0 / fragmentHeatReadMap.size();

    List<FragmentMeta> candidateFragments = new ArrayList<>();
    // 判断是否要合并不再被写入的的历史分片
    for (FragmentMeta fragmentMeta : fragmentMetaSet) {
      long writeLoad = fragmentHeatWriteMap.getOrDefault(fragmentMeta, 0L);
      long readLoad = fragmentHeatReadMap.getOrDefault(fragmentMeta, 0L);
      if (fragmentMeta.getKeyInterval().getEndKey() != Long.MAX_VALUE
          && writeLoad
              < ConfigDescriptor.getInstance().getConfig().getFragmentCompactionWriteThreshold()
                  * ConfigDescriptor.getInstance().getConfig().getLoadBalanceCheckInterval()
          && readLoad
              < averageReadHeats
                  * ConfigDescriptor.getInstance()
                      .getConfig()
                      .getFragmentCompactionReadRatioThreshold()
          && readLoad
              > ConfigDescriptor.getInstance().getConfig().getFragmentCompactionReadThreshold()
                  * ConfigDescriptor.getInstance().getConfig().getLoadBalanceCheckInterval()) {
        candidateFragments.add(fragmentMeta);
      }
    }

    return packFragmentsByGroup(candidateFragments);
  }

  @Override
  public void compact() throws Exception {
    LOGGER.info("start to compact low write fragments");
    Map<FragmentMeta, Long> fragmentMetaPointsMap = metaManager.loadFragmentPoints();
    executeCompaction(toCompactFragmentGroups, fragmentMetaPointsMap);
  }

  public void executeCompaction(
      List<List<FragmentMeta>> toCompactFragmentGroups,
      Map<FragmentMeta, Long> fragmentMetaPointsMap)
      throws PhysicalException {
    for (List<FragmentMeta> fragmentGroup : toCompactFragmentGroups) {
      if (fragmentGroup.size() > 1) {
        // 分别计算每个du的数据量，取其中数据量最多的du作为目标合并du
        StorageUnitMeta maxStorageUnitMeta = fragmentGroup.get(0).getMasterStorageUnit();
        long maxStorageUnitPoint = 0;
        Map<String, Long> storageUnitPointsMap = new HashMap<>();
        long totalPoints = 0;
        for (FragmentMeta fragmentMeta : fragmentGroup) {
          long pointsNum =
              storageUnitPointsMap.getOrDefault(fragmentMeta.getMasterStorageUnitId(), 0L);
          pointsNum += fragmentMetaPointsMap.getOrDefault(fragmentMeta, 0L);
          if (pointsNum > maxStorageUnitPoint) {
            maxStorageUnitMeta = fragmentMeta.getMasterStorageUnit();
          }
          storageUnitPointsMap.put(fragmentMeta.getMasterStorageUnitId(), pointsNum);
          totalPoints += fragmentMetaPointsMap.getOrDefault(fragmentMeta, 0L);
        }

        compactFragmentGroupToTargetStorageUnit(fragmentGroup, maxStorageUnitMeta, totalPoints);
      } else {
        LOGGER.info("fragmentGroup size = {}", fragmentGroup.size());
      }
    }
  }
}
