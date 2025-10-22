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
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Migration;
import cn.edu.tsinghua.iginx.engine.shared.operator.ShowColumns;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import java.util.*;

public abstract class Compaction {
  protected PhysicalEngine physicalEngine;
  protected IMetaManager metaManager;

  public Compaction(PhysicalEngine physicalEngine, IMetaManager metaManager) {
    this.physicalEngine = physicalEngine;
    this.metaManager = metaManager;
  }

  public abstract boolean needCompaction() throws Exception;

  public abstract void compact() throws Exception;

  protected List<List<FragmentMeta>> packFragmentsByGroup(List<FragmentMeta> fragmentMetas) {
    // 排序以减少算法时间复杂度
    fragmentMetas.sort(
        (o1, o2) -> {
          // 先按照时间维度排序，再按照时间序列维度排序
          if (o1.getKeyInterval().getStartKey() == o2.getKeyInterval().getStartKey()) {
            if (o1.getColumnsInterval().getStartColumn() == null) {
              return -1;
            } else if (o2.getColumnsInterval().getStartColumn() == null) {
              return 1;
            } else {
              return o1.getColumnsInterval()
                  .getStartColumn()
                  .compareTo(o2.getColumnsInterval().getStartColumn());
            }
          } else {
            // 所有分片在时间维度上是统一的，因此只需要根据起始时间排序即可
            return Long.compare(
                o1.getKeyInterval().getStartKey(), o2.getKeyInterval().getStartKey());
          }
        });

    // 对筛选出来要合并的所有分片按连通性进行分组（同一组中的分片可以合并）
    List<List<FragmentMeta>> result = new ArrayList<>();
    List<FragmentMeta> lastFragmentGroup = new ArrayList<>();
    FragmentMeta lastFragment = null;
    for (FragmentMeta fragmentMeta : fragmentMetas) {
      if (lastFragment == null) {
        lastFragmentGroup.add(fragmentMeta);
      } else {
        if (isNext(lastFragment, fragmentMeta)) {
          lastFragmentGroup.add(fragmentMeta);
        } else {
          if (lastFragmentGroup.size() > 1) {
            result.add(lastFragmentGroup);
          }
          lastFragmentGroup = new ArrayList<>();
        }
      }
      lastFragment = fragmentMeta;
    }
    if (!lastFragmentGroup.isEmpty()) {
      result.add(lastFragmentGroup);
    }
    return result;
  }

  private boolean isNext(FragmentMeta firstFragment, FragmentMeta secondFragment) {
    if (firstFragment.getColumnsInterval().getEndColumn() == null
        || secondFragment.getColumnsInterval().getStartColumn() == null) {
      return false;
    } else {
      return firstFragment.getKeyInterval().equals(secondFragment.getKeyInterval())
          && firstFragment
              .getColumnsInterval()
              .getEndColumn()
              .equals(secondFragment.getColumnsInterval().getStartColumn());
    }
  }

  protected void compactFragmentGroupToTargetStorageUnit(
      List<FragmentMeta> fragmentGroup, StorageUnitMeta targetStorageUnit, long totalPoints)
      throws PhysicalException {
    String startTimeseries = fragmentGroup.get(0).getColumnsInterval().getStartColumn();
    String endTimeseries = fragmentGroup.get(0).getColumnsInterval().getEndColumn();
    long startTime = fragmentGroup.get(0).getKeyInterval().getStartKey();
    long endTime = fragmentGroup.get(0).getKeyInterval().getEndKey();

    for (FragmentMeta fragmentMeta : fragmentGroup) {
      // 找到新分片空间
      if (startTimeseries == null || fragmentMeta.getColumnsInterval().getStartColumn() == null) {
        startTimeseries = null;
      } else {
        startTimeseries =
            startTimeseries.compareTo(fragmentMeta.getColumnsInterval().getStartColumn()) > 0
                ? fragmentMeta.getColumnsInterval().getStartColumn()
                : startTimeseries;
      }

      if (endTimeseries == null || fragmentMeta.getColumnsInterval().getEndColumn() == null) {
        endTimeseries = null;
      } else {
        endTimeseries =
            endTimeseries.compareTo(fragmentMeta.getColumnsInterval().getEndColumn()) > 0
                ? endTimeseries
                : fragmentMeta.getColumnsInterval().getEndColumn();
      }

      String storageUnitId = fragmentMeta.getMasterStorageUnitId();
      if (!storageUnitId.equals(targetStorageUnit.getId())) {
        // 重写该分片的数据
        Set<String> pathRegexSet = new HashSet<>();
        ShowColumns showColumns =
            new ShowColumns(new GlobalSource(), pathRegexSet, null, Integer.MAX_VALUE, 0);

        SortedSet<String> pathSet = new TreeSet<>();
        try (RowStream rowStream = physicalEngine.execute(new RequestContext(), showColumns)) {
          while (rowStream != null && rowStream.hasNext()) {
            Row row = rowStream.next();
            String timeSeries = new String((byte[]) row.getValue(0));
            if (timeSeries.contains("{") && timeSeries.contains("}")) {
              timeSeries = timeSeries.split("\\{")[0];
            }
            if (fragmentMeta.getColumnsInterval().isContain(timeSeries)) {
              pathSet.add(timeSeries);
            }
          }
        }

        // TODO: pathSet 不应为空，目前通过判空做补丁，有待深入处理
        if (!pathSet.isEmpty()) {
          Migration migration =
              new Migration(
                  new GlobalSource(), fragmentMeta, new ArrayList<>(pathSet), targetStorageUnit);
          try (RowStream ignored = physicalEngine.execute(new RequestContext(), migration)) {}
        }
      }
    }
    // TODO add write lock
    // 创建新分片
    FragmentMeta newFragment =
        new FragmentMeta(startTimeseries, endTimeseries, startTime, endTime, targetStorageUnit);
    metaManager.addFragment(newFragment);
    // 更新存储点数信息
    metaManager.updateFragmentPoints(newFragment, totalPoints);

    for (FragmentMeta fragmentMeta : fragmentGroup) {
      metaManager.removeFragment(fragmentMeta);
    }
    // TODO release write lock

    for (FragmentMeta fragmentMeta : fragmentGroup) {
      String storageUnitId = fragmentMeta.getMasterStorageUnitId();
      if (!storageUnitId.equals(targetStorageUnit.getId())) {
        // 删除原分片节点数据
        Delete delete =
            new Delete(
                new FragmentSource(fragmentMeta), new ArrayList<>(), new ArrayList<>(), null);
        physicalEngine.execute(new RequestContext(), delete);
      }
    }
  }
}
