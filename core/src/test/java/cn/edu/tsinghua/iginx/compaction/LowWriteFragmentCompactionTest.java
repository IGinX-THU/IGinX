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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.MetaManagerMock;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class LowWriteFragmentCompactionTest {

  private List<FragmentMeta> fragmentMetaSet = new ArrayList<>();
  private Map<FragmentMeta, Long> fragmentHeatWriteMap = new HashMap<>();
  private Map<FragmentMeta, Long> fragmentHeatReadMap = new HashMap<>();
  private Map<FragmentMeta, Long> fragmentMetaPointsMap = new HashMap<>();
  private PhysicalEngine physicalEngine = new PhysicalEngineMock();
  private IMetaManager metaManager = MetaManagerMock.getInstance();
  private LowWriteFragmentCompaction compaction =
      new LowWriteFragmentCompaction(physicalEngine, metaManager);

  @Before
  public void setUp() {
    SnowFlakeUtils.init(0);

    StorageUnitMeta storageUnitMeta1 = new StorageUnitMeta("1", 1);
    StorageUnitMeta storageUnitMeta2 = new StorageUnitMeta("2", 2);
    FragmentMeta fragmentMeta1 =
        new FragmentMeta("root.a.b", "root.z", 0L, 1000L, storageUnitMeta1);
    FragmentMeta fragmentMeta2 =
        new FragmentMeta("root.z", "root.z.a", 0L, 1000L, storageUnitMeta1);
    FragmentMeta fragmentMeta3 =
        new FragmentMeta("root.z.a", "root.z.z", 0L, 1000L, storageUnitMeta1);
    FragmentMeta fragmentMeta4 = new FragmentMeta("root.z.z", null, 0L, 1000L, storageUnitMeta2);
    fragmentMetaSet.add(fragmentMeta1);
    fragmentMetaSet.add(fragmentMeta2);
    fragmentMetaSet.add(fragmentMeta3);
    fragmentMetaSet.add(fragmentMeta4);
    fragmentHeatWriteMap.put(fragmentMeta1, 5000L);
    fragmentHeatWriteMap.put(fragmentMeta2, 10000L);
    fragmentHeatReadMap.put(fragmentMeta1, 50000L);
    fragmentHeatReadMap.put(fragmentMeta2, 10000L);
    fragmentHeatReadMap.put(fragmentMeta3, 10000L);
    fragmentHeatReadMap.put(fragmentMeta4, 10000L);
    fragmentMetaPointsMap.put(fragmentMeta1, 10000L);
    fragmentMetaPointsMap.put(fragmentMeta1, 10000L);
    fragmentMetaPointsMap.put(fragmentMeta1, 5000L);
    fragmentMetaPointsMap.put(fragmentMeta1, 10000L);

    ConfigDescriptor.getInstance().getConfig().setFragmentCompactionWriteThreshold(1000);
    ConfigDescriptor.getInstance().getConfig().setFragmentCompactionReadRatioThreshold(0.8);
    ConfigDescriptor.getInstance().getConfig().setFragmentCompactionReadThreshold(50);
  }

  @Test
  public void testFragmentSelection() {
    List<List<FragmentMeta>> toCompactFragmentGroups =
        compaction.judgeCompaction(fragmentMetaSet, fragmentHeatWriteMap, fragmentHeatReadMap);
    assertEquals(toCompactFragmentGroups.size(), 1);
    assertEquals(toCompactFragmentGroups.get(0).size(), 2);
  }

  @Test
  public void testFragmentCompact() throws PhysicalException {
    List<List<FragmentMeta>> toCompactFragmentGroups =
        compaction.judgeCompaction(fragmentMetaSet, fragmentHeatWriteMap, fragmentHeatReadMap);
    compaction.executeCompaction(toCompactFragmentGroups, fragmentMetaPointsMap);
    List<FragmentMeta> fragmentMetas = metaManager.getFragments();
    assertEquals(fragmentMetas.size(), 1);
    assertEquals(fragmentMetas.get(0).getColumnsInterval().getStartColumn(), "root.z.a");
    assertNull(fragmentMetas.get(0).getColumnsInterval().getEndColumn());
    assertEquals(fragmentMetas.get(0).getMasterStorageUnit().getStorageEngineId(), 1);
    assertEquals(fragmentMetas.get(0).getKeyInterval().getStartKey(), 0);
    assertEquals(fragmentMetas.get(0).getKeyInterval().getEndKey(), 1000);
    metaManager.removeFragment(fragmentMetas.get(0));
  }
}
