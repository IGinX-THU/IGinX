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
package cn.edu.tsinghua.iginx.policy;

import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.List;

public interface IPolicy {

  void notify(DataStatement statement);

  void init(IMetaManager iMetaManager);

  StorageEngineChangeHook getStorageEngineChangeHook();

  Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateInitialFragmentsAndStorageUnits(
      DataStatement statement);

  Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateFragmentsAndStorageUnits(
      DataStatement statement);

  Pair<FragmentMeta, StorageUnitMeta> generateFragmentAndStorageUnitByColumnsIntervalAndKeyInterval(
      String startPath, String endPath, long startKey, long endKey, List<Long> storageEngineList);

  boolean isNeedReAllocate();

  void setNeedReAllocate(boolean needReAllocate);
}
