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
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractPolicy implements IPolicy {
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

  @Override
  public boolean isNeedReAllocate() {
    return needReAllocate.getAndSet(false);
  }

  @Override
  public void setNeedReAllocate(boolean needReAllocate) {
    this.needReAllocate.set(needReAllocate);
  }
}
