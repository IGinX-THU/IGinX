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
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.lsm.buffer.conflict;

import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.lsm.buffer.MemTable;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.lsm.buffer.chunk.Chunk;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RecursiveTryLockResolver extends TryLockResolver {

  @Override
  public void append(MemTable activeTable, Iterable<Chunk.Snapshot> data) {
    List<Chunk.Snapshot> toStore = new ArrayList<>();
    data.forEach(toStore::add);
    while (!toStore.isEmpty()) {
      List<Chunk.Snapshot> blocked = tryAppend(activeTable, toStore);
      if (blocked.size() * 2 > toStore.size()) {
        Collections.shuffle(toStore);
        awaitAppend(activeTable, toStore);
        break;
      }
      toStore = blocked;
    }
  }
}
