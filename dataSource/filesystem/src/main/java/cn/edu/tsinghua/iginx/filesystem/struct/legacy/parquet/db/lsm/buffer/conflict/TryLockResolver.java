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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.arrow.vector.types.pojo.Field;

public class TryLockResolver implements ConflictResolver {

  protected final ConcurrentHashMap<Field, Lock> locks = new ConcurrentHashMap<>();

  @Override
  public void reset() {
    locks.clear();
  }

  @Override
  public void append(MemTable activeTable, Iterable<Chunk.Snapshot> data) {
    List<Chunk.Snapshot> blocked = tryAppend(activeTable, data);

    Collections.shuffle(blocked);

    awaitAppend(activeTable, blocked);
  }

  protected Lock getLock(Field field) {
    return locks.computeIfAbsent(field, f -> new ReentrantLock());
  }

  protected Lock getLock(Chunk.Snapshot data) {
    return getLock(data.getField());
  }

  protected List<Chunk.Snapshot> tryAppend(MemTable activeTable, Iterable<Chunk.Snapshot> data) {
    List<Chunk.Snapshot> blocked = new ArrayList<>();

    for (Chunk.Snapshot snapshot : data) {
      Lock lock = getLock(snapshot);
      if (lock.tryLock()) {
        try {
          activeTable.store(snapshot);
        } finally {
          lock.unlock();
        }
      } else {
        blocked.add(snapshot);
      }
    }
    return blocked;
  }

  protected void awaitAppend(MemTable activeTable, Iterable<Chunk.Snapshot> data) {
    for (Chunk.Snapshot snapshot : data) {
      Lock lock = getLock(snapshot);
      lock.lock();
      try {
        activeTable.store(snapshot);
      } finally {
        lock.unlock();
      }
    }
  }
}
