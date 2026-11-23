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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.metadata;

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ColumnTableIndex {
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final Map<Long, Range<Long>> tableRange = new HashMap<>();
  private final DataType type;

  public ColumnTableIndex(DataType type) {
    this.type = type;
  }

  public void addTable(long id, Range<Long> range) {
    lock.writeLock().lock();
    try {
      if (this.tableRange.containsKey(id)) {
        throw new StorageRuntimeException("table " + id + " already exists");
      }
      this.tableRange.put(id, range);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Set<Long> find(RangeSet<Long> ranges) {
    Set<Long> result = new HashSet<>();
    lock.readLock().lock();
    try {
      for (Map.Entry<Long, Range<Long>> entry : tableRange.entrySet()) {
        if (ranges.intersects(entry.getValue())) {
          result.add(entry.getKey());
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  public void delete(RangeSet<Long> keyRangeSet) {
    lock.writeLock().lock();
    try {
      RangeSet<Long> validRanges = keyRangeSet.complement();
      Iterator<Map.Entry<Long, Range<Long>>> iterator = tableRange.entrySet().iterator();
      Map<Long, Range<Long>> overlap = new HashMap<>();
      while (iterator.hasNext()) {
        Map.Entry<Long, Range<Long>> entry = iterator.next();
        if (!keyRangeSet.intersects(entry.getValue())) {
          continue;
        }
        if (keyRangeSet.encloses(entry.getValue())) {
          iterator.remove();
        } else {
          overlap.put(entry.getKey(), validRanges.subRangeSet(entry.getValue()).span());
        }
      }
      tableRange.putAll(overlap);
    } finally {
      lock.writeLock().unlock();
    }
  }
}
