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
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.lsm.buffer.chunk;

import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.arrow.ArrowVectors;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;

@ThreadSafe
public class NoIndexChunk extends IndexedChunk {

  private final TreeMap<Integer, RangeSet<Long>> tombstone = new TreeMap<>();

  private int valueCount = 0;

  public NoIndexChunk(@WillCloseWhenClosed Chunk chunk, BufferAllocator allocator) {
    super(chunk, allocator);
    tombstone.put(0, TreeRangeSet.create());
  }

  @Override
  protected IntVector indexOf(Snapshot snapshot, BufferAllocator allocator) {
    if (tombstone.get(0).isEmpty()) {
      if (ArrowVectors.isStrictlyOrdered(snapshot.keys)) {
        return null;
      }
    }

    IntVector indexes = ArrowVectors.stableSortIndexes(snapshot.keys, allocator);
    ArrowVectors.dedupSortedIndexes(snapshot.keys, indexes);
    if (!tombstone.get(0).isEmpty()) {
      ArrowVectors.filter(indexes, i -> !isDeleted(snapshot, i));
    }
    return indexes;
  }

  private boolean isDeleted(Snapshot snapshot, int index) {
    Map.Entry<Integer, RangeSet<Long>> entry = tombstone.higherEntry(index);
    if (entry == null) {
      return false;
    }
    RangeSet<Long> deleted = entry.getValue();
    long key = snapshot.getKey(index);
    return deleted.contains(key);
  }

  @Override
  protected void updateIndex(Snapshot data, int offset) {
    valueCount = offset + data.getValueCount();
  }

  @Override
  protected void deleteIndex(RangeSet<Long> rangeSet) {
    tombstone.computeIfAbsent(valueCount, k -> TreeRangeSet.create()).addAll(rangeSet);
    tombstone.values().forEach(r -> r.addAll(rangeSet));
  }
}
