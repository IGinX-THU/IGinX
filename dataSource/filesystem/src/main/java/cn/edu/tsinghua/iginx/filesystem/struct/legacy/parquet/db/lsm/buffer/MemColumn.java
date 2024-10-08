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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.lsm.buffer;

import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.lsm.buffer.chunk.Chunk;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.lsm.buffer.chunk.IndexedChunk;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.iterator.DedupIterator;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.iterator.StableMergeIterator;
import com.google.common.collect.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;

@ThreadSafe
public class MemColumn implements AutoCloseable {

  private final int maxChunkValueCount;
  private final int minChunkValueCount;
  private final List<ChunkSnapshotHolder> compactedChunkSnapshots = new ArrayList<>();
  private final ChunkHolder active;

  public MemColumn(
      IndexedChunk.Factory factory,
      BufferAllocator allocator,
      int maxChunkValueCount,
      int minChunkValueCount) {
    Preconditions.checkNotNull(allocator);
    Preconditions.checkNotNull(factory);
    Preconditions.checkArgument(maxChunkValueCount > 0);
    Preconditions.checkArgument(minChunkValueCount > 0);

    this.active = new ChunkHolder(factory, allocator);
    this.maxChunkValueCount = maxChunkValueCount;
    this.minChunkValueCount = minChunkValueCount;
  }

  // TODO: make use of ValueFilter
  public synchronized Snapshot snapshot(RangeSet<Long> ranges, BufferAllocator allocator) {
    active.refresh(compactedChunkSnapshots);
    return createSnapshot(ranges, compactedChunkSnapshots, allocator);
  }

  public synchronized Snapshot snapshot(BufferAllocator allocator) {
    return snapshot(ImmutableRangeSet.of(Range.all()), allocator);
  }

  private static Snapshot createSnapshot(
      RangeSet<Long> ranges,
      List<ChunkSnapshotHolder> snapshotHolders,
      @Nullable BufferAllocator allocator) {
    List<ChunkSnapshotHolder> snapshots = new ArrayList<>();
    for (ChunkSnapshotHolder snapshot : snapshotHolders) {
      ChunkSnapshotHolder filtered;
      if (allocator != null) {
        filtered = snapshot.slice(allocator);
      } else {
        filtered = snapshot.slice();
      }
      filtered.delete(ranges.complement());
      if (filtered.isEmpty()) {
        filtered.close();
      } else {
        snapshots.add(filtered);
      }
    }
    return new Snapshot(snapshots);
  }

  public synchronized void store(Chunk.Snapshot snapshot) {
    int length = snapshot.getValueCount();
    if (length < minChunkValueCount) {
      copyStore(snapshot, 0, length);
    } else {
      splitStore(snapshot, 0, length);
    }
  }

  public synchronized void splitStore(Chunk.Snapshot snapshot, int offset, int length) {
    int activeValueCount = active.getValueCount();
    int padding = minChunkValueCount - active.getValueCount();
    if (activeValueCount > 0) {
      if (padding > 0) {
        if (padding >= length) {
          active.store(snapshot, offset, length);
          return;
        }
        active.store(snapshot, offset, padding);
        offset += padding;
        length -= padding;
      }
      compact();
    }

    if (length >= minChunkValueCount) {
      compactedChunkSnapshots.add(active.sorted(snapshot, offset, length));
    } else {
      active.store(snapshot, offset, length);
    }
  }

  public synchronized void copyStore(Chunk.Snapshot snapshot, int offset, int length) {
    for (int written = offset; written != length; ) {
      int free = maxChunkValueCount - active.getValueCount();
      if (free == 0) {
        compact();
      }
      int toWrite = Math.min(free, length - written);
      active.store(snapshot, written, toWrite);
      written += toWrite;
    }
  }

  public synchronized void delete(RangeSet<Long> ranges) {
    active.delete(ranges);
    for (ChunkSnapshotHolder snapshot : compactedChunkSnapshots) {
      snapshot.delete(ranges);
    }
  }

  @Override
  public synchronized void close() {
    compactedChunkSnapshots.forEach(ChunkSnapshotHolder::close);
    compactedChunkSnapshots.clear();
    active.close();
  }

  public synchronized void compact() {
    active.refresh(compactedChunkSnapshots);
    active.reset();
  }

  public static class Snapshot implements AutoCloseable, Iterable<Map.Entry<Long, Object>> {

    private final List<ChunkSnapshotHolder> snapshots;

    Snapshot(@WillCloseWhenClosed List<ChunkSnapshotHolder> snapshots) {
      this.snapshots = Preconditions.checkNotNull(snapshots);
    }

    public Snapshot slice(RangeSet<Long> ranges, BufferAllocator allocator) {
      return createSnapshot(ranges, snapshots, allocator);
    }

    public Snapshot slice(RangeSet<Long> ranges) {
      return createSnapshot(ranges, snapshots, null);
    }

    @Override
    public void close() {
      snapshots.forEach(ChunkSnapshotHolder::close);
      snapshots.clear();
    }

    @Override
    @Nonnull
    public Iterator<Map.Entry<Long, Object>> iterator() {
      Iterator<Map.Entry<Long, Object>> mergedIterator =
          new StableMergeIterator<>(getIterators(), Map.Entry.comparingByKey());
      return new DedupIterator<>(mergedIterator, Map.Entry::getKey);
    }

    private List<Iterator<Map.Entry<Long, Object>>> getIterators() {
      // TODO: 对 Iterator 进行 concat 减少 StableMergeIterator 内 queue 中的项数
      //       目前使用贪心算法，可能不是最优解
      List<RangeMap<Long, Integer>> rangeGroups = new ArrayList<>();
      RangeMap<Long, Integer> currentRanges = TreeRangeMap.create();
      for (int i = 0; i < snapshots.size(); i++) {
        ChunkSnapshotHolder snapshot = snapshots.get(i);
        Range<Long> keyRange = snapshot.getKeyRange();
        if (currentRanges.subRangeMap(keyRange).asMapOfRanges().isEmpty()) {
          currentRanges.put(keyRange, i);
        } else {
          rangeGroups.add(currentRanges);
          currentRanges = TreeRangeMap.create();
          currentRanges.put(keyRange, i);
        }
      }
      rangeGroups.add(currentRanges);
      List<Iterator<Map.Entry<Long, Object>>> iterators = new ArrayList<>();
      for (RangeMap<Long, Integer> rangeMap : rangeGroups) {
        List<Iterator<Map.Entry<Long, Object>>> groupIterators = new ArrayList<>();
        for (int i : rangeMap.asMapOfRanges().values()) {
          groupIterators.add(snapshots.get(i).iterator());
        }
        iterators.add(Iterators.concat(groupIterators.iterator()));
      }
      return iterators;
    }

    public RangeSet<Long> getRanges() {
      RangeSet<Long> range = TreeRangeSet.create();
      snapshots.forEach(snapshot -> range.add(snapshot.getKeyRange()));
      return range;
    }
  }

  private static class ChunkSnapshotHolder
      implements AutoCloseable, Iterable<Map.Entry<Long, Object>> {

    private final Chunk.Snapshot snapshot;
    private RangeSet<Long> mask;

    public ChunkSnapshotHolder(@WillCloseWhenClosed Chunk.Snapshot snapshot) {
      this(snapshot, null);
    }

    private ChunkSnapshotHolder(@WillCloseWhenClosed Chunk.Snapshot snapshot, RangeSet<Long> mask) {
      Preconditions.checkArgument(snapshot.getValueCount() > 0);
      this.snapshot = snapshot;
      if (mask != null) {
        this.mask = TreeRangeSet.create(mask);
      } else {
        this.mask = null;
      }
    }

    public boolean isEmpty() {
      return mask != null && mask.isEmpty();
    }

    public void delete(RangeSet<Long> ranges) {
      if (mask == null) {
        Range<Long> fullRange = getKeyRange(snapshot);
        if (!ranges.intersects(fullRange)) {
          return;
        }
        mask = TreeRangeSet.create();
        mask.add(fullRange);
      }
      mask.removeAll(ranges);
    }

    public Range<Long> getKeyRange() {
      if (mask == null) {
        return getKeyRange(snapshot);
      }
      if (mask.isEmpty()) {
        return Range.closedOpen(0L, 0L);
      }
      return mask.span();
    }

    private static Range<Long> getKeyRange(Chunk.Snapshot snapshot) {
      return Range.closed(snapshot.getKey(0), snapshot.getKey(snapshot.getValueCount() - 1));
    }

    @Override
    public void close() {
      snapshot.close();
    }

    public ChunkSnapshotHolder slice(BufferAllocator allocator) {
      return new ChunkSnapshotHolder(snapshot.slice(allocator), mask);
    }

    public ChunkSnapshotHolder slice() {
      return new ChunkSnapshotHolder(snapshot.slice(), mask);
    }

    @Override
    @Nonnull
    public Iterator<Map.Entry<Long, Object>> iterator() {
      Iterator<Map.Entry<Long, Object>> iterator = snapshot.iterator();
      if (mask == null) {
        return iterator;
      }
      return Iterators.filter(iterator, entry -> mask.contains(entry.getKey()));
    }
  }

  private static class ChunkHolder implements AutoCloseable {
    private final IndexedChunk.Factory factory;
    private final BufferAllocator allocator;
    private IndexedChunk activeChunk = null;
    private boolean isDirty = false;
    private boolean hasOld = false;

    ChunkHolder(IndexedChunk.Factory factory, BufferAllocator allocator) {
      this.factory = factory;
      this.allocator = allocator;
    }

    public int getValueCount() {
      return activeChunk == null ? 0 : activeChunk.getValueCount();
    }

    public ChunkSnapshotHolder sorted(Chunk.Snapshot snapshot, int offset, int length) {
      Chunk.Snapshot sorted = factory.sorted(snapshot.slice(offset, length, allocator), allocator);
      return new ChunkSnapshotHolder(sorted);
    }

    public void store(Chunk.Snapshot data) {
      if (data.getValueCount() == 0) {
        return;
      }
      if (activeChunk == null) {
        activeChunk = factory.wrap(data, allocator);
      }
      activeChunk.store(data);
      isDirty = true;
    }

    public void store(Chunk.Snapshot snapshot, int offset, int length) {
      try (Chunk.Snapshot slice = snapshot.slice(offset, length, allocator)) {
        store(slice);
      }
    }

    public void delete(RangeSet<Long> ranges) {
      if (activeChunk != null) {
        activeChunk.delete(ranges);
      }
    }

    public void refresh(List<ChunkSnapshotHolder> compactedChunkSnapshots) {
      if (!isDirty) {
        return;
      }
      isDirty = false;

      if (hasOld) {
        int offset = compactedChunkSnapshots.size() - 1;
        compactedChunkSnapshots.remove(offset).close();
      }

      Chunk.Snapshot snapshot = activeChunk.snapshot(allocator);
      if (snapshot.getValueCount() > 0) {
        compactedChunkSnapshots.add(new ChunkSnapshotHolder(snapshot));
        hasOld = true;
      } else {
        snapshot.close();
        reset();
      }
    }

    public void reset() {
      if (activeChunk != null) {
        activeChunk.close();
        activeChunk = null;
      }
      isDirty = false;
      hasOld = false;
    }

    public void close() {
      reset();
    }
  }
}
