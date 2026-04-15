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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer;

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.NoexceptAutoCloseable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import it.unimi.dsi.fastutil.shorts.ShortArrays;
import it.unimi.dsi.fastutil.shorts.ShortComparator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.pojo.Field;

@ThreadSafe
public class MemSubTable implements NoexceptAutoCloseable {

  private ImmutableList<Field> fields;
  private final BufferAllocator allocator;
  private final int maxChunkValueCount;
  private List<SortedChunkSnapshot> snapshots;
  private MemBatch active;
  private SortedChunkSnapshot activeChunkSnapshot;

  public MemSubTable(
      ImmutableList<Field> fields, BufferAllocator allocator, int maxChunkValueCount) {
    this(fields, allocator, maxChunkValueCount, new ArrayList<>(), null, null);
  }

  private MemSubTable(
      ImmutableList<Field> fields,
      BufferAllocator allocator,
      int maxChunkValueCount,
      List<SortedChunkSnapshot> snapshots,
      MemBatch active,
      SortedChunkSnapshot activeChunkSnapshot) {
    Preconditions.checkNotNull(fields);
    Preconditions.checkNotNull(allocator);
    Preconditions.checkArgument(maxChunkValueCount > 0);
    this.fields = fields;
    this.allocator = allocator;
    this.maxChunkValueCount = maxChunkValueCount;
    this.snapshots = snapshots;
    this.active = active;
    this.activeChunkSnapshot = activeChunkSnapshot;
  }

  public synchronized ImmutableList<Field> getFields() {
    return fields;
  }

  public synchronized MemSubTable split(IntLinkedOpenHashSet fields, BufferAllocator allocator) {
    int[] remainingFields =
        IntStream.range(0, this.fields.size()).filter(i -> !fields.contains(i)).toArray();
    ImmutableList<Field> splitFields =
        fields.intStream().mapToObj(this.fields::get).collect(ImmutableList.toImmutableList());
    this.fields =
        Arrays.stream(remainingFields)
            .mapToObj(this.fields::get)
            .collect(ImmutableList.toImmutableList());

    int[] fieldsArray = fields.toIntArray();
    List<SortedChunkSnapshot> splitSnapshots = new ArrayList<>();
    List<SortedChunkSnapshot> remainingSnapshots = new ArrayList<>();
    for (SortedChunkSnapshot snapshot : this.snapshots) {
      splitSnapshots.add(snapshot.slice(fieldsArray, allocator));
      remainingSnapshots.add(snapshot.slice(remainingFields, this.allocator));
      snapshot.close();
    }
    this.snapshots = remainingSnapshots;

    SortedChunkSnapshot splitActiveChunkSnapshot = null;
    if (this.activeChunkSnapshot != null) {
      splitActiveChunkSnapshot = this.activeChunkSnapshot.slice(fieldsArray, allocator);
      SortedChunkSnapshot remainingActiveChunkSnapshot =
          this.activeChunkSnapshot.slice(remainingFields, this.allocator);
      this.activeChunkSnapshot.close();
      this.activeChunkSnapshot = remainingActiveChunkSnapshot;
    }

    MemBatch splitActive = null;
    if (active != null) {
      splitActive = active.split(fields, allocator);
    }

    return new MemSubTable(
        splitFields,
        allocator,
        maxChunkValueCount,
        splitSnapshots,
        splitActive,
        splitActiveChunkSnapshot);
  }

  public synchronized Snapshot snapshot(BufferAllocator allocator) {
    return snapshot(IntStream.range(0, fields.size()).toArray(), allocator);
  }

  public synchronized Snapshot snapshot(int[] fields, BufferAllocator allocator) {
    if (active != null) {
      if (activeChunkSnapshot == null) {
        try (MemBatch.Snapshot activeSnapshot = active.snapshot(allocator)) {
          activeChunkSnapshot = new SortedChunkSnapshot(activeSnapshot, allocator);
        }
      }
    }
    List<SortedChunkSnapshot> chunkSnapshots = new ArrayList<>(snapshots);
    if (activeChunkSnapshot != null) {
      chunkSnapshots.add(activeChunkSnapshot);
    }

    return new Snapshot(
        Arrays.stream(fields).mapToObj(this.fields::get).collect(ImmutableList.toImmutableList()),
        chunkSnapshots.stream()
            .map(s -> s.slice(fields, allocator))
            .collect(ImmutableList.toImmutableList()));
  }

  public synchronized void append(MemBatch.Snapshot batch) {
    int start = 0;
    int end = batch.getValueCount();
    while (start < end) {
      int activeValueCount = active == null ? 0 : active.getValueCount();
      int length = Math.min(end - start, maxChunkValueCount - activeValueCount);
      try (MemBatch.Snapshot slice = batch.slice(start, length, allocator)) {
        if (length == maxChunkValueCount) {
          snapshots.add(new SortedChunkSnapshot(slice, allocator));
        } else {
          if (active == null) {
            active = new MemBatch(fields, length, allocator);
          }
          active.append(slice);
          if (activeChunkSnapshot != null) {
            activeChunkSnapshot.close();
            activeChunkSnapshot = null;
          }
          if (active.getValueCount() >= maxChunkValueCount) {
            try (MemBatch.Snapshot activeSnapshot = active.snapshot(allocator)) {
              snapshots.add(new SortedChunkSnapshot(activeSnapshot, allocator));
            }
            active.close();
            active = null;
          }
        }
      }
      start += length;
    }
  }

  @Override
  public synchronized void close() {
    snapshots.forEach(SortedChunkSnapshot::close);
    snapshots.clear();
    if (active != null) {
      active.close();
    }
    if (activeChunkSnapshot != null) {
      activeChunkSnapshot.close();
    }
  }

  public static class Snapshot implements NoexceptAutoCloseable {
    private final ImmutableList<Field> fields;
    private final ImmutableList<SortedChunkSnapshot> chunks;

    Snapshot(
        ImmutableList<Field> fields,
        @WillCloseWhenClosed ImmutableList<SortedChunkSnapshot> chunks) {
      this.fields = Objects.requireNonNull(fields);
      this.chunks = Objects.requireNonNull(chunks);
    }

    public List<Field> getFields() {
      return fields;
    }

    public List<SortedChunkSnapshot> getChunks() {
      return chunks;
    }

    @Override
    public void close() {
      chunks.forEach(SortedChunkSnapshot::close);
    }
  }

  public static class SortedChunkSnapshot implements NoexceptAutoCloseable {

    private final MemBatch.Snapshot snapshot;
    private final ShareMeta shareMeta;

    private SortedChunkSnapshot(
        MemBatch.Snapshot snapshot, int[] fields, BufferAllocator allocator, ShareMeta shareMeta) {
      Preconditions.checkArgument(snapshot.getValueCount() > 0);
      Preconditions.checkArgument(snapshot.getValueCount() <= Short.MAX_VALUE);
      this.snapshot = snapshot.slice(fields, allocator);
      this.shareMeta = shareMeta;
    }

    SortedChunkSnapshot(MemBatch.Snapshot snapshot, BufferAllocator allocator) {
      this(
          snapshot,
          IntStream.range(0, snapshot.getFieldVectors().size()).toArray(),
          allocator,
          new ShareMeta());
    }

    public SortedChunkSnapshot slice(int[] fields, BufferAllocator allocator) {
      return new SortedChunkSnapshot(snapshot, fields, allocator, shareMeta);
    }

    public MemBatch.Snapshot getSnapshot() {
      return snapshot;
    }

    public short[] getIndex() {
      return shareMeta.getKeyIndex(snapshot.getKeyVector());
    }

    public Range<Long> getKeyRange() {
      return shareMeta.getKeyRange(snapshot.getKeyVector());
    }

    @Override
    public void close() {
      snapshot.close();
    }

    private static class ShareMeta {
      private short[] index = null;
      private Range<Long> keyRange = null;

      public synchronized short[] getKeyIndex(BigIntVector keyVector) {
        if (index == null) {
          int valueCount = keyVector.getValueCount();
          index = new short[valueCount];
          for (short i = 0; i < valueCount; i++) {
            index[i] = i;
          }
          ShortArrays.stableSort(index, ShortComparator.comparingLong(keyVector::getValueAsLong));
        }
        return index;
      }

      public synchronized Range<Long> getKeyRange(BigIntVector keyVector) {
        if (keyRange == null) {
          short[] keyIndex = getKeyIndex(keyVector);
          long minKey = Long.MAX_VALUE;
          long maxKey = Long.MIN_VALUE;
          if (keyIndex.length > 0) {
            minKey = keyVector.getValueAsLong(keyIndex[0]);
            maxKey = keyVector.getValueAsLong(keyIndex[keyIndex.length - 1]);
          }
          keyRange = Range.closed(minKey, maxKey);
        }
        return keyRange;
      }
    }
  }
}
