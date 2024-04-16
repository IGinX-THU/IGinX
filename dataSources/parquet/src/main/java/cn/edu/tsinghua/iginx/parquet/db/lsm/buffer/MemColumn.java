package cn.edu.tsinghua.iginx.parquet.db.lsm.buffer;

import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk.IndexedChunk;
import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk.UnorderedChunk;
import cn.edu.tsinghua.iginx.parquet.util.iterator.DedupIterator;
import cn.edu.tsinghua.iginx.parquet.util.iterator.StableMergeIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.*;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;

@ThreadSafe
public class MemColumn implements AutoCloseable {

  private final int maxChunkValueCount;
  private final List<ChunkSnapshotHolder> compactedChunkSnapshots = new ArrayList<>();
  private final ChunkHolder active;

  public MemColumn(
      IndexedChunk.Factory factory, BufferAllocator allocator, int maxChunkValueCount) {
    Preconditions.checkNotNull(allocator);
    Preconditions.checkNotNull(factory);
    Preconditions.checkArgument(maxChunkValueCount > 0);

    this.active = new ChunkHolder(factory, allocator);
    this.maxChunkValueCount = maxChunkValueCount;
  }

  // TODO: make use of ValueFilter
  public synchronized Snapshot snapshot(RangeSet<Long> ranges, BufferAllocator allocator) {
    active.refresh(compactedChunkSnapshots);
    return createSnapshot(ranges, compactedChunkSnapshots, allocator);
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

  public synchronized void store(UnorderedChunk.Snapshot snapshot) {
    int total = snapshot.getValueCount();
    for (int written = 0; written != total; ) {
      int free = maxChunkValueCount - active.getValueCount();
      if (free == 0) {
        active.refresh(compactedChunkSnapshots);
        active.reset();
      }
      int toWrite = Math.min(free, total - written);
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
    }

    @Override
    @Nonnull
    public Iterator<Map.Entry<Long, Object>> iterator() {
      Iterable<Iterator<Map.Entry<Long, Object>>> iterators =
          Iterables.transform(snapshots, ChunkSnapshotHolder::iterator);
      Iterator<Map.Entry<Long, Object>> mergedIterator =
          new StableMergeIterator<>(iterators, Map.Entry.comparingByKey());
      return new DedupIterator<>(mergedIterator, Map.Entry::getKey);
    }

    public RangeSet<Long> getRanges() {
      RangeSet<Long> range = TreeRangeSet.create();
      snapshots.forEach(snapshot -> range.addAll(snapshot.mask));
      return range;
    }
  }

  private static class ChunkSnapshotHolder
      implements AutoCloseable, Iterable<Map.Entry<Long, Object>> {

    private final IndexedChunk.IndexedSnapshot snapshot;
    protected final RangeSet<Long> mask;

    public ChunkSnapshotHolder(@WillCloseWhenClosed IndexedChunk.IndexedSnapshot snapshot) {
      this(snapshot, TreeRangeSet.create());
      mask.add(snapshot.getKeyRange());
    }

    private ChunkSnapshotHolder(
        @WillCloseWhenClosed IndexedChunk.IndexedSnapshot snapshot, RangeSet<Long> mask) {
      this.snapshot = snapshot;
      this.mask = mask;
    }

    public boolean isEmpty() {
      return mask.isEmpty();
    }

    public void delete(RangeSet<Long> ranges) {
      mask.removeAll(ranges);
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

    public void store(UnorderedChunk.Snapshot data) {
      if (activeChunk == null) {
        activeChunk = factory.like(data, allocator);
      }
      activeChunk.store(data);
      isDirty = true;
    }

    public void store(UnorderedChunk.Snapshot snapshot, int offset, int length) {
      try (UnorderedChunk.Snapshot slice = snapshot.slice(offset, length, allocator)) {
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

      if (hasOld) {
        int offset = compactedChunkSnapshots.size() - 1;
        compactedChunkSnapshots.remove(offset).close();
      }
      ChunkSnapshotHolder snapshot = new ChunkSnapshotHolder(activeChunk.snapshot(allocator));
      compactedChunkSnapshots.add(snapshot);

      hasOld = true;
      isDirty = false;
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
