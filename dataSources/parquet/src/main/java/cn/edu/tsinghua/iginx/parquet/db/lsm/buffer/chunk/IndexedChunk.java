package cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk;

import cn.edu.tsinghua.iginx.parquet.util.arrow.ArrowVectors;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import javax.annotation.Nullable;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.reader.BigIntReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;

@ThreadSafe
public abstract class IndexedChunk extends UnorderedChunk {

  protected final BufferAllocator allocator;

  protected IndexedChunk(@WillCloseWhenClosed UnorderedChunk chunk, BufferAllocator allocator) {
    super(chunk.keys, chunk.values);
    this.allocator = allocator;
  }

  @Override
  public synchronized IndexedSnapshot snapshot(BufferAllocator allocator) {
    Snapshot snapshot = super.snapshot(allocator);
    IntVector indexes = indexOf(snapshot, allocator);
    return new IndexedSnapshot(snapshot, indexes);
  }

  @Override
  public synchronized int store(Snapshot data) {
    int offset = super.store(data);
    updateIndex(data, offset);
    return offset;
  }

  public synchronized void delete(RangeSet<Long> rangeSet) {
    Preconditions.checkNotNull(rangeSet);
    deleteIndex(rangeSet);
  }

  @Override
  public synchronized void close() {
    super.close();
  }

  protected abstract IntVector indexOf(Snapshot snapshot, BufferAllocator allocator);

  protected abstract void updateIndex(Snapshot data, int offset);

  protected abstract void deleteIndex(RangeSet<Long> rangeSet);

  public interface Factory {
    IndexedChunk wrap(@WillCloseWhenClosed UnorderedChunk chunk, BufferAllocator allocator);

    default IndexedChunk like(Snapshot snapshot, BufferAllocator allocator) {
      return wrap(UnorderedChunk.like(snapshot, allocator), allocator);
    }
  }

  @Immutable
  public static class IndexedSnapshot extends Snapshot {

    protected final IntVector indexes;

    public IndexedSnapshot(
        @WillCloseWhenClosed Snapshot snapshot, @WillCloseWhenClosed IntVector indexes) {
      super(snapshot.keys, snapshot.values);
      this.indexes = indexes;
      Preconditions.checkArgument(snapshot.keys.getMinorType() == Types.MinorType.BIGINT);
    }

    @Override
    public void close() {
      super.close();
      indexes.close();
    }

    public Range<Long> getKeyRange() {
      if (getValueCount() == 0) {
        return Range.closedOpen(0L, 0L);
      }
      BigIntReader keyReader = getKeyReader();
      keyReader.setPosition(0);
      long start = keyReader.readLong();
      keyReader.setPosition(getValueCount() - 1);
      long end = keyReader.readLong();
      return Range.closedOpen(start, end + 1);
    }

    public FieldReader getIndexReader() {
      return indexes.getReader();
    }

    @Override
    public int getValueCount() {
      return indexes.getValueCount();
    }

    @Override
    public BigIntReader getKeyReader() {
      return new IndexedFieldReader(keys.getReader(), getIndexReader());
    }

    @Override
    public FieldReader getValueReader() {
      return new IndexedFieldReader(super.getValueReader(), getIndexReader());
    }

    @Override
    public IndexedSnapshot slice() {
      return doSlice(0, indexes.getValueCount(), null);
    }

    @Override
    public IndexedSnapshot slice(int start, int length) {
      return doSlice(start, length, null);
    }

    @Override
    public IndexedSnapshot slice(BufferAllocator allocator) {
      return doSlice(0, indexes.getValueCount(), allocator);
    }

    @Override
    public IndexedSnapshot slice(int start, int length, BufferAllocator allocator) {
      return doSlice(start, length, allocator);
    }

    private IndexedSnapshot doSlice(int start, int length, @Nullable BufferAllocator allocator) {
      if (allocator == null) {
        return new IndexedSnapshot(super.slice(), ArrowVectors.slice(indexes, start, length));
      }
      return new IndexedSnapshot(
          super.slice(allocator), ArrowVectors.slice(indexes, start, length, allocator));
    }
  }
}
