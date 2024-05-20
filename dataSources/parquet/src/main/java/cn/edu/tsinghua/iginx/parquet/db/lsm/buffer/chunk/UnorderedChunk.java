package cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk;

import cn.edu.tsinghua.iginx.parquet.util.arrow.ArrowVectors;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.types.pojo.Field;

@ThreadSafe
public class UnorderedChunk implements AutoCloseable {

  @GuardedBy("this")
  protected final ValueVector keys;

  @GuardedBy("this")
  protected final ValueVector values;

  protected UnorderedChunk(
      @WillCloseWhenClosed ValueVector keys, @WillCloseWhenClosed ValueVector values) {
    Preconditions.checkNotNull(keys);
    Preconditions.checkNotNull(values);
    Preconditions.checkArgument(keys.getValueCount() == values.getValueCount());

    this.keys = keys;
    this.values = values;
  }

  public static UnorderedChunk like(Snapshot snapshot, BufferAllocator allocator) {
    return new UnorderedChunk(
        ArrowVectors.like(snapshot.keys, allocator), ArrowVectors.like(snapshot.values, allocator));
  }

  public synchronized Snapshot snapshot(BufferAllocator allocator) {
    return new Snapshot(ArrowVectors.slice(keys, allocator), ArrowVectors.slice(values, allocator));
  }

  public synchronized int store(Snapshot data) {
    int offset = keys.getValueCount();
    ArrowVectors.append(keys, data.keys);
    ArrowVectors.append(values, data.values);
    return offset;
  }

  public synchronized int getValueCount() {
    return keys.getValueCount();
  }

  @Override
  public synchronized void close() {
    keys.close();
    values.close();
  }

  @Immutable
  public static class Snapshot implements AutoCloseable, Iterable<Map.Entry<Long, Object>> {

    protected final ValueVector keys;
    protected final ValueVector values;

    public Snapshot(
        @WillCloseWhenClosed ValueVector keys, @WillCloseWhenClosed ValueVector values) {
      Preconditions.checkNotNull(keys);
      Preconditions.checkNotNull(values);
      Preconditions.checkArgument(keys.getValueCount() == values.getValueCount());
      Preconditions.checkArgument(!keys.getField().isNullable());
      Preconditions.checkArgument(!values.getField().isNullable());

      this.keys = keys;
      this.values = values;
    }

    public static Snapshot empty(Field key, Field value) {
      Preconditions.checkNotNull(key);
      Preconditions.checkNotNull(value);
      return new Snapshot(new ZeroVector(key), new ZeroVector(value));
    }

    public Field getField() {
      return values.getField();
    }

    public int getValueCount() {
      return keys.getValueCount();
    }

    public Snapshot slice() {
      return doSlice(0, keys.getValueCount(), null);
    }

    public Snapshot slice(int start, int length) {
      return doSlice(start, length, null);
    }

    public Snapshot slice(BufferAllocator allocator) {
      return doSlice(0, keys.getValueCount(), allocator);
    }

    public Snapshot slice(int start, int length, BufferAllocator allocator) {
      return doSlice(start, length, allocator);
    }

    private Snapshot doSlice(int start, int length, @Nullable BufferAllocator allocator) {
      if (allocator == null) {
        return new Snapshot(
            ArrowVectors.slice(keys, start, length), ArrowVectors.slice(values, start, length));
      }
      return new Snapshot(
          ArrowVectors.slice(keys, start, length, allocator),
          ArrowVectors.slice(values, start, length, allocator));
    }

    @Override
    public void close() {
      keys.close();
      values.close();
    }

    public long getKey(int index) {
      return doGetKey(index);
    }

    private long doGetKey(int index) {
      return keys.getDataBuffer().getLong((long) index * BigIntVector.TYPE_WIDTH);
    }

    public Map.Entry<Long, Object> get(int index) {
      long key = doGetKey(index);
      Object value = values.getObject(index);
      return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }

    @Override
    @Nonnull
    public Iterator<Map.Entry<Long, Object>> iterator() {
      return new ChunkSnapshotReader();
    }

    public class ChunkSnapshotReader implements Iterator<Map.Entry<Long, Object>> {
      private int position = 0;
      private final int valueCount = getValueCount();

      @Override
      public boolean hasNext() {
        return position < valueCount;
      }

      @Override
      public Map.Entry<Long, Object> next() {
        if (!hasNext()) {
          throw new IndexOutOfBoundsException();
        }
        return get(position++);
      }
    }
  }
}
