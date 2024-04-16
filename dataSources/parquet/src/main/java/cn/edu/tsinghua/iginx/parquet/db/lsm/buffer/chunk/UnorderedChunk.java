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
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.reader.BigIntReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
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

    public BigIntReader getKeyReader() {
      return keys.getReader();
    }

    public FieldReader getValueReader() {
      return values.getReader();
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

    @Override
    @Nonnull
    public Iterator<Map.Entry<Long, Object>> iterator() {
      return new ChunkSnapshotIterator(getKeyReader(), getValueReader(), getValueCount());
    }

    private static class ChunkSnapshotIterator implements Iterator<Map.Entry<Long, Object>> {
      private final BigIntReader keyReader;
      private final FieldReader valueReader;
      private final int valueCount;
      private int position;

      public ChunkSnapshotIterator(
          BigIntReader keyReader, FieldReader valueReader, int valueCount) {
        this.keyReader = keyReader;
        this.valueReader = valueReader;
        this.valueCount = valueCount;
      }

      @Override
      public boolean hasNext() {
        return position < valueCount;
      }

      @Override
      public Map.Entry<Long, Object> next() {
        if (!hasNext()) {
          throw new IndexOutOfBoundsException();
        }
        keyReader.setPosition(position);
        valueReader.setPosition(position);
        Long key = keyReader.readLong();
        Object value = valueReader.readObject();
        position++;
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
      }
    }
  }
}
