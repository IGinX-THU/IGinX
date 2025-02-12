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
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.WillClose;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.Types;

@ThreadSafe
public abstract class IndexedChunk extends Chunk {

  protected final BufferAllocator allocator;

  protected IndexedChunk(@WillCloseWhenClosed Chunk chunk, BufferAllocator allocator) {
    super(chunk.keys, chunk.values);
    this.allocator = allocator;
  }

  @Override
  public synchronized Snapshot snapshot(BufferAllocator allocator) {
    Snapshot snapshot = super.snapshot(allocator);
    IntVector indexes = indexOf(snapshot, allocator);
    if (indexes == null) {
      return snapshot;
    }
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

  @Nullable
  protected abstract IntVector indexOf(Snapshot snapshot, BufferAllocator allocator);

  protected abstract void updateIndex(Snapshot data, int offset);

  protected abstract void deleteIndex(RangeSet<Long> rangeSet);

  public interface Factory {
    IndexedChunk wrap(@WillCloseWhenClosed Chunk chunk, BufferAllocator allocator);

    default IndexedChunk wrap(Snapshot snapshot, BufferAllocator allocator) {
      return wrap(Chunk.like(snapshot, allocator), allocator);
    }

    default Snapshot sorted(@WillClose Snapshot snapshot, BufferAllocator allocator) {
      try (IndexedChunk chunk = wrap(new Chunk(snapshot.keys, snapshot.values), allocator)) {
        return chunk.snapshot(allocator);
      }
    }
  }

  @Immutable
  protected static class IndexedSnapshot extends Snapshot {

    protected final IntVector indexes;

    public IndexedSnapshot(
        @WillCloseWhenClosed Snapshot snapshot, @WillCloseWhenClosed IntVector indexes) {
      super(snapshot.keys, snapshot.values);
      this.indexes = indexes;
      Preconditions.checkArgument(snapshot.keys.getMinorType() == Types.MinorType.BIGINT);
      Preconditions.checkArgument(!indexes.getField().isNullable());
    }

    @Override
    public void close() {
      super.close();
      indexes.close();
    }

    @Override
    public int getValueCount() {
      return indexes.getValueCount();
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

    public int getIndex(int index) {
      return indexes.getDataBuffer().getInt((long) index * IntVector.TYPE_WIDTH);
    }

    @Override
    public long getKey(int index) {
      return super.getKey(getIndex(index));
    }

    @Override
    public Map.Entry<Long, Object> get(int index) {
      return super.get(getIndex(index));
    }
  }
}
