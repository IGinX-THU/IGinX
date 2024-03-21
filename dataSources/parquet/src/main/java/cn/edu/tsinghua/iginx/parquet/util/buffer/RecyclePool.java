/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.util.buffer;

import cn.edu.tsinghua.iginx.parquet.util.recycle.ConcurrentQueueRecycler;
import cn.edu.tsinghua.iginx.parquet.util.recycle.Recycler;
import cn.edu.tsinghua.iginx.parquet.util.recycle.ReferenceRecycler;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class RecyclePool implements BufferPool {
  private final AtomicReferenceArray<Recycler<ByteBuffer>> smallBuffers;

  private final ConcurrentHashMap<Integer, Recycler<ByteBuffer>> alignedBuffers;

  private final ConcurrentSkipListMap<Integer, Recycler<ByteBuffer>> hugeBuffers;

  private final BufferPool pool;

  private final int align;

  private final int limit;

  public RecyclePool(BufferPool pool, int align, int limit) {
    if (align <= 0) {
      throw new IllegalArgumentException("align must be a positive number, but got " + align);
    }
    if (limit <= 0) {
      throw new IllegalArgumentException("limit must be a positive number, but got " + limit);
    }
    if (limit < align) {
      throw new IllegalArgumentException(
          "limit must be greater than or equal to align, but got limit: "
              + limit
              + " align: "
              + align);
    }
    this.pool =
        Objects.requireNonNull(
            pool, RecyclePool.class.getSimpleName() + " requires a non-null pool");
    this.align = align;
    this.limit = limit;
    this.smallBuffers =
        new AtomicReferenceArray<>(Integer.SIZE - Integer.numberOfLeadingZeros(align) + 1);
    this.alignedBuffers = new ConcurrentHashMap<>((limit - 1) / align + 1);
    this.hugeBuffers = new ConcurrentSkipListMap<>();
  }

  @Override
  public ByteBuffer allocate(int capacity) {
    if (capacity < 0) {
      throw new IllegalArgumentException(
          "capacity must be a non-negative number, but got " + capacity);
    }
    int atLeastCapacity = getAtLeastCapacity(capacity);
    Recycler<ByteBuffer> recycler = getRecyclerAtLeast(atLeastCapacity);
    ByteBuffer byteBuffer = recycler.get();
    if (byteBuffer == null) {
      byteBuffer = pool.allocate(atLeastCapacity);
    }
    byteBuffer.clear();
    byteBuffer.limit(capacity);
    return byteBuffer;
  }

  @Override
  public void release(ByteBuffer byteBuffer) {
    if (byteBuffer == null) {
      return;
    }
    int capacity = byteBuffer.capacity();
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity must be a positive number, but got " + capacity);
    }
    int atMostCapacity = getAtMostCapacity(capacity);
    Recycler<ByteBuffer> recycler = getAtMostRecycler(atMostCapacity);
    recycler.recycle(byteBuffer);
  }

  public void clear() {
    for (int i = 0; i < smallBuffers.length(); i++) {
      Recycler<ByteBuffer> recycler = smallBuffers.get(i);
      if (recycler != null) {
        recycler.clear();
        smallBuffers.set(i, null);
      }
    }
    alignedBuffers.values().forEach(Recycler::clear);
    alignedBuffers.clear();
    hugeBuffers.values().forEach(Recycler::clear);
    hugeBuffers.clear();
  }

  private int getAtLeastCapacity(int capacity) {
    if (capacity <= align) {
      return capacity < 2 ? capacity : Integer.highestOneBit(capacity - 1) << 1;
    } else if (capacity <= limit) {
      return ((capacity - 1) / align + 1) * align;
    } else {
      return capacity;
    }
  }

  private Recycler<ByteBuffer> getRecyclerAtLeast(int aligned) {
    if (aligned <= align) {
      return getSmallRecycler(aligned);
    } else if (aligned <= limit) {
      return getAlignedRecycler(aligned);
    } else {
      return getHugeRecyclerAtLeast(aligned);
    }
  }

  private int getAtMostCapacity(int capacity) {
    if (capacity <= align) {
      return Integer.highestOneBit(capacity);
    } else if (capacity <= limit) {
      return capacity - capacity % align;
    } else {
      return limit;
    }
  }

  private Recycler<ByteBuffer> getAtMostRecycler(int aligned) {
    if (aligned <= align) {
      return getSmallRecycler(aligned);
    } else if (aligned <= limit) {
      return getAlignedRecycler(aligned);
    } else {
      return getHugeRecycler(aligned);
    }
  }

  private Recycler<ByteBuffer> getSmallRecycler(int aligned) {
    int index = Integer.SIZE - Integer.numberOfLeadingZeros(aligned);
    return smallBuffers.updateAndGet(
        index, old -> old == null ? createSmallRecycler(aligned) : old);
  }

  private Recycler<ByteBuffer> getAlignedRecycler(int aligned) {
    int index = aligned / align;
    return alignedBuffers.computeIfAbsent(index, this::createAlignedRecycler);
  }

  private Recycler<ByteBuffer> getHugeRecycler(int capacity) {
    return hugeBuffers.computeIfAbsent(capacity, this::createHugeRecycler);
  }

  private Recycler<ByteBuffer> getHugeRecyclerAtLeast(int capacity) {
    Map.Entry<Integer, Recycler<ByteBuffer>> entry = hugeBuffers.ceilingEntry(capacity);
    if (entry == null) {
      return getHugeRecycler(capacity);
    }
    return entry.getValue();
  }

  protected Recycler<ByteBuffer> createSmallRecycler(int capacity) {
    return new ReferenceRecycler<>(new ConcurrentQueueRecycler<>(), WeakReference::new);
  }

  protected Recycler<ByteBuffer> createAlignedRecycler(int capacity) {
    return new ReferenceRecycler<>(new ConcurrentQueueRecycler<>(), WeakReference::new);
  }

  protected Recycler<ByteBuffer> createHugeRecycler(int capacity) {
    return new ReferenceRecycler<>(new ConcurrentQueueRecycler<>(), WeakReference::new);
  }
}
