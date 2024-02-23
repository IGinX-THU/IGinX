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

package cn.edu.tsinghua.iginx.parquet.io.common.buffer;

import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.IntFunction;

public class RecycleSoftPool implements BufferPool {

  private final IntFunction<ByteBuffer> factory;

  private final int maxBufferSize;

  private final ConcurrentLinkedQueue<SoftReference<ByteBuffer>> pool;

  public RecycleSoftPool(IntFunction<ByteBuffer> factory, int maxBufferSize) {
    this.factory = factory;
    this.maxBufferSize = maxBufferSize;
    this.pool = new ConcurrentLinkedQueue<>();
  }

  @Override
  public ByteBuffer allocate(int capacity) {
    if (capacity > maxBufferSize) {
      return null;
    }

    ByteBuffer buffer = doAllocate(capacity);
    buffer.limit(capacity);
    return buffer;
  }

  private ByteBuffer doAllocate(int capacity) {
    while (true) {
      SoftReference<ByteBuffer> soft = pool.poll();

      if (soft == null) {
        return factory.apply(maxBufferSize);
      }
      ByteBuffer buf = soft.get();
      if (buf != null) {
        return buf;
      }
    }
  }

  @Override
  public void release(ByteBuffer buffer) {
    if (buffer != null && buffer.capacity() >= maxBufferSize) {
      buffer.clear();
      pool.add(new SoftReference<>(buffer));
    }
  }
}
