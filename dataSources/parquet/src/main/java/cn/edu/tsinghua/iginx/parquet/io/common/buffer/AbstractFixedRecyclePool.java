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

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

abstract class AbstractFixedRecyclePool<R> implements BufferPool<ByteBuffer> {

  private final BufferPool<ByteBuffer> subPool;
  private final int recycleSize;
  private final ConcurrentLinkedQueue<R> queue = new ConcurrentLinkedQueue<>();

  public AbstractFixedRecyclePool(BufferPool<ByteBuffer> subPool, int recycleSize) {
    this.subPool = subPool;
    this.recycleSize = recycleSize;
  }

  protected abstract R reference(ByteBuffer buffer);

  protected abstract ByteBuffer deReference(R reference);

  @Override
  public ByteBuffer allocate(int capacity) {
    if (capacity <= recycleSize) {
      while (true) {
        R ref = queue.poll();
        if (ref == null) {
          break;
        }
        ByteBuffer buf = deReference(ref);
        if (buf != null) {
          return buf;
        }
      }
    }

    return subPool.allocate(capacity);
  }

  @Override
  public void release(ByteBuffer buffer) {
    if (buffer == null) {
      return;
    }

    if(buffer.capacity() > recycleSize) {
      subPool.release(buffer);
      return;
    }

    buffer.clear();
    R ref = reference(buffer);
    queue.add(ref);
  }
}
