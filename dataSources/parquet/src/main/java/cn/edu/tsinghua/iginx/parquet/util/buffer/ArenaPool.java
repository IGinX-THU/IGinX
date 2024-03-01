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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ArenaPool implements BufferPool, AutoCloseable {
  private final BufferPool pool;

  public ArenaPool(BufferPool pool) {
    this.pool = pool;
  }

  private final Set<ByteBuffer> unreleased = Collections.newSetFromMap(new ConcurrentHashMap<>());

  @Override
  public ByteBuffer allocate(int capacity) {
    ByteBuffer buffer = pool.allocate(capacity);
    unreleased.add(buffer);
    return buffer;
  }

  @Override
  public void release(ByteBuffer buffer) {
    if (unreleased.remove(buffer)) {
      pool.release(buffer);
    }
  }

  @Override
  public void close() {
    for (ByteBuffer buffer : unreleased) {
      pool.release(buffer);
    }
  }
}
