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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Random;
import org.junit.Test;

public abstract class BufferPoolTest {

  protected abstract BufferPool getBufferPool();

  protected Iterable<Integer> getAllocateSequence() {
    // random sequence between 1 and 16*1024*1024
    Random random = new Random();
    int n = 100;
    return () ->
        new Iterator<Integer>() {
          int i = 0;

          @Override
          public boolean hasNext() {
            return i < n;
          }

          @Override
          public Integer next() {
            i++;
            return random.nextInt(16 * 1024 * 1024) + 1;
          }
        };
  }

  @Test
  public void testAllocate() {
    BufferPool bufferPool = getBufferPool();
    assertAllocate(bufferPool, 1);
    assertAllocate(bufferPool, 1024);
    assertAllocate(bufferPool, 1024 * 1024);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAllocateZero() {
    BufferPool bufferPool = getBufferPool();
    assertAllocate(bufferPool, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAllocateNegative() {
    BufferPool bufferPool = getBufferPool();
    assertAllocate(bufferPool, -1);
  }

  @Test
  public void testRelease() {
    BufferPool bufferPool = getBufferPool();
    bufferPool.release(ByteBuffer.allocate(1024));
  }

  @Test
  public void testReleaseNull() {
    BufferPool bufferPool = getBufferPool();
    bufferPool.release(null);
  }

  @Test
  public void testAllocateRelease() {
    BufferPool bufferPool = getBufferPool();
    ByteBuffer buffer = assertAllocate(bufferPool, 1024);
    bufferPool.release(buffer);
    assertAllocate(bufferPool, 1024);
  }

  @Test
  public void testAllocateReleaseAllocate() {
    BufferPool bufferPool = getBufferPool();
    ByteBuffer buffer = assertAllocate(bufferPool, 1024);
    bufferPool.release(buffer);
    assertAllocate(bufferPool, 1024);
  }

  @Test
  public void testAllocateSequence() {
    BufferPool bufferPool = getBufferPool();
    for (int capacity : getAllocateSequence()) {
      assertAllocate(bufferPool, capacity);
    }
  }

  @Test
  public void testAllocateReleaseSequence() {
    BufferPool bufferPool = getBufferPool();
    for (int capacity : getAllocateSequence()) {
      ByteBuffer buffer = assertAllocate(bufferPool, capacity);
      bufferPool.release(buffer);
    }
  }

  @Test
  public void testConcurrentAllocateRelease() {
    BufferPool bufferPool = getBufferPool();
    for (int i = 0; i < 100; i++) {
      new Thread(
              () -> {
                for (int capacity : getAllocateSequence()) {
                  ByteBuffer buffer = assertAllocate(bufferPool, capacity);
                  bufferPool.release(buffer);
                }
              })
          .start();
    }
  }

  public static ByteBuffer assertAllocate(BufferPool bufferPool, int atLeast) {
    ByteBuffer buffer = assertAllocateOnly(bufferPool, atLeast);
    for (int i = 0; i < atLeast; i++) {
      buffer.put(i, (byte) i);
    }
    return buffer;
  }

  public static ByteBuffer assertAllocateOnly(BufferPool bufferPool, int atLeast) {
    ByteBuffer buffer = bufferPool.allocate(atLeast);
    assertNotNull(buffer);
    assertTrue(
        "Buffer capacity should be at least " + atLeast + ", but was " + buffer.capacity(),
        buffer.capacity() >= atLeast);
    return buffer;
  }
}
