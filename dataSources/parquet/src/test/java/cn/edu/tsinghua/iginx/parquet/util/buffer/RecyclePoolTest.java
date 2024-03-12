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

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.*;
import org.junit.Test;

public class RecyclePoolTest extends BufferPoolTest {

  @Override
  protected BufferPool getBufferPool() {
    return new RecyclePool(new HeapPool(), 1024, 1024 * 1024);
  }

  @Test
  public void testSmall() {
    RecyclePool pool = new RecyclePool(new HeapPool(), 1024, 1024 * 1024);
    for (int i = 1; i <= 1024; i = i * 2) {
      int random = i - (int) (Math.random() * (i / 2));
      assertEquals(i, assertAllocate(pool, random).capacity());
    }
  }

  @Test
  public void testAlign() {
    RecyclePool pool = new RecyclePool(new HeapPool(), 1024, 1024 * 1024);
    Random ran = new Random();
    for (int i = 2048; i <= 1024 * 1024; i += 1024) {
      int random = i - (int) (Math.random() * 1024);
      assertEquals(i, assertAllocate(pool, random).capacity());
    }
  }

  @Test
  public void testLimit() {
    RecyclePool pool = new RecyclePool(new HeapPool(), 1024, 1024 * 1024 + 1);
    assertEquals(1025 * 1024, assertAllocate(pool, 1024 * 1024 + 1).capacity());
    assertEquals(1024 * 1024 + 2, assertAllocate(pool, 1024 * 1024 + 2).capacity());
    assertEquals(1024 * 1024 + 10000, assertAllocate(pool, 1024 * 1024 + 10000).capacity());
  }

  @Test
  public void testRecycleSingleSmall() {
    RecyclePool pool = new RecyclePool(new HeapPool(), 1024, 1024 * 1024);
    ByteBuffer buffer = assertAllocate(pool, 333);
    pool.release(buffer);
    assertEquals(buffer, assertAllocate(pool, 257));
    pool.release(buffer);
    assertEquals(buffer, assertAllocate(pool, 333));
    pool.release(buffer);
    assertEquals(buffer, assertAllocate(pool, 334));
    pool.release(buffer);
    assertEquals(buffer, assertAllocate(pool, 512));
    pool.release(buffer);
    assertNotEquals(buffer, assertAllocate(pool, 513));
  }

  @Test
  public void testRecycleSingleAlign() {
    RecyclePool pool = new RecyclePool(new HeapPool(), 1024, 1024 * 1024);
    ByteBuffer buffer = assertAllocate(pool, 2000);
    pool.release(buffer);
    assertEquals(buffer, assertAllocate(pool, 1025));
    pool.release(buffer);
    assertEquals(buffer, assertAllocate(pool, 2000));
    pool.release(buffer);
    assertEquals(buffer, assertAllocate(pool, 2048));
    pool.release(buffer);
    assertNotEquals(buffer, assertAllocate(pool, 2049));
  }

  @Test
  public void testRecycleSingleHuge() {
    RecyclePool pool = new RecyclePool(new HeapPool(), 1024, 1024 * 1024 + 1);
    ByteBuffer buffer = assertAllocate(pool, 1024 * 1024 + 2);
    pool.release(buffer);
    assertEquals(buffer, assertAllocate(pool, 1024 * 1024 + 2));
    pool.release(buffer);
    assertNotEquals(buffer, assertAllocate(pool, 1024 * 1024 + 3));
  }

  @Test
  public void testRecycleSequence() {
    RecyclePool pool = new RecyclePool(new HeapPool(), 1024, 1024 * 1024);
    List<Integer> capacities = new ArrayList<>();
    for (int capacity : getAllocateSequence()) {
      capacities.add(capacity);
    }

    List<ByteBuffer> buffers = new ArrayList<>();
    for (int capacity : capacities) {
      ByteBuffer buffer = assertAllocateOnly(pool, capacity);
      buffer.putInt(capacity);
      buffers.add(buffer);
    }
    for (ByteBuffer buffer : buffers) {
      pool.release(buffer);
    }

    Collections.shuffle(capacities);

    for (int capacity : capacities) {
      ByteBuffer buffer = assertAllocateOnly(pool, capacity);
      assertEquals(capacity, buffer.getInt());
    }
  }
}
