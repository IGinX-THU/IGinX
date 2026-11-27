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
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.lsm.buffer;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.Shared;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.StorageProperties;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.Test;

public class MemTableQueueNotificationTest {
  @Test(timeout = 10000)
  public void tablePublishedBeforeWaitingIsReturned() throws Exception {
    try (Shared shared = Shared.of(StorageProperties.builder().build());
        MemTableQueue queue = new MemTableQueue(shared, shared.getAllocator())) {
      queue.store(Collections.emptyList());
      queue.compact();
      assertEquals(0, queue.awaitNext(0));
    }
  }

  @Test(timeout = 10000)
  public void publicationBetweenEmptyCheckAndAwaitIsNotLost() throws Exception {
    try (Shared shared = Shared.of(StorageProperties.builder().build());
        MemTableQueue queue = new MemTableQueue(shared, shared.getAllocator())) {
      queue.store(Collections.emptyList());
      // Hold the existing condition lock to reproduce the old gap without scheduling luck
      // or adding a test callback to production code. The public compact() path publishes.
      Field field = MemTableQueue.class.getDeclaredField("pollLock");
      field.setAccessible(true);
      ReentrantLock conditionLock = (ReentrantLock) field.get(queue);
      FutureTask<Long> result = new FutureTask<>(() -> queue.awaitNext(0));
      Thread consumer = new Thread(result, "memtable-notification-consumer");
      try {
        conditionLock.lock();
        try {
          consumer.start();
          long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
          while (!conditionLock.hasQueuedThread(consumer)) {
            assertTrue("Consumer did not reach the condition lock", System.nanoTime() < deadline);
            Thread.sleep(1);
          }
          // Before the fix, the consumer already observed an empty queue and this signal
          // is lost. With the fix it must inspect the queue after taking the condition lock.
          queue.compact();
        } finally {
          conditionLock.unlock();
        }
        assertEquals(Long.valueOf(0), result.get(3, TimeUnit.SECONDS));
      } finally {
        consumer.interrupt();
        consumer.join(3000);
        assertFalse("Queue waiter did not stop", consumer.isAlive());
      }
    }
  }
}
