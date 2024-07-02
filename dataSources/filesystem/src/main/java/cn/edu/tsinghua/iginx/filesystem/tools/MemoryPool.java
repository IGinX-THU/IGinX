/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.filesystem.tools;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryPool {

  private final Logger LOGGER = LoggerFactory.getLogger(MemoryPool.class);

  private final Queue<byte[]> freeBlocks = new ConcurrentLinkedQueue<>();

  private AtomicInteger numberOfBlocks;

  private int maxNumberOfBlocks;

  public int chunkSize;

  public MemoryPool(int capacity, int chunkSize) {
    this.maxNumberOfBlocks = capacity;
    this.chunkSize = chunkSize;
    this.numberOfBlocks = new AtomicInteger(maxNumberOfBlocks);
    for (int i = 0; i < maxNumberOfBlocks; i++) {
      freeBlocks.add(new byte[chunkSize]);
    }
  }

  public byte[] allocate() {
    byte[] buffer = freeBlocks.poll();
    if (buffer == null) {
      //      LOGGER.warn("Out of memory: No more blocks available");
      return new byte[chunkSize];
    }
    if (numberOfBlocks.get() > 0) {
      numberOfBlocks.decrementAndGet();
    }
    return buffer;
  }

  public void release(byte[] buffer) {
    if (numberOfBlocks.get() < maxNumberOfBlocks) {
      numberOfBlocks.incrementAndGet();
      if (buffer.length != chunkSize) {
        freeBlocks.offer(new byte[chunkSize]);
      } else {
        freeBlocks.offer(buffer);
      }
    }
  }

  public void close() {
    freeBlocks.clear();
  }
}
