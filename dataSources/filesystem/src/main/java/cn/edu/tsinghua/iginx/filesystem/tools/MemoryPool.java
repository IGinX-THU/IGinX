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
