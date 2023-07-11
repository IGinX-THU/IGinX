package cn.edu.tsinghua.iginx.filesystem.tools;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryPool {
  private static final Logger logger = LoggerFactory.getLogger(MemoryPool.class);
  private static int blockSize; // 1024 bytes per block
  private static ConcurrentLinkedQueue<byte[]> freeBlocks;
  private static int poolSize;
  private static AtomicInteger numberOfBlocks;
  private static int maxNumberOfBlocks;

  static {
    poolSize = 1024 * 1024 * 100;
    blockSize = 1024 * 1024;
    maxNumberOfBlocks = poolSize / blockSize;
    numberOfBlocks = new AtomicInteger(poolSize / blockSize);
    freeBlocks = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < numberOfBlocks.get(); i++) {
      freeBlocks.add(new byte[blockSize]);
    }
  }

  public static byte[] allocate(int size) {
    byte[] buffer = freeBlocks.poll();
    if (numberOfBlocks.get() > 0) numberOfBlocks.decrementAndGet();
    if (buffer == null) {
      logger.warn("Out of memory: No more blocks available");
      return new byte[size];
    }
    return buffer;
  }

  public static void release(byte[] buffer) {
    if (numberOfBlocks.get() < maxNumberOfBlocks) {
      numberOfBlocks.incrementAndGet();
      freeBlocks.offer(buffer);
    }
  }

  public static int getBlockSize() {
    return blockSize;
  }
}
