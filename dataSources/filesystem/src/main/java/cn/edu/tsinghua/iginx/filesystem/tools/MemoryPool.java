package cn.edu.tsinghua.iginx.filesystem.tools;

import static cn.edu.tsinghua.iginx.filesystem.constant.Constant.BLOCK_SIZE;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryPool {

  private static final Logger logger = LoggerFactory.getLogger(MemoryPool.class);

  private static final Queue<byte[]> freeBlocks = new ConcurrentLinkedQueue<>();

  private static final AtomicInteger numberOfBlocks = new AtomicInteger(100);

  private static final int maxNumberOfBlocks = 100;

  private static MemoryPool INSTANCE = null;

  public static MemoryPool getInstance() {
    if (INSTANCE == null) {
      synchronized (MemoryPool.class) {
        if (INSTANCE == null) {
          INSTANCE = new MemoryPool();
        }
      }
    }
    return INSTANCE;
  }

  public MemoryPool() {
    for (int i = 0; i < maxNumberOfBlocks; i++) {
      freeBlocks.add(new byte[BLOCK_SIZE]);
    }
  }

  public byte[] allocate() {
    byte[] buffer = freeBlocks.poll();
    if (buffer == null) {
      logger.warn("Out of memory: No more blocks available");
      return new byte[BLOCK_SIZE];
    }
    if (numberOfBlocks.get() > 0) {
      numberOfBlocks.decrementAndGet();
    }
    return buffer;
  }

  public void release(byte[] buffer) {
    if (numberOfBlocks.get() < maxNumberOfBlocks) {
      numberOfBlocks.incrementAndGet();
      freeBlocks.offer(buffer);
    }
  }
}
