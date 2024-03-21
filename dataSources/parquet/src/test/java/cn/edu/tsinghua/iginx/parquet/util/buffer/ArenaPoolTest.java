package cn.edu.tsinghua.iginx.parquet.util.buffer;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class ArenaPoolTest extends BufferPoolTest {

  @Override
  protected BufferPool getBufferPool() {
    return new ArenaPool(new HeapPool());
  }

  @Test
  public void testRelease() {
    CounterPool internalPool = new CounterPool();
    ArenaPool pool = new ArenaPool(internalPool);

    List<Integer> capacities = new ArrayList<>();
    for (int capacity : getAllocateSequence()) {
      capacities.add(capacity);
    }

    List<ByteBuffer> buffers = new ArrayList<>();
    for (int capacity : capacities) {
      ByteBuffer buffer = assertAllocateOnly(pool, capacity);
      if (capacity >= Integer.BYTES) {
        buffer.putInt(capacity);
      }
      if (capacity % 2 == 0) {
        buffers.add(buffer);
      }
    }

    assertEquals(capacities.size(), internalPool.getCounter());

    for (ByteBuffer buffer : buffers) {
      pool.release(buffer);
    }

    assertEquals(capacities.size() - buffers.size(), internalPool.getCounter());

    pool.close();

    assertEquals(0, internalPool.getCounter());
  }
}
