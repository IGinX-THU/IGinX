package cn.edu.tsinghua.iginx.parquet.util.buffer;

import java.nio.ByteBuffer;

public class CounterPool implements BufferPool {
  private int counter = 0;

  @Override
  public ByteBuffer allocate(int capacity) {
    counter++;
    return ByteBuffer.allocate(capacity);
  }

  @Override
  public void release(ByteBuffer byteBuffer) {
    counter--;
  }

  int getCounter() {
    return counter;
  }
}
