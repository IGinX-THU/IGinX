package cn.edu.tsinghua.iginx.parquet.entity;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public class SequenceGenerator implements LongSupplier {

  private static final long DELTA = 1L;

  private final AtomicLong current = new AtomicLong();;

  @Override
  public long getAsLong() {
    return current.getAndAdd(DELTA);
  }

  public void reset(long last) {
    current.set(last);
  }
}
