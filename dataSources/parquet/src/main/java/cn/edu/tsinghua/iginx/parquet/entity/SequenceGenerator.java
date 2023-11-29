package cn.edu.tsinghua.iginx.parquet.entity;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public class SequenceGenerator implements LongSupplier {

  private static final long DELTA = 1L;

  private final AtomicLong current;

  public SequenceGenerator(long last) {
    current = new AtomicLong(last);
  }

  @Override
  public long getAsLong() {
    return current.getAndAdd(DELTA);
  }
}
