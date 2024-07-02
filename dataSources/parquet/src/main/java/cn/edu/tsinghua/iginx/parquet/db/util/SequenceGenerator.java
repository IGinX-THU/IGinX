package cn.edu.tsinghua.iginx.parquet.db.util;

import java.util.concurrent.atomic.AtomicLong;

public class SequenceGenerator {

  private final AtomicLong current = new AtomicLong();

  public long next() {
    return current.incrementAndGet();
  }

  public void reset() {
    reset(new AtomicLong().get());
  }

  public void reset(long last) {
    current.set(last);
  }
}
