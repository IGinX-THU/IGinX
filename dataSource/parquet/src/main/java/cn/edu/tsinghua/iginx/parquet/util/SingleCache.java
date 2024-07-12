package cn.edu.tsinghua.iginx.parquet.util;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class SingleCache<T> {
  private T obj;
  private final Supplier<T> supplier;

  public SingleCache(Supplier<T> supplier) {
    this.supplier = supplier;
  }

  public T get() {
    if (obj == null) {
      obj = supplier.get();
    }
    return obj;
  }

  public void invalidate(Consumer<T> consumer) {
    if (obj != null) {
      consumer.accept(obj);
      obj = null;
    }
  }
}
