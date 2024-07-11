package cn.edu.tsinghua.iginx.parquet.util;

import org.apache.arrow.util.AutoCloseables;

public class NoexceptAutoCloseables {

  public static void close(Iterable<? extends NoexceptAutoCloseable> closeables) {
    try {
      AutoCloseables.close(closeables);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static NoexceptAutoCloseable all(Iterable<? extends NoexceptAutoCloseable> closeables) {
    return () -> close(closeables);
  }
}
