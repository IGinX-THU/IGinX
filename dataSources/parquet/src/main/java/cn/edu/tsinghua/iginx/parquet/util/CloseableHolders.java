package cn.edu.tsinghua.iginx.parquet.util;

import java.util.NoSuchElementException;
import org.apache.arrow.util.Preconditions;

public class CloseableHolders {

  public static class NoexceptAutoCloseableHolder<T extends NoexceptAutoCloseable>
      implements NoexceptAutoCloseable {
    private T closeable;

    NoexceptAutoCloseableHolder(T closeable) {
      this.closeable = Preconditions.checkNotNull(closeable);
    }

    public T transfer() {
      if (closeable == null) {
        throw new NoSuchElementException("Already transferred");
      }
      T ret = closeable;
      closeable = null;
      return ret;
    }

    public T peek() {
      return closeable;
    }

    @Override
    public void close() {
      if (closeable != null) {
        closeable.close();
        closeable = null;
      }
    }
  }

  public static <T extends NoexceptAutoCloseable> NoexceptAutoCloseableHolder<T> hold(T closeable) {
    return new NoexceptAutoCloseableHolder<>(closeable);
  }
}
