package cn.edu.tsinghua.iginx.filestore.common;

import java.io.Closeable;
import java.io.IOException;

public class Closeables {

  private Closeables() {
  }

  public static void close(Iterable<? extends Closeable> ac) throws IOException {
    if (ac == null) {
      return;
    } else if (ac instanceof Closeable) {
      ((Closeable) ac).close();
      return;
    }

    IOException exception = null;
    for (Closeable closeable : ac) {
      try {
        if (closeable != null) {
          closeable.close();
        }
      } catch (IOException e) {
        if (exception == null) {
          exception = e;
        } else if (e != exception) {
          exception.addSuppressed(e);
        }
      }
    }
    if (exception != null) {
      throw exception;
    }
  }

  public static Closeable closeAsIOException(AutoCloseable ac) {
    return () -> {
      try {
        ac.close();
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException(e);
      }
    };
  }
}
