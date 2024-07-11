package cn.edu.tsinghua.iginx.parquet.db.util.iterator;

import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;

public class ListenCloseScanner<K, V> extends DelegateScanner<K, V> {

  private final Runnable callback;

  public ListenCloseScanner(Scanner<K, V> scanner, Runnable callback) {
    super(scanner);
    this.callback = callback;
  }

  @Override
  public void close() throws StorageException {
    super.close();
    callback.run();
  }
}
