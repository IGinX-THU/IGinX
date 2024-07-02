package cn.edu.tsinghua.iginx.parquet.db.util.iterator;

import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import javax.annotation.Nonnull;

public interface Scanner<K, V> extends AutoCloseable {
  @Nonnull
  K key();

  @Nonnull
  V value();

  boolean iterate() throws StorageException;

  @Override
  void close() throws StorageException;
}
