package cn.edu.tsinghua.iginx.parquet.entity;

import cn.edu.tsinghua.iginx.parquet.entity.NativeStorageException;
import java.util.NoSuchElementException;
import javax.annotation.Nonnull;

public interface Scanner<K, V> extends AutoCloseable {
  @Nonnull
  K key() throws NoSuchElementException;

  @Nonnull
  V value() throws NoSuchElementException;

  boolean iterate() throws NativeStorageException;

  @Override
  void close() throws NativeStorageException;
}
