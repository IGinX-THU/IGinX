package cn.edu.tsinghua.iginx.parquet.io;

import cn.edu.tsinghua.iginx.parquet.db.DataBuffer;
import cn.edu.tsinghua.iginx.parquet.entity.NativeStorageException;
import cn.edu.tsinghua.iginx.parquet.entity.Range;
import cn.edu.tsinghua.iginx.parquet.entity.Scanner;
import java.nio.file.Path;
import java.util.Set;

public interface ReadWriter<K extends Comparable<K>, F, V> {
  void flush(Path path, DataBuffer<K, F, V> buffer) throws NativeStorageException;

  Scanner<K, Scanner<F, V>> read(Path path, Set<F> fields, Range<K> range)
      throws NativeStorageException;
}
