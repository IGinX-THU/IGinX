package cn.edu.tsinghua.iginx.parquet.io;

import cn.edu.tsinghua.iginx.parquet.db.DataBuffer;
import cn.edu.tsinghua.iginx.parquet.db.RangeTombstone;
import cn.edu.tsinghua.iginx.parquet.entity.NativeStorageException;
import cn.edu.tsinghua.iginx.parquet.entity.Range;
import cn.edu.tsinghua.iginx.parquet.entity.Scanner;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public interface ReadWriter<K extends Comparable<K>, F, V> {
  void flush(Path path, DataBuffer<K, F, V> buffer, RangeTombstone<K, F> tombstone)
      throws NativeStorageException;

  Scanner<K, Scanner<F, V>> read(
      Path path, Set<F> fields, Range<K> range, RangeTombstone<K, F> tombstoneDst)
      throws NativeStorageException;

  com.google.common.collect.Range<K> readMeta(
      Path path, Map<F, DataType> types, RangeTombstone<Long, String> tombstoneDst)
      throws NativeStorageException;
}
