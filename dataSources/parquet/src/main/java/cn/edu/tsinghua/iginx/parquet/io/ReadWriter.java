/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.io;

import cn.edu.tsinghua.iginx.parquet.db.DataBuffer;
import cn.edu.tsinghua.iginx.parquet.db.RangeTombstone;
import cn.edu.tsinghua.iginx.parquet.entity.NativeStorageException;
import cn.edu.tsinghua.iginx.parquet.entity.Range;
import cn.edu.tsinghua.iginx.parquet.entity.Scanner;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public interface ReadWriter<K extends Comparable<K>, F, V, T> {
  void flush(Path path, DataBuffer<K, F, V> buffer, RangeTombstone<K, F> tombstone)
      throws NativeStorageException;

  Scanner<K, Scanner<F, V>> read(
      Path path, Set<F> fields, Range<K> range, RangeTombstone<K, F> tombstoneDst)
      throws NativeStorageException;

  com.google.common.collect.Range<K> readMeta(
      Path path, Map<F, T> schemaDst, RangeTombstone<K, F> tombstoneDst)
      throws NativeStorageException;
}
