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

package cn.edu.tsinghua.iginx.parquet.db.lsm.table;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.common.scanner.Scanner;
import cn.edu.tsinghua.iginx.parquet.io.ReadWriter;
import com.google.common.collect.Range;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTable<K extends Comparable<K>, F, V, T> implements Table<K, F, V, T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileTable.class);
  private final Path path;

  private final ReadWriter<K, F, V, T> readWriter;

  public FileTable(Path path, ReadWriter<K, F, V, T> readWriter) {
    this.path = path;
    this.readWriter = readWriter;
  }

  @Override
  @Nonnull
  public TableMeta<F, T> getMeta() throws IOException {
    Map.Entry<Map<F, T>, Map<String, String>> meta = readWriter.readMeta(path);
    return new TableMeta<>(meta.getKey(), meta.getValue());
  }

  @Override
  @Nonnull
  public Scanner<K, Scanner<F, V>> scan(
      @Nonnull Set<F> fields, @Nonnull Range<K> range, @Nullable Filter predicate)
      throws IOException {
    LOGGER.info("read {} where {} & {} from {}", fields, range, predicate, path);
    return readWriter.scanData(path, fields, range, predicate);
  }

  @Override
  public String toString() {
    return "FileTable{" + "path=" + path + '}';
  }
}
