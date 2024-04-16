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
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ReadWriter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.Set;
import java.util.StringJoiner;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTable<K extends Comparable<K>, F, T, V> implements Table<K, F, T, V> {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileTable.class);
  private final String tableName;

  private final ReadWriter<K, F, T, V> readWriter;

  public FileTable(String tableName, ReadWriter<K, F, T, V> readWriter) {
    this.tableName = tableName;
    this.readWriter = readWriter;
  }

  @Override
  public TableMeta<K, F, T, V> getMeta() throws IOException {
    return readWriter.readMeta(tableName);
  }

  @Override
  public Scanner<K, Scanner<F, V>> scan(
      Set<F> fields, RangeSet<K> ranges, @Nullable Filter superSetPredicate) throws IOException {
    LOGGER.debug("read {} where {} & {} from {}", fields, ranges, superSetPredicate, tableName);
    return readWriter.scanData(tableName, fields, ranges, superSetPredicate);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", FileTable.class.getSimpleName() + "[", "]")
        .add("tableName='" + tableName + "'")
        .add("readWriter=" + readWriter)
        .toString();
  }
}
