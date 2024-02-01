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

package cn.edu.tsinghua.iginx.parquet.db.lsm.api;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.Set;

public interface ReadWriter<K extends Comparable<K>, F, T, V> {

  void flush(String name, TableMeta<K, F, T, V> meta, Scanner<K, Scanner<F, V>> scanner)
      throws IOException;

  TableMeta<K, F, T, V> readMeta(String name) throws IOException;

  Scanner<K, Scanner<F, V>> scanData(
      String name, Set<F> fields, RangeSet<K> ranges, Filter predicate) throws IOException;

  Iterable<String> tableNames() throws IOException;

  void clear() throws IOException;

  ObjectFormat<K> getKeyFormat();

  ObjectFormat<F> getFieldFormat();
}
