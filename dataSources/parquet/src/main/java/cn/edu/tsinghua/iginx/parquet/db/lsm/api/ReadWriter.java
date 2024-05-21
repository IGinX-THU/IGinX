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
import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.Set;

public interface ReadWriter {

  String getName();

  void flush(String name, TableMeta meta, Scanner<Long, Scanner<String, Object>> scanner)
      throws IOException;

  TableMeta readMeta(String name) throws IOException;

  Scanner<Long, Scanner<String, Object>> scanData(
      String name, Set<String> fields, RangeSet<Long> ranges, Filter predicate) throws IOException;

  void delete(String name, AreaSet<Long, String> areas) throws IOException;

  void delete(String name);

  Iterable<String> tableNames() throws IOException;

  void clear() throws IOException;
}
