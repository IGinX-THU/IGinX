/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.parquet.db.lsm.api;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.Set;

public interface ReadWriter<K extends Comparable<K>, F, T, V> {

  void flush(String name, TableMeta<K, F, T, V> meta, Scanner<K, Scanner<F, V>> scanner)
      throws IOException;

  TableMeta<K, F, T, V> readMeta(String name) throws IOException;

  Scanner<K, Scanner<F, V>> scanData(
      String name, Set<F> fields, RangeSet<K> ranges, Filter predicate) throws IOException;

  void delete(String name, AreaSet<K, F> areas) throws IOException;

  void delete(String name);

  Iterable<String> tableNames() throws IOException;

  void clear() throws IOException;
}
