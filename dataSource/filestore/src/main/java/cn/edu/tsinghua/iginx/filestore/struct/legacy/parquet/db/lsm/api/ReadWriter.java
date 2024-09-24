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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.lsm.api;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.util.iterator.Scanner;
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

  Iterable<String> reload() throws IOException;

  void clear() throws IOException;
}
