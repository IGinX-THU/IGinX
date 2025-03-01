/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.vector.types.pojo.Field;

public interface Database extends AutoCloseable {

  Scanner<Long, Scanner<String, Object>> query(
      Set<Field> fields, RangeSet<Long> ranges, Filter filter) throws StorageException, IOException;

  Map<String, Long> count(Set<Field> strings)
      throws InterruptedException, IOException, StorageException;

  Set<Field> schema() throws StorageException;

  void upsertRows(Scanner<Long, Scanner<String, Object>> scanner, Map<String, DataType> schema)
      throws StorageException, InterruptedException;

  void upsertColumns(Scanner<String, Scanner<Long, Object>> scanner, Map<String, DataType> schema)
      throws StorageException, InterruptedException;

  void delete(AreaSet<Long, Field> areas) throws StorageException;

  void clear() throws StorageException;
}
