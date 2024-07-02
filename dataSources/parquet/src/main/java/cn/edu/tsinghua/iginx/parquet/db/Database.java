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
package cn.edu.tsinghua.iginx.parquet.db;

import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import java.util.Map;

public interface Database<K extends Comparable<K>, F, T, V> extends ImmutableDatabase<K, F, T, V> {

  void upsertRows(Scanner<K, Scanner<F, V>> scanner, Map<F, T> schema) throws StorageException;

  void upsertColumns(Scanner<F, Scanner<K, V>> scanner, Map<F, T> schema) throws StorageException;

  void delete(AreaSet<K, F> areas) throws StorageException;

  void clear() throws StorageException;
}
