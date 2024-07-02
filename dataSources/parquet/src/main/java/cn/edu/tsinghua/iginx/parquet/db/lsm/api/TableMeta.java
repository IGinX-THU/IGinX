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

import com.google.common.collect.Range;
import java.util.Map;

public interface TableMeta<K extends Comparable<K>, F, T, V> {
  Map<F, T> getSchema();

  /**
   * Get the range of the table. 若某个字段没有数据，则返回的range为null
   *
   * @return the range of the table, the key is the field name, the value is the range of the field
   */
  Map<F, Range<K>> getRanges();
}
