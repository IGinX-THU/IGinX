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
package cn.edu.tsinghua.iginx.parquet.io;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface FileMeta {

  /**
   * get all fields' name in file
   *
   * @return all names of fields in file. null if not existed
   */
  @Nullable
  Set<String> fields();

  /**
   * get type of specified field
   *
   * @param field field type
   * @return type of specified field. null if not existed
   */
  @Nullable
  DataType getType(@Nonnull String field);

  /**
   * get extra information of file
   *
   * @return extra information of file. null if not existed
   */
  @Nullable
  Map<String, String> extra();
}
