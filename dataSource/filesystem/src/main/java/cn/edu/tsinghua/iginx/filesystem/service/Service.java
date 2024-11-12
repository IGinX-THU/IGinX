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
package cn.edu.tsinghua.iginx.filesystem.service;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.filesystem.common.FileSystemException;
import cn.edu.tsinghua.iginx.filesystem.struct.DataTarget;
import cn.edu.tsinghua.iginx.filesystem.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.filesystem.thrift.DataUnit;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import java.util.Map;
import javax.annotation.Nullable;

public interface Service extends AutoCloseable {

  /**
   * Get the boundary of the dummy data
   *
   * @param prefix filter path by prefix, null only if match all paths
   * @return the column boundary of data in the given iginx path prefix
   * @throws FileSystemException if any exception occurs during the process
   */
  Map<DataUnit, DataBoundary> getUnits(@Nullable String prefix) throws FileSystemException;

  /**
   * Query data
   *
   * @param unit the storage unit to query, null only if query dummy data
   * @param target the data target to query
   * @param aggregate the aggregate type of the query, null only if no aggregate
   * @return the result of the query
   * @throws FileSystemException if any exception occurs during the process
   */
  RowStream query(DataUnit unit, DataTarget target, @Nullable AggregateType aggregate)
      throws FileSystemException;

  /**
   * Delete data
   *
   * @param unit the storage unit to delete
   * @param target the data target to delete
   * @throws FileSystemException if any exception occurs during the process
   */
  void delete(DataUnit unit, DataTarget target) throws FileSystemException;

  /**
   * Insert data
   *
   * @param unit the storage unit to insert
   * @param dataView the data to insert
   * @throws FileSystemException if any exception occurs during the process
   */
  void insert(DataUnit unit, DataView dataView) throws FileSystemException;

  @Override
  void close() throws FileSystemException;
}
