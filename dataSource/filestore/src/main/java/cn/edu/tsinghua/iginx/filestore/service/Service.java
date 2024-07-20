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

package cn.edu.tsinghua.iginx.filestore.service;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.filestore.common.FileStoreException;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.filestore.thrift.DataUnit;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import java.util.Map;
import javax.annotation.Nullable;

public interface Service extends AutoCloseable {

  /**
   * Get the boundary of the dummy data
   *
   * @param prefix filter path by prefix, null only if match all paths
   * @return the column boundary of data in the given iginx path prefix
   * @throws FileStoreException if any exception occurs during the process
   */
  Map<DataUnit, DataBoundary> getUnits(@Nullable String prefix) throws FileStoreException;

  /**
   * Query data
   *
   * @param unit the storage unit to query, null only if query dummy data
   * @param target the data target to query
   * @param aggregate the aggregate type of the query, null only if no aggregate
   * @return the result of the query
   * @throws FileStoreException if any exception occurs during the process
   */
  RowStream query(DataUnit unit, DataTarget target, @Nullable AggregateType aggregate)
      throws FileStoreException;

  /**
   * Delete data
   *
   * @param unit the storage unit to delete
   * @param target the data target to delete
   * @throws FileStoreException if any exception occurs during the process
   */
  void delete(DataUnit unit, DataTarget target) throws FileStoreException;

  /**
   * Insert data
   *
   * @param unit the storage unit to insert
   * @param dataView the data to insert
   * @throws FileStoreException if any exception occurs during the process
   */
  void insert(DataUnit unit, DataView dataView) throws FileStoreException;
  
  @Override
  void close() throws FileStoreException;
}
