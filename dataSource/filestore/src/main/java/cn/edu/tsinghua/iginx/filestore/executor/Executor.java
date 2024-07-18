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

package cn.edu.tsinghua.iginx.filestore.executor;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

public interface Executor extends AutoCloseable {

  /**
   * Get the boundary of the dummy data
   *
   * @param prefix filter path by prefix, null only if match all paths
   * @return the column boundary of the dummy data in the given iginx path prefix
   * @throws FileStoreException if any exception occurs during the process
   */
  ColumnsInterval getDummyBoundary(@Nullable String prefix) throws FileStoreException;

  /**
   * Get the schema of all data
   *
   * @param patterns filter columns, null only if return all columns, empty list only if not query
   * @param tagFilter filter tags, null only if return all tags
   * @return the matched fields of each storage unit. if storage unit is null, the fields are from
   *     dummy data
   * @throws FileStoreException if any exception occurs during the process
   */
  Map<String, Set<Field>> getSchema(@Nullable List<String> patterns, @Nullable TagFilter tagFilter)
      throws FileStoreException;

  /**
   * Query data
   *
   * @param storageUnit the storage unit to query, null only if query dummy data
   * @param filter filter rows, null only if return all rows
   * @param patterns filter columns, null only if return all columns, empty list only if not query
   * @param tagFilter filter tags, null only if return all tags
   * @param functionCalls call functions to the data, null only if return raw data, empty list only
   *     if no data to be queried
   * @return the result of the query
   * @throws FileStoreException if any exception occurs during the process
   */
  RowStream query(
      @Nullable String storageUnit,
      @Nullable Filter filter,
      @Nullable List<String> patterns,
      @Nullable TagFilter tagFilter,
      @Nullable List<FunctionCall> functionCalls)
      throws FileStoreException;

  /**
   * Insert data
   *
   * @param storageUnit the storage unit to insert
   * @param dataView the data to insert
   * @throws FileStoreException if any exception occurs during the process
   */
  void insert(String storageUnit, DataView dataView) throws FileStoreException;

  /**
   * Delete data
   *
   * @param storageUnit the storage unit to delete
   * @param filter filter rows, null only if delete all rows
   * @param patterns filter columns, null only if delete all columns, empty list only if not deleted
   * @param tagFilter filter tags, null only if delete all tags
   * @throws FileStoreException if any exception occurs during the process
   */
  void delete(
      String storageUnit,
      @Nullable Filter filter,
      @Nullable List<String> patterns,
      @Nullable TagFilter tagFilter)
      throws FileStoreException;
}
