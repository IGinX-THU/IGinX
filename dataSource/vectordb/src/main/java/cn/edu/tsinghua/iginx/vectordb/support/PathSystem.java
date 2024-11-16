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
package cn.edu.tsinghua.iginx.vectordb.support;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.List;
import java.util.Map;

public interface PathSystem {

  void addPath(String path, boolean isDummy, DataType type);

  boolean deletePath(String path);

  List<String> findPaths(String pattern, TagFilter tagFilter);

  Column getColumn(String path);

  Map<String, Column> getColumns();

  /**
   * 获取完整的path（collection.field)，不存在返回null
   *
   * @param path
   * @param tags
   * @return
   */
  String findPath(String path, Map<String, String> tags);

  /**
   * 根据collection名称获取完整的path，不存在返回null
   *
   * @param path
   * @return
   */
  String findCollection(String path);
}
