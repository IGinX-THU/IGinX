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
package cn.edu.tsinghua.iginx.minio.tree.format;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.RangeSet;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/** Parses an object as columns, the key of a row is its index in the object. */
public interface ObjectFormat {

  /** name of the format, also the key of its config in {@code formats} */
  String getName();

  /** extensions of objects in this format, without dot */
  List<String> getExtensions();

  /**
   * @param prefix prefix of all columns of the object, null means no prefix
   * @param config format config, such as {@code formats.CSV}
   */
  Reader newReader(@Nullable String prefix, Path path, Config config) throws IOException;

  interface Reader {

    /** columns matching any of the patterns, with their types */
    Map<String, DataType> find(Collection<String> patterns) throws IOException;

    /**
     * read columns of rows whose key is in keys, rows are ordered by key
     *
     * @param fields columns returned by {@link #find}
     */
    RowStream read(List<String> fields, RangeSet<Long> keys) throws IOException;
  }
}
