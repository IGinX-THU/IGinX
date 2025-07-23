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
package cn.edu.tsinghua.iginx.filesystem.struct.tree.query.ftj;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.BoolFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.Filters;
import cn.edu.tsinghua.iginx.filesystem.common.Patterns;
import cn.edu.tsinghua.iginx.filesystem.common.RowStreams;
import cn.edu.tsinghua.iginx.filesystem.common.Strings;
import cn.edu.tsinghua.iginx.filesystem.format.FileFormat;
import cn.edu.tsinghua.iginx.filesystem.struct.DataTarget;
import cn.edu.tsinghua.iginx.filesystem.struct.tree.query.AbstractQuerier;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

class FormatQuerier extends AbstractQuerier {

  private final FileFormat.Reader reader;
  private final List<String> patterns;
  private final Filter filter;

  FormatQuerier(
      Path path,
      String prefix,
      DataTarget target,
      FileFormat.Reader reader,
      ExecutorService executor) {
    super(path, prefix, target, executor);
    this.reader = Objects.requireNonNull(reader);
    this.patterns = Patterns.nonNull(target.getPatterns());
    this.filter = Filters.isTrue(target.getFilter()) ? new BoolFilter(true) : target.getFilter();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public String toString() {
    return super.toString() + "&reader=" + Strings.shiftWithNewline(reader.toString());
  }

  @Override
  public List<Future<RowStream>> query() {
    Future<RowStream> rowStream = getExecutor().submit(this::doQuery);
    return Collections.singletonList(rowStream);
  }

  private RowStream doQuery() throws IOException {
    Map<String, DataType> schema = reader.find(patterns);
    if (schema.isEmpty()) {
      return RowStreams.empty();
    }
    if (Filters.isFalse(filter)) {
      List<Field> fields = new ArrayList<>();
      schema.forEach((name, type) -> fields.add(new Field(name, type)));
      Header header = new Header(Field.KEY, fields);
      return RowStreams.empty(header);
    }
    Set<String> fields = Filters.getPaths(filter);
    if (fields.isEmpty()) {
      return reader.read(new ArrayList<>(schema.keySet()), filter);
    }
    Map<String, DataType> allSchema = reader.find(fields);
    Filter superSetFilter = Filters.matchWildcard(filter, allSchema.keySet());
    RowStream rowStream = reader.read(new ArrayList<>(schema.keySet()), superSetFilter);
    if (Filters.equals(filter, superSetFilter)) {
      return rowStream;
    }
    return RowStreams.filtered(rowStream, filter);
  }
}
