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
package cn.edu.tsinghua.iginx.filesystem.format.raw;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream.EmptyRowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.Filters;
import cn.edu.tsinghua.iginx.filesystem.common.Patterns;
import cn.edu.tsinghua.iginx.filesystem.common.RowStreams;
import cn.edu.tsinghua.iginx.filesystem.format.FileFormat;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Predicate;

public class RawReader implements FileFormat.Reader {

  private final RawReaderConfig config;
  private final Path path;
  private final String fieldName;
  private final Map<String, DataType> schema;
  private final Header header;

  public RawReader(String prefix, Path path, RawReaderConfig config) throws IOException {
    this.config = config;
    this.path = path;
    this.fieldName = Objects.requireNonNull(prefix);
    this.schema = Collections.singletonMap(fieldName, DataType.BINARY);
    Field field = new Field(fieldName, DataType.BINARY);
    this.header = new Header(Field.KEY, Collections.singletonList(field));
  }

  @Override
  public String toString() {
    return "RawReader{" + "config=" + config + '}';
  }

  @Override
  public Map<String, DataType> find(Collection<String> patterns) throws IOException {
    if (!Patterns.match(patterns, fieldName)) {
      return Collections.emptyMap();
    }
    return schema;
  }

  @Override
  public RowStream read(List<String> fields, Filter filter) throws IOException {
    if (fields.isEmpty()) {
      return new EmptyRowStream();
    }

    if (!Objects.equals(fields, Collections.singletonList(fieldName))) {
      throw new IllegalArgumentException("Unknown fields: " + fields);
    }

    Predicate<Filter> removeNonKeyFilter = Filters.nonKeyFilter();

    Filter keyRangeFilter = Filters.superSet(filter, removeNonKeyFilter);
    RangeSet<Long> keyRanges = Filters.toRangeSet(keyRangeFilter);
    RowStream rowStream =
        new RawFormatRowStream(header, path, config.getPageSize().toBytes(), keyRanges);

    if (!Filters.match(filter, removeNonKeyFilter)) {
      rowStream = RowStreams.filtered(rowStream, filter);
    }

    return rowStream;
  }

  @Override
  public void close() throws IOException {}
}
