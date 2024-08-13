package cn.edu.tsinghua.iginx.filestore.format.raw;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream.EmptyRowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filestore.common.Filters;
import cn.edu.tsinghua.iginx.filestore.common.Patterns;
import cn.edu.tsinghua.iginx.filestore.common.RowStreams;
import cn.edu.tsinghua.iginx.filestore.format.FileFormat;
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
    return "RawReader{" +
        "config=" + config +
        ", path=" + path +
        '}';
  }

  @Override
  public Map<String, DataType> find(Collection<String> fieldPatterns) throws IOException {
    if(!Patterns.match(fieldPatterns, fieldName)) {
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
        new RawFormatRowStream(
            header,
            path,
            config.getPageSize().toBytes(),
            keyRanges);

    if (!Filters.match(filter, removeNonKeyFilter)) {
      rowStream = RowStreams.filtered(rowStream, filter);
    }

    return rowStream;
  }

  @Override
  public void close() throws IOException {
  }
}
