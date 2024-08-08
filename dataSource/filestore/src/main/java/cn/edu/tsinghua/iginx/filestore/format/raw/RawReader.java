package cn.edu.tsinghua.iginx.filestore.format.raw;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream.EmptyRowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filestore.common.Filters;
import cn.edu.tsinghua.iginx.filestore.format.FileReader;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.RangeSet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

public class RawReader implements FileReader {

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
  public Map<String, DataType> findFields(Collection<String> fieldPatterns) throws IOException {
    return schema;
  }

  @Override
  public RowStream readRows(Predicate<String> fieldMatcher, Filter filter) throws IOException {
    if (!fieldMatcher.test(fieldName)) {
      return new EmptyRowStream();
    }

    Predicate<Filter> removeNonKeyFilter = Filters.removeNonKeyFilter();

    Filter keyRangeFilter = Filters.superSet(filter, removeNonKeyFilter);
    RangeSet<Long> keyRanges = Filters.toRangeSet(keyRangeFilter);
    RowStream rowStream = new RawFileRowStream(
        header,
        Files.newByteChannel(path, StandardOpenOption.READ),
        config.getPageSize().toBytes(),
        keyRanges);

    if (!Filters.match(filter, removeNonKeyFilter)) {
      rowStream = Filters.filter(rowStream, filter);
    }

    return rowStream;
  }

  @Override
  public void close() throws IOException {
  }
}
