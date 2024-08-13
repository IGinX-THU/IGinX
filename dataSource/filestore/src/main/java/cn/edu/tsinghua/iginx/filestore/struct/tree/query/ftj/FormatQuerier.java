package cn.edu.tsinghua.iginx.filestore.struct.tree.query.ftj;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.BoolFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filestore.common.Filters;
import cn.edu.tsinghua.iginx.filestore.common.Patterns;
import cn.edu.tsinghua.iginx.filestore.common.RowStreams;
import cn.edu.tsinghua.iginx.filestore.format.FileFormat;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier;
import cn.edu.tsinghua.iginx.thrift.DataType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

class FormatQuerier implements Querier {

  private final FileFormat.Reader reader;
  private final List<String> patterns;
  private final Filter filter;

  FormatQuerier(FileFormat.Reader reader, DataTarget target) {
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
    return "FormatQuerier{" +
        "reader=" + reader +
        ", patterns=" + patterns +
        ", filter=" + filter +
        '}';
  }

  @Override
  public List<RowStream> query() throws IOException {
    RowStream rowStream = doQuery();
    if (rowStream == null) {
      return Collections.emptyList();
    } else {
      return Collections.singletonList(rowStream);
    }
  }

  @Nullable
  private RowStream doQuery() throws IOException {
    Map<String, DataType> schema = reader.find(patterns);
    if (schema.isEmpty()) {
      return null;
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
