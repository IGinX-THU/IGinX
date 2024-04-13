package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Rename;
import java.util.Map;

public class RenameLazyStream extends UnaryLazyStream {

  private final Rename rename;

  private Header header;

  public RenameLazyStream(Rename rename, RowStream stream) {
    super(stream);
    this.rename = rename;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (header == null) {
      Header header = stream.getHeader();
      Map<String, String> aliasMap = rename.getAliasMap();

      this.header = header.renamedHeader(aliasMap, rename.getIgnorePatterns());
    }
    return header;
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    return stream.hasNext();
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }

    Row row = stream.next();
    if (header.hasKey()) {
      return new Row(header, row.getKey(), row.getValues());
    } else {
      return new Row(header, row.getValues());
    }
  }
}
