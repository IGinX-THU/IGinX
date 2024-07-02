package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import java.util.Collections;

public class EmptyRowStream implements RowStream {

  private final Header header;

  public EmptyRowStream() {
    this.header = new Header(Field.KEY, Collections.emptyList());
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return header;
  }

  @Override
  public void close() throws PhysicalException {}

  @Override
  public boolean hasNext() throws PhysicalException {
    return false;
  }

  @Override
  public Row next() throws PhysicalException {
    return null;
  }
}
