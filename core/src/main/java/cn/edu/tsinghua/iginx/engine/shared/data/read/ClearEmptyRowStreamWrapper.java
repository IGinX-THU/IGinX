package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;

public class ClearEmptyRowStreamWrapper implements RowStream {

  private final RowStream stream;

  private Row nextRow;

  public ClearEmptyRowStreamWrapper(RowStream stream) {
    this.stream = stream;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return stream.getHeader();
  }

  @Override
  public void close() throws PhysicalException {
    stream.close();
  }

  @Override
  public boolean hasNext() throws PhysicalException { // 调用 hasNext 之后，如果返回 true，那么 nextRow 必然存在
    if (nextRow != null) {
      return true;
    }
    loadNextRow();
    return nextRow != null;
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new PhysicalException("the row stream has used up");
    }
    Row row = nextRow;
    nextRow = null;
    return row;
  }

  private void loadNextRow() throws PhysicalException {
    if (nextRow != null) {
      return;
    }
    do {
      if (!stream.hasNext()) {
        nextRow = null;
        break;
      }
      nextRow = stream.next();
    } while (nextRow.isEmpty());
  }
}
