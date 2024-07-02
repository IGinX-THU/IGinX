package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;

public class RowStreamWrapper implements RowStream {

  private final RowStream rowStream;

  private Row nextRow; // 如果不为空，表示 row stream 的下一行已经取出来，并缓存在 wrapper 里

  public RowStreamWrapper(RowStream rowStream) {
    this.rowStream = rowStream;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return rowStream.getHeader();
  }

  @Override
  public void close() throws PhysicalException {
    rowStream.close();
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    return nextRow != null || rowStream.hasNext();
  }

  @Override
  public Row next() throws PhysicalException {
    Row row = null;
    if (nextRow != null) { // 本地已经缓存了下一行
      row = nextRow;
      nextRow = null;
    } else {
      row = rowStream.next();
    }
    return row;
  }

  public long nextTimestamp() throws PhysicalException {
    if (nextRow == null) { // 本地已经缓存了下一行
      nextRow = rowStream.next();
    }
    return nextRow.getKey();
  }
}
