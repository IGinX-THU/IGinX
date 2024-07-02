package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.RowFetchException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import java.util.NoSuchElementException;

public class FilterRowStreamWrapper implements RowStream {

  private final RowStream stream;

  private final Filter filter;

  private Row nextRow;

  public FilterRowStreamWrapper(RowStream stream, Filter filter) {
    this.stream = stream;
    this.filter = filter;
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
  public boolean hasNext() throws PhysicalException {
    if (nextRow != null) {
      return true;
    }
    nextRow = loadNextRow();
    return nextRow != null;
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new RowFetchException(new NoSuchElementException());
    }

    Row row = nextRow;
    nextRow = null;
    return row;
  }

  private Row loadNextRow() throws PhysicalException {
    while (stream.hasNext()) {
      Row row = stream.next();
      if (FilterUtils.validate(filter, row)) {
        return row;
      }
    }
    return null;
  }
}
