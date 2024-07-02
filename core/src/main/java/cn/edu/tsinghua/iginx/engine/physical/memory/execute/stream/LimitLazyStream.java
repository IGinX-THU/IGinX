package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Limit;

public class LimitLazyStream extends UnaryLazyStream {

  private final Limit limit;

  private int index = 0;

  public LimitLazyStream(Limit limit, RowStream stream) {
    super(stream);
    this.limit = limit;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return stream.getHeader();
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    while (index < limit.getOffset() && stream.hasNext()) {
      stream.next();
      index++;
    }
    return index - limit.getOffset() < limit.getLimit() && stream.hasNext();
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    index++;
    return stream.next();
  }
}
