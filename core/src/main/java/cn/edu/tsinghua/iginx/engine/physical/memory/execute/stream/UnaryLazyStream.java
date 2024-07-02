package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;

public abstract class UnaryLazyStream implements RowStream {

  protected final RowStream stream;

  protected RequestContext context;

  public UnaryLazyStream(RowStream stream) {
    this.stream = stream;
  }

  @Override
  public void close() throws PhysicalException {
    stream.close();
  }

  @Override
  public void setContext(RequestContext context) {
    this.context = context;
  }

  @Override
  public RequestContext getContext() {
    return context;
  }
}
