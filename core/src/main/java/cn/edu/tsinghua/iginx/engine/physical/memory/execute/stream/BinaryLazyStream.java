package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;

public abstract class BinaryLazyStream implements RowStream {

  protected final RowStream streamA;

  protected final RowStream streamB;

  protected RequestContext context;

  public BinaryLazyStream(RowStream streamA, RowStream streamB) {
    this.streamA = streamA;
    this.streamB = streamB;
  }

  @Override
  public void close() throws PhysicalException {
    PhysicalException pe = null;
    try {
      streamA.close();
    } catch (PhysicalException e) {
      pe = e;
    }
    try {
      streamB.close();
    } catch (PhysicalException e) {
      pe = e;
    }
    if (pe != null) {
      throw pe;
    }
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
