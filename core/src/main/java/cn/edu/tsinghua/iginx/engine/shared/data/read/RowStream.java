package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;

public interface RowStream {

  Header getHeader() throws PhysicalException;

  void close() throws PhysicalException;

  boolean hasNext() throws PhysicalException;

  Row next() throws PhysicalException;

  default void setContext(RequestContext context) {}

  default RequestContext getContext() {
    return null;
  }
}
