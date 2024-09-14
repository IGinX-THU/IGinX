package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;

public interface BatchStream extends AutoCloseable {

  BatchSchema getSchema() throws PhysicalException;

  Batch getNext() throws PhysicalException;

  @Override
  void close() throws PhysicalException;
}
