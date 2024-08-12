package cn.edu.tsinghua.iginx.filestore.common;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;

public abstract class FileStoreRowStream implements RowStream {

  @Override
  public abstract Header getHeader() throws FileStoreException;

  @Override
  public abstract void close() throws FileStoreException;

  @Override
  public abstract boolean hasNext() throws FileStoreException;

  @Override
  public abstract Row next() throws FileStoreException;
}
