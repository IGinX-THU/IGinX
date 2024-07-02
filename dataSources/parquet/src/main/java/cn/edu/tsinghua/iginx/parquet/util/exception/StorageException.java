package cn.edu.tsinghua.iginx.parquet.util.exception;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;

public class StorageException extends PhysicalException {

  public StorageException(Throwable cause) {
    super(cause);
  }

  public StorageException(String message) {
    super(message);
  }

  public StorageException(String message, Throwable cause) {
    super(message, cause);
  }
}
