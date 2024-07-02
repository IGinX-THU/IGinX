package cn.edu.tsinghua.iginx.parquet.util.exception;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalRuntimeException;

public class StorageRuntimeException extends PhysicalRuntimeException {
  public StorageRuntimeException(String message) {
    super(message);
  }

  public StorageRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public StorageRuntimeException(Throwable cause) {
    super(cause);
  }
}
