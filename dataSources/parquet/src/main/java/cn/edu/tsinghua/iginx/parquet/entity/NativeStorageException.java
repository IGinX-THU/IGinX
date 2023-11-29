package cn.edu.tsinghua.iginx.parquet.entity;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;

public class NativeStorageException extends PhysicalException {

  public NativeStorageException(Throwable cause) {
    super(cause);
  }

  public NativeStorageException(String message) {
    super(message);
  }

  public NativeStorageException(String message, Throwable cause) {
    super(message, cause);
  }
}
