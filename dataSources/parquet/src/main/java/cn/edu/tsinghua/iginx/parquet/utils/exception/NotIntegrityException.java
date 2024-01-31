package cn.edu.tsinghua.iginx.parquet.utils.exception;

public class NotIntegrityException extends StorageRuntimeException {
  public NotIntegrityException(String message) {
    super(message);
  }

  public NotIntegrityException(String message, Throwable cause) {
    super(message, cause);
  }

  public NotIntegrityException(Throwable cause) {
    super(cause);
  }
}
