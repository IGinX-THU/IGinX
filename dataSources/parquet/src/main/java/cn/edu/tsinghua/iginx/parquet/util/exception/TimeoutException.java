package cn.edu.tsinghua.iginx.parquet.util.exception;

public class TimeoutException extends StorageException {
  public TimeoutException(String message) {
    super(message);
  }

  public TimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}
