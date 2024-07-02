package cn.edu.tsinghua.iginx.exception;

public class IginxRuntimeException extends RuntimeException {
  public IginxRuntimeException() {}

  public IginxRuntimeException(String message) {
    super(message);
  }

  public IginxRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public IginxRuntimeException(Throwable cause) {
    super(cause);
  }

  public IginxRuntimeException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
