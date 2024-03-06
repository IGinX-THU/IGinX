package cn.edu.tsinghua.iginx.client.exception;

public class ClientRuntimeException extends RuntimeException {

  public ClientRuntimeException(String message) {
    super(message);
  }

  public ClientRuntimeException(String message, int errorCode) {
    super(message);
  }

  public ClientRuntimeException(String message, Throwable cause, int errorCode) {
    super(message, cause);
  }

  public ClientRuntimeException(Throwable cause, int errorCode) {
    super(cause);
  }

  public ClientRuntimeException() {
    super();
  }

  public ClientRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public ClientRuntimeException(Throwable cause) {
    super(cause);
  }

  public ClientRuntimeException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
