package cn.edu.tsinghua.iginx.client.exception;

public class ClientException extends Exception {

  public ClientException(String message) {
    super(message);
  }

  public ClientException(String message, int errorCode) {
    super(message);
  }

  public ClientException(String message, Throwable cause, int errorCode) {
    super(message, cause);
  }

  public ClientException(Throwable cause, int errorCode) {
    super(cause);
  }

  public ClientException() {
    super();
  }

  public ClientException(String message, Throwable cause) {
    super(message, cause);
  }

  public ClientException(Throwable cause) {
    super(cause);
  }

  public ClientException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
