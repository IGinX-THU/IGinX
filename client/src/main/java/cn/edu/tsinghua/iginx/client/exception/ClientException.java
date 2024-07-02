package cn.edu.tsinghua.iginx.client.exception;

/**
 * For more detailed information, please refer to
 * shared/src/main/java/cn/edu/tsinghua/iginx/exception/package-info.java
 */
public class ClientException extends Exception {

  private static final long serialVersionUID = 1747821121898717874L;

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
