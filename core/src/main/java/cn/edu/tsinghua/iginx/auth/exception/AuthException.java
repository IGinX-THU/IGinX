package cn.edu.tsinghua.iginx.auth.exception;

import cn.edu.tsinghua.iginx.exception.IginxException;

public class AuthException extends IginxException {

  public AuthException() {
    super();
  }

  public AuthException(String message) {
    super(message);
  }

  public AuthException(String message, Throwable cause) {
    super(message, cause);
  }

  public AuthException(Throwable cause) {
    super(cause);
  }

  public AuthException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
