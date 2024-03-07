package cn.edu.tsinghua.iginx.rest.exception;

import cn.edu.tsinghua.iginx.exception.IginxException;

public class RestException extends IginxException {

  public RestException() {
    super();
  }

  public RestException(String message) {
    super(message);
  }

  public RestException(String message, Throwable cause) {
    super(message, cause);
  }

  public RestException(Throwable cause) {
    super(cause);
  }

  public RestException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
