package cn.edu.tsinghua.iginx.rest.exception;

import cn.edu.tsinghua.iginx.exception.IginxException;

public class RESTException extends IginxException {

  public RESTException() {
    super();
  }

  public RESTException(String message) {
    super(message);
  }

  public RESTException(String message, Throwable cause) {
    super(message, cause);
  }

  public RESTException(Throwable cause) {
    super(cause);
  }

  public RESTException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
