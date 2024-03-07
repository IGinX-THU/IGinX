package cn.edu.tsinghua.iginx.policy.exception;

import cn.edu.tsinghua.iginx.exception.IginxException;

public class PolicyException extends IginxException {

  public PolicyException() {
    super();
  }

  public PolicyException(String message) {
    super(message);
  }

  public PolicyException(String message, Throwable cause) {
    super(message, cause);
  }

  public PolicyException(Throwable cause) {
    super(cause);
  }

  public PolicyException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
