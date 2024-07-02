package cn.edu.tsinghua.iginx.exception;

import cn.edu.tsinghua.iginx.thrift.Status;

/**
 * For more detailed information, please refer to
 * shared/src/main/java/cn/edu/tsinghua/iginx/exception/package-info.java
 */
public class SessionException extends Exception {

  private static final long serialVersionUID = -2811585771984779297L;

  protected int errorCode;

  public SessionException(Status status) {
    super(status.message);
    errorCode = status.code;
  }

  public SessionException(String message) {
    super(message);
    errorCode = StatusCode.SESSION_ERROR.getStatusCode();
  }

  public SessionException(Throwable cause) {
    super(cause);
    errorCode = StatusCode.SESSION_ERROR.getStatusCode();
  }

  public SessionException(String message, Throwable cause) {
    super(message, cause);
    errorCode = StatusCode.SESSION_ERROR.getStatusCode();
  }
}
