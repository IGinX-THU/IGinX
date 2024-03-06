package cn.edu.tsinghua.iginx.engine.logical.exception;

import cn.edu.tsinghua.iginx.engine.shared.exception.EngineException;

public class LogicalException extends EngineException {

  public LogicalException(String message) {
    super(message);
  }

  public LogicalException(String message, int errorCode) {
    super(message, errorCode);
  }

  public LogicalException(String message, Throwable cause, int errorCode) {
    super(message, cause, errorCode);
  }

  public LogicalException(Throwable cause, int errorCode) {
    super(cause, errorCode);
  }

  public LogicalException() {
    super();
  }

  public LogicalException(String message, Throwable cause) {
    super(message, cause);
  }

  public LogicalException(Throwable cause) {
    super(cause);
  }

  public LogicalException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
