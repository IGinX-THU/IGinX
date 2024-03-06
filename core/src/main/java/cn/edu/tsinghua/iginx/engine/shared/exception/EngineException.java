package cn.edu.tsinghua.iginx.engine.shared.exception;

import cn.edu.tsinghua.iginx.exception.IginxException;

public class EngineException extends IginxException {

  public EngineException(String message) {
    super(message);
  }

  public EngineException(String message, int errorCode) {
    super(message, errorCode);
  }

  public EngineException(String message, Throwable cause, int errorCode) {
    super(message, cause, errorCode);
  }

  public EngineException(Throwable cause, int errorCode) {
    super(cause, errorCode);
  }

  public EngineException() {
    super();
  }

  public EngineException(String message, Throwable cause) {
    super(message, cause);
  }

  public EngineException(Throwable cause) {
    super(cause);
  }

  public EngineException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
