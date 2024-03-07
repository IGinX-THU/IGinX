package cn.edu.tsinghua.iginx.monitor.exception;

import cn.edu.tsinghua.iginx.exception.IginxException;

public class MonitorException extends IginxException {

  public MonitorException() {
    super();
  }

  public MonitorException(String message) {
    super(message);
  }

  public MonitorException(String message, Throwable cause) {
    super(message, cause);
  }

  public MonitorException(Throwable cause) {
    super(cause);
  }

  public MonitorException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
