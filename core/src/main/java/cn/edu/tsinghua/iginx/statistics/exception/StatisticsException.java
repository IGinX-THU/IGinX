package cn.edu.tsinghua.iginx.statistics.exception;

import cn.edu.tsinghua.iginx.exception.IginxException;

public class StatisticsException extends IginxException {

  public StatisticsException() {
    super();
  }

  public StatisticsException(String message) {
    super(message);
  }

  public StatisticsException(String message, Throwable cause) {
    super(message, cause);
  }

  public StatisticsException(Throwable cause) {
    super(cause);
  }

  public StatisticsException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
