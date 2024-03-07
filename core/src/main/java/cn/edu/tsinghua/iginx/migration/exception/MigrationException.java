package cn.edu.tsinghua.iginx.migration.exception;

import cn.edu.tsinghua.iginx.exception.IginxException;

public class MigrationException extends IginxException {

  public MigrationException() {
    super();
  }

  public MigrationException(String message) {
    super(message);
  }

  public MigrationException(String message, Throwable cause) {
    super(message, cause);
  }

  public MigrationException(Throwable cause) {
    super(cause);
  }

  public MigrationException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
