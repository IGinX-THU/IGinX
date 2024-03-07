package cn.edu.tsinghua.iginx.compaction.exception;

import cn.edu.tsinghua.iginx.exception.IginxException;

public class CompactionException extends IginxException {
  public CompactionException() {
    super();
  }

  public CompactionException(String message) {
    super(message);
  }

  public CompactionException(String message, Throwable cause) {
    super(message, cause);
  }

  public CompactionException(Throwable cause) {
    super(cause);
  }

  public CompactionException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
