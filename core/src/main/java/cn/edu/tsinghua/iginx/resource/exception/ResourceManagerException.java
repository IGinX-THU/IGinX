package cn.edu.tsinghua.iginx.resource.exception;

import cn.edu.tsinghua.iginx.exception.IginxException;

public class ResourceManagerException extends IginxException {

  public ResourceManagerException() {
    super();
  }

  public ResourceManagerException(String message) {
    super(message);
  }

  public ResourceManagerException(String message, Throwable cause) {
    super(message, cause);
  }

  public ResourceManagerException(Throwable cause) {
    super(cause);
  }

  public ResourceManagerException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
