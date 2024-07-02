package cn.edu.tsinghua.iginx.exception;

/**
 * For more detailed information, please refer to
 * shared/src/main/java/cn/edu/tsinghua/iginx/exception/package-info.java
 */
public class IginxException extends Exception {

  private static final long serialVersionUID = -1355829042896588219L;
  protected int errorCode;

  public IginxException(String message, int errorCode) {
    super(message);
    this.errorCode = errorCode;
  }

  public IginxException(String message, Throwable cause, int errorCode) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  public IginxException(Throwable cause, int errorCode) {
    super(cause);
    this.errorCode = errorCode;
  }

  public int getErrorCode() {
    return errorCode;
  }
}
