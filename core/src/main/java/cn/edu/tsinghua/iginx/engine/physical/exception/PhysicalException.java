package cn.edu.tsinghua.iginx.engine.physical.exception;

/**
 * For more detailed information, please refer to
 * shared/src/main/java/cn/edu/tsinghua/iginx/exception/package-info.java
 */
public class PhysicalException extends Exception {

  private static final long serialVersionUID = -1547005178512213280L;

  public PhysicalException() {}

  public PhysicalException(String message) {
    super(message);
  }

  public PhysicalException(String message, Throwable cause) {
    super(message, cause);
  }

  public PhysicalException(Throwable cause) {
    super(cause);
  }

  public PhysicalException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
