package cn.edu.tsinghua.iginx.engine.physical.exception;

public class PhysicalTaskExecuteFailureException extends PhysicalException {

  private static final long serialVersionUID = 1535411462205865453L;

  public PhysicalTaskExecuteFailureException(String message) {
    super(message);
  }

  public PhysicalTaskExecuteFailureException(String message, Throwable cause) {
    super(message, cause);
  }
}
