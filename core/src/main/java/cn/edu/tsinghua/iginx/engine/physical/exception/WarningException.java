package cn.edu.tsinghua.iginx.engine.physical.exception;

public class WarningException extends PhysicalException {

  private static final long serialVersionUID = -7588140856540962133L;

  public WarningException(Throwable cause) {
    super(cause);
  }

  public WarningException(String message) {
    super(message);
  }
}
