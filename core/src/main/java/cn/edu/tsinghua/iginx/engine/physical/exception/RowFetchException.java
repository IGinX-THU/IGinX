package cn.edu.tsinghua.iginx.engine.physical.exception;

public class RowFetchException extends PhysicalException {

  private static final long serialVersionUID = 649847686271181167L;

  public RowFetchException(Throwable cause) {
    super(cause);
  }

  public RowFetchException(String message, Throwable cause) {
    super(message, cause);
  }
}
