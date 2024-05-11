package cn.edu.tsinghua.iginx.relational.exception;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;

public class RelationalException extends PhysicalException {

  public RelationalException(Throwable cause) {
    super(cause);
  }

  public RelationalException(String message) {
    super(message);
  }

  public RelationalException(String message, Throwable cause) {
    super(message, cause);
  }
}
