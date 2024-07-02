package cn.edu.tsinghua.iginx.engine.physical.exception;

import cn.edu.tsinghua.iginx.exception.IginxRuntimeException;

public class PhysicalRuntimeException extends IginxRuntimeException {

  public PhysicalRuntimeException(String message) {
    super(message);
  }

  public PhysicalRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public PhysicalRuntimeException(Throwable cause) {
    super(cause);
  }
}
