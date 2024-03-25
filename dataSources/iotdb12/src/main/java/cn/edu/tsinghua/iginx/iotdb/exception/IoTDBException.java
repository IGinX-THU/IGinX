package cn.edu.tsinghua.iginx.iotdb.exception;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;

public class IoTDBException extends PhysicalException {

  public IoTDBException(String message) {
    super(message);
  }

  public IoTDBException(String message, Throwable cause) {
    super(message, cause);
  }
}
