package cn.edu.tsinghua.iginx.iotdb.exception;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;

public class IoTDBTaskExecuteFailureException extends PhysicalTaskExecuteFailureException {

  public IoTDBTaskExecuteFailureException(String message) {
    super(message);
  }

  public IoTDBTaskExecuteFailureException(String message, Throwable cause) {
    super(message, cause);
  }
}
