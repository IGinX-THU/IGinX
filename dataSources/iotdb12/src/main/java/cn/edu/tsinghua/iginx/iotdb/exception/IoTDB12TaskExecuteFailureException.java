package cn.edu.tsinghua.iginx.iotdb.exception;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;

public class IoTDB12TaskExecuteFailureException extends PhysicalTaskExecuteFailureException {

  public IoTDB12TaskExecuteFailureException(String message) {
    super(message);
  }

  public IoTDB12TaskExecuteFailureException(String message, Throwable cause) {
    super(message, cause);
  }
}
