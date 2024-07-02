package cn.edu.tsinghua.iginx.influxdb.exception;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;

public class InfluxDBTaskExecuteFailureException extends PhysicalTaskExecuteFailureException {

  public InfluxDBTaskExecuteFailureException(String message) {
    super(message);
  }

  public InfluxDBTaskExecuteFailureException(String message, Throwable cause) {
    super(message, cause);
  }
}
