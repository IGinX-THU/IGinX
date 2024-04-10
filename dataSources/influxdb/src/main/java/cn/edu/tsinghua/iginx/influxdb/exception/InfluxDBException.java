package cn.edu.tsinghua.iginx.influxdb.exception;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;

public class InfluxDBException extends PhysicalException {

  public InfluxDBException(String message) {
    super(message);
  }

  public InfluxDBException(String message, Throwable cause) {
    super(message, cause);
  }

  public InfluxDBException(Throwable cause) {
    super(cause);
  }
}
