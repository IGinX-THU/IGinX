package cn.edu.tsinghua.iginx.influxdb.exception;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;

public class InfluxdbException extends PhysicalException {

  public InfluxdbException(String message) {
    super(message);
  }

  public InfluxdbException(String message, Throwable cause) {
    super(message, cause);
  }

  public InfluxdbException(Throwable cause) {
    super(cause);
  }
}
