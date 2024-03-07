package cn.edu.tsinghua.iginx.mqtt.exception;

import cn.edu.tsinghua.iginx.exception.IginxException;

public class MqttException extends IginxException {

  public MqttException() {
    super();
  }

  public MqttException(String message) {
    super(message);
  }

  public MqttException(String message, Throwable cause) {
    super(message, cause);
  }

  public MqttException(Throwable cause) {
    super(cause);
  }

  public MqttException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
