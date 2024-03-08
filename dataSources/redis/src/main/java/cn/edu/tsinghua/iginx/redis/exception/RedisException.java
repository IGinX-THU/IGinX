package cn.edu.tsinghua.iginx.redis.exception;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;

public class RedisException extends PhysicalException {

  public RedisException(String message) {
    super(message);
  }

  public RedisException(String message, Throwable cause) {
    super(message, cause);
  }
}
