package cn.edu.tsinghua.iginx.postgresql.exception;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;

public class PostgresqlTaskExecuteFailureException extends PhysicalTaskExecuteFailureException {

  public PostgresqlTaskExecuteFailureException(String message) {
    super(message);
  }

  public PostgresqlTaskExecuteFailureException(String message, Throwable cause) {
    super(message, cause);
  }
}
