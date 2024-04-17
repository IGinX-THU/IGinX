package cn.edu.tsinghua.iginx.relational.exception;

public class PostgresqlTaskExecuteFailureException extends RelationalTaskExecuteFailureException {

  public PostgresqlTaskExecuteFailureException(String message) {
    super(message);
  }

  public PostgresqlTaskExecuteFailureException(String message, Throwable cause) {
    super(message, cause);
  }
}
