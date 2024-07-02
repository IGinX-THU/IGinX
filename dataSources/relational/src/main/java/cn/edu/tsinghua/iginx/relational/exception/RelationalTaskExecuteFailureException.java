package cn.edu.tsinghua.iginx.relational.exception;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;

public class RelationalTaskExecuteFailureException extends PhysicalTaskExecuteFailureException {

  public RelationalTaskExecuteFailureException(String message) {
    super(message);
  }

  public RelationalTaskExecuteFailureException(String message, Throwable cause) {
    super(message, cause);
  }
}
