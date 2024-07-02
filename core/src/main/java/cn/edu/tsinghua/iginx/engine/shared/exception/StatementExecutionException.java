package cn.edu.tsinghua.iginx.engine.shared.exception;

import cn.edu.tsinghua.iginx.exception.IginxException;
import cn.edu.tsinghua.iginx.exception.StatusCode;
import cn.edu.tsinghua.iginx.thrift.Status;

public class StatementExecutionException extends IginxException {

  private static final long serialVersionUID = -7769482614133326007L;

  public StatementExecutionException(Status status) {
    super(status.message, status.code);
  }

  public StatementExecutionException(String message) {
    super(message, StatusCode.STATEMENT_EXECUTION_ERROR.getStatusCode());
  }

  public StatementExecutionException(Throwable cause) {
    super(cause, StatusCode.STATEMENT_EXECUTION_ERROR.getStatusCode());
  }

  public StatementExecutionException(String message, Throwable cause) {
    super(message, cause, StatusCode.STATEMENT_EXECUTION_ERROR.getStatusCode());
  }
}
