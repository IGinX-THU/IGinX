package cn.edu.tsinghua.iginx.exception;

public enum StatusCode {
  WRONG_USERNAME_OR_PASSWORD(100),
  ACCESS_DENY(101),

  SUCCESS_STATUS(200),
  PARTIAL_SUCCESS(204),

  REDIRECT(300),

  SESSION_ERROR(400),
  STATEMENT_EXECUTION_ERROR(401),
  STATEMENT_PARSE_ERROR(402),

  SYSTEM_ERROR(500),
  SERVICE_UNAVAILABLE(503),

  JOB_FINISHED(600),
  JOB_CREATED(601),
  JOB_RUNNING(602),
  JOB_FAILING(603),
  JOB_FAILED(604),
  JOB_CLOSING(605),
  JOB_CLOSED(606);

  private int statusCode;

  StatusCode(int statusCode) {
    this.statusCode = statusCode;
  }

  public int getStatusCode() {
    return statusCode;
  }
}
