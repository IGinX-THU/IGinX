package cn.edu.tsinghua.iginx.utils;

import cn.edu.tsinghua.iginx.exception.StatusCode;
import cn.edu.tsinghua.iginx.thrift.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcUtils {
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(RpcUtils.class);

  public static Status WRONG_USERNAME_OR_PASSWORD =
      new Status(StatusCode.WRONG_USERNAME_OR_PASSWORD.getStatusCode());

  public static Status ACCESS_DENY = new Status(StatusCode.ACCESS_DENY.getStatusCode());

  public static Status SUCCESS = new Status(StatusCode.SUCCESS_STATUS.getStatusCode());

  public static Status PARTIAL_SUCCESS = new Status(StatusCode.PARTIAL_SUCCESS.getStatusCode());

  public static Status FAILURE = new Status(StatusCode.STATEMENT_EXECUTION_ERROR.getStatusCode());

  public static Status SERVICE_UNAVAILABLE =
      new Status(StatusCode.SERVICE_UNAVAILABLE.getStatusCode());

  public static Status JOB_FINISHED = new Status(StatusCode.JOB_FINISHED.getStatusCode());
  public static Status JOB_CREATED = new Status(StatusCode.JOB_CREATED.getStatusCode());
  public static Status JOB_RUNNING = new Status(StatusCode.JOB_RUNNING.getStatusCode());
  public static Status JOB_FAILING = new Status(StatusCode.JOB_FAILING.getStatusCode());
  public static Status JOB_FAILED = new Status(StatusCode.JOB_FAILED.getStatusCode());
  public static Status JOB_CLOSING = new Status(StatusCode.JOB_CLOSING.getStatusCode());
  public static Status JOB_CLOSED = new Status(StatusCode.JOB_CLOSED.getStatusCode());

  static {
    WRONG_USERNAME_OR_PASSWORD.setMessage("wrong username or password");
    ACCESS_DENY.setMessage("access deny");
    PARTIAL_SUCCESS.setMessage("partial success");
    FAILURE.setMessage("unexpected error");
    SERVICE_UNAVAILABLE.setMessage("service unavailable for the sake of high workload");
  }

  public static Status status(StatusCode code, String msg) {
    Status status = new Status(code.getStatusCode());
    status.setMessage(msg);
    return status;
  }
}
