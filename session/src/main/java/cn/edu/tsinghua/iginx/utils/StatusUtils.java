package cn.edu.tsinghua.iginx.utils;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.exception.StatusCode;
import cn.edu.tsinghua.iginx.thrift.Status;

public class StatusUtils {

  public static void verifySuccess(Status status) throws SessionException {
    if (status.code != StatusCode.SUCCESS_STATUS.getStatusCode()
        && status.code != StatusCode.PARTIAL_SUCCESS.getStatusCode()) {
      throw new SessionException(status);
    }
  }

  public static boolean verifyNoRedirect(Status status) {
    return status.code != StatusCode.REDIRECT.getStatusCode();
  }
}
