package cn.edu.tsinghua.iginx.resource.exception;

import cn.edu.tsinghua.iginx.exception.IginxException;
import cn.edu.tsinghua.iginx.thrift.Status;

public class ResourceException extends IginxException {

  private final Status status;

  public ResourceException(Status status) {
    super(status.message, status.code);
    this.status = status;
  }

  public Status getStatus() {
    return status;
  }
}
