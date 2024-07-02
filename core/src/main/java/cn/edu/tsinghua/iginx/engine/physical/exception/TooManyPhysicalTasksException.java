package cn.edu.tsinghua.iginx.engine.physical.exception;

public class TooManyPhysicalTasksException extends PhysicalException {

  public TooManyPhysicalTasksException(long storageId) {
    super("too many physical tasks need to do for storage: " + storageId);
  }
}
