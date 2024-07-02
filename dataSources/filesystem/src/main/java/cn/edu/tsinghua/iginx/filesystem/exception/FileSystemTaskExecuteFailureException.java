package cn.edu.tsinghua.iginx.filesystem.exception;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;

public class FileSystemTaskExecuteFailureException extends PhysicalTaskExecuteFailureException {

  public FileSystemTaskExecuteFailureException(String message) {
    super(message);
  }

  public FileSystemTaskExecuteFailureException(String message, Throwable cause) {
    super(message, cause);
  }
}
