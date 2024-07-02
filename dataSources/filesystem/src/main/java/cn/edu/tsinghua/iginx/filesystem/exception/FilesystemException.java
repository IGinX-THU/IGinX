package cn.edu.tsinghua.iginx.filesystem.exception;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;

public class FilesystemException extends PhysicalException {

  public FilesystemException(String message) {
    super(message);
  }

  public FilesystemException(String message, Throwable cause) {
    super(message, cause);
  }

  public FilesystemException(Throwable cause) {
    super(cause);
  }
}
