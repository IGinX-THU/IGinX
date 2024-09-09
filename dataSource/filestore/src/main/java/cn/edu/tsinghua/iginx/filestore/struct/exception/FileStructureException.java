package cn.edu.tsinghua.iginx.filestore.struct.exception;

import java.io.IOException;

public class FileStructureException extends IOException {

  public FileStructureException() {}

  public FileStructureException(String message) {
    super(message);
  }

  public FileStructureException(String message, Throwable cause) {
    super(message, cause);
  }

  public FileStructureException(Throwable cause) {
    super(cause);
  }
}
