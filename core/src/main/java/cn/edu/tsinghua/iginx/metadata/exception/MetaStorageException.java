package cn.edu.tsinghua.iginx.metadata.exception;

public class MetaStorageException extends Exception {

  private static final long serialVersionUID = -8128973325398925370L;

  public MetaStorageException(String message) {
    super(message);
  }

  public MetaStorageException(String message, Throwable cause) {
    super(message, cause);
  }

  public MetaStorageException(Throwable cause) {
    super(cause);
  }
}
