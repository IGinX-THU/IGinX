package cn.edu.tsinghua.iginx.parquet.util.exception;

public class StorageClosedException extends StorageException {
  public StorageClosedException(String message) {
    super(message);
  }
}
