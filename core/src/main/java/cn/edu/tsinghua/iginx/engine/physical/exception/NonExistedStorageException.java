package cn.edu.tsinghua.iginx.engine.physical.exception;

public class NonExistedStorageException extends PhysicalException {

  private static final long serialVersionUID = 2361886892149089975L;

  public NonExistedStorageException(long id) {
    super("non existed storage " + id);
  }
}
