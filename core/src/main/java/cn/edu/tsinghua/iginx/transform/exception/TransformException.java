package cn.edu.tsinghua.iginx.transform.exception;

import cn.edu.tsinghua.iginx.exception.IginxException;

public class TransformException extends IginxException {

  private static final long serialVersionUID = -222780953901154063L;

  public TransformException() {
    super();
  }

  public TransformException(String message) {
    super(message);
  }

  public TransformException(String message, Throwable cause) {
    super(message, cause);
  }

  public TransformException(Throwable cause) {
    super(cause);
  }

  protected TransformException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
