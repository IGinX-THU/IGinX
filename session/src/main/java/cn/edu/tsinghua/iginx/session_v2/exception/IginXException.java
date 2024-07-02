package cn.edu.tsinghua.iginx.session_v2.exception;

public class IginXException extends RuntimeException {

  private final String message;

  public IginXException(String message) {
    this.message = message;
  }

  public IginXException(Throwable cause) {
    super(cause);
    this.message = cause.getMessage();
  }

  public IginXException(String message, Throwable cause) {
    super(cause);
    this.message = message + cause.getMessage();
  }

  @Override
  public String getMessage() {
    return message;
  }
}
