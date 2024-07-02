package cn.edu.tsinghua.iginx.engine.physical.exception;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public class NotSupportedOperatorException extends PhysicalException {

  private static final long serialVersionUID = 2361886892149089975L;

  public NotSupportedOperatorException(Operator operator, String detail) {
    super("non supported operator " + operator.getType() + ", because " + detail);
  }
}
