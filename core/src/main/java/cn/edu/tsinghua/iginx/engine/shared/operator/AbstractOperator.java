package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;

public abstract class AbstractOperator implements Operator {

  private final OperatorType type;

  public AbstractOperator() {
    this.type = OperatorType.Unknown;
  }

  public AbstractOperator(OperatorType type) {
    if (type == null) {
      throw new IllegalArgumentException("operator type shouldn't be null");
    }
    this.type = type;
  }

  @Override
  public OperatorType getType() {
    return type;
  }
}
