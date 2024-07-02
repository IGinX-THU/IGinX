package cn.edu.tsinghua.iginx.engine.shared.source;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public class OperatorSource extends AbstractSource {

  private final Operator operator;

  public OperatorSource(Operator operator) {
    super(SourceType.Operator);
    if (operator == null) {
      throw new IllegalArgumentException("operator shouldn't be null");
    }
    this.operator = operator;
  }

  public Operator getOperator() {
    return operator;
  }

  @Override
  public Source copy() {
    return new OperatorSource(operator.copy());
  }
}
