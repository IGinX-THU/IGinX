package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class Load extends AbstractUnaryOperator {

  private final Operator operator;

  private final int index;

  public Load(Source source, int index, Operator operator) {
    super(OperatorType.Load, source);
    this.index = index;
    this.operator = operator;
  }

  public Operator getOperator() {
    return operator;
  }

  @Override
  public Operator copy() {
    return new Load(getSource().copy(), index, operator.copy());
  }

  @Override
  public String getInfo() {
    return "Load" + index + ": " + getSource().toString();
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new Load(source, index, operator.copy());
  }
}
