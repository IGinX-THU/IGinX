package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class Except extends AbstractBinaryOperator {

  private final boolean isDistinct;

  public Except(Source sourceA, Source sourceB, boolean isDistinct) {
    super(OperatorType.Except, sourceA, sourceB);
    this.isDistinct = isDistinct;
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  @Override
  public Operator copy() {
    return new Except(getSourceA().copy(), getSourceB().copy(), isDistinct);
  }

  @Override
  public String getInfo() {
    return "isDistinct: " + isDistinct;
  }
}
