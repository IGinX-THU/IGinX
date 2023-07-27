package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class Union extends AbstractBinaryOperator {

  private final boolean isDistinct;

  public Union(Source sourceA, Source sourceB, boolean isDistinct) {
    super(OperatorType.Union, sourceA, sourceB);
    this.isDistinct = isDistinct;
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  @Override
  public Operator copy() {
    return new Union(getSourceA().copy(), getSourceB().copy(), isDistinct);
  }

  @Override
  public String getInfo() {
    return "isDistinct: " + isDistinct;
  }
}
