package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class Intersect extends AbstractBinaryOperator {

  private final boolean isDistinct;

  public Intersect(Source sourceA, Source sourceB, boolean isDistinct) {
    super(OperatorType.Intersect, sourceA, sourceB);
    this.isDistinct = isDistinct;
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  @Override
  public Operator copy() {
    return new Intersect(getSourceA().copy(), getSourceB().copy(), isDistinct);
  }

  @Override
  public String getInfo() {
    return "isDistinct: " + isDistinct;
  }
}
