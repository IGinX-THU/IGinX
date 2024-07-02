package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class Join extends AbstractBinaryOperator {

  private final String joinBy;

  public Join(Source sourceA, Source sourceB) {
    this(sourceA, sourceB, Constants.KEY);
  }

  public Join(Source sourceA, Source sourceB, String joinBy) {
    super(OperatorType.Join, sourceA, sourceB);
    if (joinBy == null) {
      throw new IllegalArgumentException("joinBy shouldn't be null");
    }
    this.joinBy = joinBy;
  }

  public String getJoinBy() {
    return joinBy;
  }

  @Override
  public Operator copy() {
    return new Join(getSourceA().copy(), getSourceB().copy(), joinBy);
  }

  @Override
  public BinaryOperator copyWithSource(Source sourceA, Source sourceB) {
    return new Join(sourceA, sourceB, joinBy);
  }

  @Override
  public String getInfo() {
    return "JoinBy: " + joinBy;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    Join join = (Join) object;
    return joinBy.equals(join.joinBy);
  }
}
