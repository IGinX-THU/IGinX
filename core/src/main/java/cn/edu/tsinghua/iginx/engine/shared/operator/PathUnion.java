package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class PathUnion extends AbstractBinaryOperator {

  public PathUnion(Source sourceA, Source sourceB) {
    super(OperatorType.PathUnion, sourceA, sourceB);
  }

  @Override
  public Operator copy() {
    return new PathUnion(getSourceA().copy(), getSourceB().copy());
  }

  @Override
  public BinaryOperator copyWithSource(Source sourceA, Source sourceB) {
    return new PathUnion(sourceA, sourceB);
  }

  @Override
  public String getInfo() {
    return "";
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    return object != null && getClass() == object.getClass();
  }
}
