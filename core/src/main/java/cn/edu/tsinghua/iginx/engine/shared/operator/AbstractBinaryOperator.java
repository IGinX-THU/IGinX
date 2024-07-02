package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.visitor.OperatorVisitor;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;

public abstract class AbstractBinaryOperator extends AbstractOperator implements BinaryOperator {

  private Source sourceA;

  private Source sourceB;

  public AbstractBinaryOperator(OperatorType type, Source sourceA, Source sourceB) {
    super(type);
    if (sourceA == null || sourceB == null) {
      throw new IllegalArgumentException("source shouldn't be null");
    }
    this.sourceA = sourceA;
    this.sourceB = sourceB;
  }

  public AbstractBinaryOperator(Source sourceA, Source sourceB) {
    this(OperatorType.Binary, sourceA, sourceB);
  }

  @Override
  public void accept(OperatorVisitor visitor) {
    visitor.enter();
    visitor.visit(this);

    if (visitor.needStop()) {
      return;
    }

    Source sourceA = this.getSourceA();
    if (sourceA.getType() == SourceType.Operator) {
      ((OperatorSource) sourceA).getOperator().accept(visitor);
    }
    Source sourceB = this.getSourceB();
    if (sourceB.getType() == SourceType.Operator) {
      ((OperatorSource) sourceB).getOperator().accept(visitor);
    }
    visitor.leave();
  }

  @Override
  public Source getSourceA() {
    return sourceA;
  }

  @Override
  public Source getSourceB() {
    return sourceB;
  }

  @Override
  public void setSourceA(Source source) {
    this.sourceA = source;
  }

  @Override
  public void setSourceB(Source source) {
    this.sourceB = source;
  }
}
