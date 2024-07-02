package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.visitor.OperatorVisitor;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;

public abstract class AbstractUnaryOperator extends AbstractOperator implements UnaryOperator {

  private Source source;

  public AbstractUnaryOperator(OperatorType type, Source source) {
    super(type);
    if (source == null) {
      throw new IllegalArgumentException("source shouldn't be null");
    }
    this.source = source;
  }

  public AbstractUnaryOperator(Source source) {
    this(OperatorType.Unary, source);
  }

  @Override
  public void accept(OperatorVisitor visitor) {
    visitor.enter();
    visitor.visit(this);

    if (visitor.needStop()) {
      return;
    }

    Source source = this.getSource();
    if (source.getType() == SourceType.Operator) {
      ((OperatorSource) source).getOperator().accept(visitor);
    }
    visitor.leave();
  }

  @Override
  public Source getSource() {
    return source;
  }

  @Override
  public void setSource(Source source) {
    this.source = source;
  }
}
