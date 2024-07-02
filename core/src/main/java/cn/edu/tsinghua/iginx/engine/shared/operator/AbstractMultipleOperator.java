package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.visitor.OperatorVisitor;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import java.util.List;

public abstract class AbstractMultipleOperator extends AbstractOperator
    implements MultipleOperator {

  private List<Source> sources;

  public AbstractMultipleOperator(OperatorType type, List<Source> sources) {
    super(type);
    if (sources == null || sources.isEmpty()) {
      throw new IllegalArgumentException("sourceList shouldn't be null or empty");
    }
    sources.forEach(
        source -> {
          if (source == null) {
            throw new IllegalArgumentException("source shouldn't be null");
          }
        });
    this.sources = sources;
  }

  public AbstractMultipleOperator(List<Source> sources) {
    this(OperatorType.Multiple, sources);
  }

  @Override
  public void accept(OperatorVisitor visitor) {
    visitor.enter();
    visitor.visit(this);

    if (visitor.needStop()) {
      return;
    }

    for (Source source : this.getSources()) {
      if (source.getType() == SourceType.Operator) {
        ((OperatorSource) source).getOperator().accept(visitor);
      }
    }
    visitor.leave();
  }

  @Override
  public List<Source> getSources() {
    return sources;
  }

  @Override
  public void setSources(List<Source> sources) {
    this.sources = sources;
  }
}
