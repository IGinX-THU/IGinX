package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class ValueToSelectedPath extends AbstractUnaryOperator {

  private final String prefix;

  public ValueToSelectedPath(Source source, String prefix) {
    super(OperatorType.ValueToSelectedPath, source);
    this.prefix = prefix;
  }

  public String getPrefix() {
    return prefix;
  }

  @Override
  public Operator copy() {
    return new ValueToSelectedPath(getSource().copy(), prefix);
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    if (prefix != null && !prefix.isEmpty()) {
      builder.append("prefix: ");
      builder.append(prefix);
    }
    return builder.toString();
  }
}
