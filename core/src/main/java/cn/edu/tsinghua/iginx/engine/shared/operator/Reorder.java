package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class Reorder extends AbstractUnaryOperator {

  private final List<String> patterns;

  private boolean needSelectedPath;

  public Reorder(Source source, List<String> patterns) {
    this(source, patterns, false);
  }

  public Reorder(Source source, List<String> patterns, boolean needSelectedPath) {
    super(OperatorType.Reorder, source);
    if (patterns == null) {
      throw new IllegalArgumentException("patterns shouldn't be null");
    }
    this.patterns = patterns;
    this.needSelectedPath = needSelectedPath;
  }

  @Override
  public Operator copy() {
    return new Reorder(getSource().copy(), new ArrayList<>(patterns));
  }

  public List<String> getPatterns() {
    return patterns;
  }

  public boolean isNeedSelectedPath() {
    return needSelectedPath;
  }

  public void setNeedSelectedPath(boolean needSelectedPath) {
    this.needSelectedPath = needSelectedPath;
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    builder.append("Order: ");
    for (String pattern : patterns) {
      builder.append(pattern).append(",");
    }
    builder.deleteCharAt(builder.length() - 1);
    return builder.toString();
  }
}
