package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class Distinct extends AbstractUnaryOperator {

  private final List<String> patterns;

  public Distinct(Source source, List<String> patterns) {
    super(OperatorType.Distinct, source);
    this.patterns = patterns;
  }

  public List<String> getPatterns() {
    return patterns;
  }

  @Override
  public Operator copy() {
    return new Distinct(getSource().copy(), new ArrayList<>(patterns));
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new Distinct(source, new ArrayList<>(patterns));
  }

  @Override
  public String getInfo() {
    return "Patterns: " + String.join(",", patterns);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    Distinct that = (Distinct) object;
    return patterns.equals(that.patterns);
  }
}
