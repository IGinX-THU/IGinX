package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Rename extends AbstractUnaryOperator {

  private final Map<String, String> aliasMap;

  private final List<String> ignorePatterns; // 不进行重命名的列

  public Rename(Source source, Map<String, String> aliasMap) {
    this(source, aliasMap, new ArrayList<>());
  }

  public Rename(Source source, Map<String, String> aliasMap, List<String> ignorePatterns) {
    super(OperatorType.Rename, source);
    if (aliasMap == null) {
      throw new IllegalArgumentException("aliasMap shouldn't be null");
    }
    this.aliasMap = aliasMap;
    this.ignorePatterns = ignorePatterns;
  }

  public Map<String, String> getAliasMap() {
    return aliasMap;
  }

  public List<String> getIgnorePatterns() {
    return ignorePatterns;
  }

  @Override
  public Operator copy() {
    return new Rename(getSource().copy(), new HashMap<>(aliasMap));
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new Rename(source, new HashMap<>(aliasMap));
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    builder.append("AliasMap: ");
    aliasMap.forEach(
        (k, v) -> builder.append("(").append(k).append(", ").append(v).append(")").append(","));
    builder.deleteCharAt(builder.length() - 1);
    if (!ignorePatterns.isEmpty()) {
      builder.append(", IgnorePatterns: ");
      builder.append(ignorePatterns);
    }
    return builder.toString();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    Rename rename = (Rename) object;
    return aliasMap.equals(rename.aliasMap) && ignorePatterns.equals(rename.ignorePatterns);
  }
}
