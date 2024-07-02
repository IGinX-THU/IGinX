package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class Reorder extends AbstractUnaryOperator {

  private final List<String> patterns;

  private final List<Boolean> isPyUDF;

  private boolean needSelectedPath;

  public Reorder(Source source, List<String> patterns) {
    this(source, patterns, new ArrayList<>(patterns.size()), false);
    patterns.forEach(p -> isPyUDF.add(false));
  }

  public Reorder(
      Source source, List<String> patterns, List<Boolean> isPyUDF, boolean needSelectedPath) {
    super(OperatorType.Reorder, source);
    if (patterns == null) {
      throw new IllegalArgumentException("patterns shouldn't be null");
    }
    this.patterns = patterns;
    this.isPyUDF = isPyUDF;
    this.needSelectedPath = needSelectedPath;
  }

  @Override
  public Operator copy() {
    return new Reorder(
        getSource().copy(), new ArrayList<>(patterns), new ArrayList<>(isPyUDF), needSelectedPath);
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new Reorder(
        source, new ArrayList<>(patterns), new ArrayList<>(isPyUDF), needSelectedPath);
  }

  public List<String> getPatterns() {
    return patterns;
  }

  public List<Boolean> getIsPyUDF() {
    return isPyUDF;
  }

  public void setPatterns(List<String> patterns) {
    this.patterns.clear();
    this.patterns.addAll(patterns);
    this.isPyUDF.clear();
    patterns.forEach(p -> isPyUDF.add(isUdfPath(p)));
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

  /**
   * 从路径判断是否是用户自定义函数
   *
   * @param path 路径
   * @return 是否是用户自定义函数
   */
  private static boolean isUdfPath(String path) {
    // 从路径中判断是否是用户自定义函数
    if (path.contains("(") && path.contains(")")) {
      // 获取函数名
      String funcName = path.substring(0, path.indexOf("("));
      // 判断是否是用户自定义函数
      try {
        return FunctionUtils.isPyUDF(funcName);
      } catch (IllegalArgumentException e) {
        // 如果出现异常，是因为该函数不是系统函数且没在UDF中注册，说明不是用户自定义函数，可能是表达中的括号
        return false;
      }
    }
    return false;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    Reorder reorder = (Reorder) object;
    return patterns.equals(reorder.patterns)
        && isPyUDF.equals(reorder.isPyUDF)
        && needSelectedPath == reorder.needSelectedPath;
  }
}
