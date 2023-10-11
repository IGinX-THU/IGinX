package cn.edu.tsinghua.iginx.engine.shared.function;

import cn.edu.tsinghua.iginx.sql.expression.Expression;
import java.util.ArrayList;
import java.util.List;

public class FunctionParams {

  private final List<String> paths;

  private final Expression expr;

  private final boolean isDistinct;

  public FunctionParams(List<String> paths) {
    this(paths, null, false);
  }

  public FunctionParams(List<String> paths, boolean isDistinct) {
    this(paths, null, isDistinct);
  }

  public FunctionParams(Expression expr) {
    this(null, expr, false);
  }

  public FunctionParams(List<String> paths, Expression expr, boolean isDistinct) {
    this.paths = paths;
    this.expr = expr;
    this.isDistinct = isDistinct;
  }

  public List<String> getPaths() {
    return paths;
  }

  public Expression getExpr() {
    return expr;
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  protected FunctionParams copy() {
    List<String> newPaths = null;
    if (paths != null && !paths.isEmpty()) {
      newPaths = new ArrayList<>(paths);
    }
    return new FunctionParams(newPaths, expr, isDistinct);
  }
}
