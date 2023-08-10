package cn.edu.tsinghua.iginx.engine.shared.function;

import cn.edu.tsinghua.iginx.sql.expression.Expression;
import java.util.ArrayList;
import java.util.List;

public class FunctionParams {

  private final List<String> paths;

  private final List<Integer> levels;

  private final Expression expr;

  private final boolean isDistinct;

  public FunctionParams(List<String> paths) {
    this(paths, null, null, false);
  }

  public FunctionParams(Expression expr) {
    this(null, null, expr, false);
  }

  public FunctionParams(List<String> paths, List<Integer> levels) {
    this(paths, levels, null, false);
  }

  public FunctionParams(List<String> paths, List<Integer> levels, boolean isDistinct) {
    this(paths, levels, null, isDistinct);
  }

  public FunctionParams(
      List<String> paths, List<Integer> levels, Expression expr, boolean isDistinct) {
    this.paths = paths;
    this.levels = levels;
    this.expr = expr;
    this.isDistinct = isDistinct;
  }

  public List<String> getPaths() {
    return paths;
  }

  public List<Integer> getLevels() {
    return levels;
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
    List<Integer> newLevels = null;
    if (levels != null && !levels.isEmpty()) {
      newLevels = new ArrayList<>(levels);
    }
    return new FunctionParams(newPaths, newLevels, expr, isDistinct);
  }
}
