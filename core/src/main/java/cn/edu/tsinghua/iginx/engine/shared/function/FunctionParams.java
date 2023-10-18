package cn.edu.tsinghua.iginx.engine.shared.function;

import cn.edu.tsinghua.iginx.sql.expression.Expression;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FunctionParams {

  private final List<String> paths;

  private final List<Object> args;

  private final Map<String, Object> kwargs;

  private final List<Integer> levels;

  private final Expression expr;

  private final boolean isDistinct;

  public FunctionParams(List<String> paths) {
    this(paths, null, null, null, null, false);
  }

  public FunctionParams(List<String> paths, List<Object> args, Map<String, Object> kwargs) {
    this(paths, args, kwargs, null, null, false);
  }

  public FunctionParams(
      List<String> paths, List<Object> args, Map<String, Object> kwargs, boolean isDistinct) {
    this(paths, args, kwargs, null, null, isDistinct);
  }

  public FunctionParams(Expression expr) {
    this(null, null, null, null, expr, false);
  }

  public FunctionParams(
      List<String> paths, List<Object> args, Map<String, Object> kwargs, List<Integer> levels) {
    this(paths, args, kwargs, levels, null, false);
  }

  public FunctionParams(
      List<String> paths,
      List<Object> args,
      Map<String, Object> kwargs,
      List<Integer> levels,
      boolean isDistinct) {
    this(paths, args, kwargs, levels, null, isDistinct);
  }

  public FunctionParams(
      List<String> paths,
      List<Object> args,
      Map<String, Object> kwargs,
      List<Integer> levels,
      Expression expr,
      boolean isDistinct) {
    this.paths = paths;
    this.args = args;
    this.kwargs = kwargs;
    this.levels = levels;
    this.expr = expr;
    this.isDistinct = isDistinct;
  }

  public List<String> getPaths() {
    return paths;
  }

  public List<Object> getArgs() {
    return args;
  }

  public Map<String, Object> getKwargs() {
    return kwargs;
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
    Map<String, Object> newKvargs = null;
    if (kwargs != null && !kwargs.isEmpty()) {
      newKvargs = new HashMap<>(kwargs);
    }
    List<Object> newArgs = null;
    if (args != null && !args.isEmpty()) {
      newArgs = new ArrayList<>(args);
    }
    return new FunctionParams(newPaths, newArgs, newKvargs, newLevels, expr, isDistinct);
  }
}
