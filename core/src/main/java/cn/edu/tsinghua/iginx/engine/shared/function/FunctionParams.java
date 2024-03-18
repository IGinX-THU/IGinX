package cn.edu.tsinghua.iginx.engine.shared.function;

import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.expr.FuncExpression;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FunctionParams {

  private final List<Pair<Integer, Object>> posArgs;

  private final Map<String, Object> kwargs;

  private final Expression expr;

  private final boolean isDistinct;

  public FunctionParams(List<String> paths) {
    this(FuncExpression.getArgListFromColumns(paths), null, null, false);
  }

  public FunctionParams(Expression expr) {
    this(null, null, expr, false);
  }

  public FunctionParams(List<Pair<Integer, Object>> posArgs, Map<String, Object> kwargs) {
    this(posArgs, kwargs, null, false);
  }

  public FunctionParams(
      List<Pair<Integer, Object>> posArgs, Map<String, Object> kwargs, boolean isDistinct) {
    this(posArgs, kwargs, null, isDistinct);
  }

  public FunctionParams(
      List<Pair<Integer, Object>> posArgs,
      Map<String, Object> kwargs,
      Expression expr,
      boolean isDistinct) {
    this.posArgs = posArgs;
    this.kwargs = kwargs;
    this.expr = expr;
    this.isDistinct = isDistinct;
  }

  public List<String> getPaths() {
    List<String> list = new ArrayList<>();
    for (Pair<Integer, Object> p : posArgs) {
      if (p.k == FuncExpression.COLUMN_ARG_TYPE) {
        list.add(p.v.toString());
      }
    }
    return list;
  }

  /**
   * 获取位置参数中所有常量参数
   *
   * @return 一个参数列表
   */
  public List<Object> getConPosArgs() {
    List<Object> list = new ArrayList<>();
    for (Pair<Integer, Object> p : posArgs) {
      if (p.k == FuncExpression.POS_ARG_TYPE) {
        list.add(p.v);
      }
    }
    return list;
  }

  /**
   * 获取所有位置参数，包括列名和常量
   *
   * @return 一个参数列表，Pair.k指示参数类型（列名或常量），Pair.v指示值（列名或常量的值）
   */
  public List<Pair<Integer, Object>> getPosArgs() {
    return posArgs;
  }

  public Map<String, Object> getKwargs() {
    return kwargs;
  }

  public Expression getExpr() {
    return expr;
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  protected FunctionParams copy() {
    List<Pair<Integer, Object>> newPosArgs = null;
    if (posArgs != null && !posArgs.isEmpty()) {
      newPosArgs = new ArrayList<>(posArgs);
    }
    Map<String, Object> newKvargs = null;
    if (kwargs != null && !kwargs.isEmpty()) {
      newKvargs = new HashMap<>(kwargs);
    }
    return new FunctionParams(newPosArgs, newKvargs, expr, isDistinct);
  }
}
