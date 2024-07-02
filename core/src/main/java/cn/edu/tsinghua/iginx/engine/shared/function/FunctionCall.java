package cn.edu.tsinghua.iginx.engine.shared.function;

import cn.edu.tsinghua.iginx.engine.shared.function.system.ArithmeticExpr;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.UDAF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.UDSF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.UDTF;

public class FunctionCall {

  private final Function function;

  private final FunctionParams params;

  public FunctionCall(Function function, FunctionParams params) {
    this.function = function;
    this.params = params;
  }

  public Function getFunction() {
    return function;
  }

  public FunctionParams getParams() {
    return params;
  }

  public FunctionCall copy() {
    return new FunctionCall(function, params.copy());
  }

  @Override
  public String toString() {
    return String.format(
        "{Name: %s, FuncType: %s, MappingType: %s}",
        function.getIdentifier(), function.getFunctionType(), function.getMappingType());
  }

  private String getFuncName() {
    if (function instanceof ArithmeticExpr) {
      return params.getExpr().getColumnName();
    } else if (function.getFunctionType() == FunctionType.UDF) {
      if (function instanceof UDAF) {
        return ((UDAF) function).getFunctionName();
      } else if (function instanceof UDTF) {
        return ((UDTF) function).getFunctionName();
      } else if (function instanceof UDSF) {
        return ((UDSF) function).getFunctionName();
      }
    } else {
      return function.getIdentifier();
    }
    return null;
  }

  public String getFunctionStr() {
    if (function instanceof ArithmeticExpr) {
      return params.getExpr().getColumnName();
    }

    return String.format(
        "%s(%s%s)",
        getFuncName(),
        params.isDistinct() ? "distinct " : "",
        String.join(", ", params.getPaths()));
  }

  public String getNameAndFuncTypeStr() {
    return String.format(
        "(%s, %s)", FunctionUtils.getFunctionName(function), function.getFunctionType());
  }
}
