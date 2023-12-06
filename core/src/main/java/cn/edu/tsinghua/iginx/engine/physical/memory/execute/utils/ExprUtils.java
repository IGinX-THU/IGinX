package cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.expr.BaseExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.BinaryExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.BracketExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.ConstantExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.expr.FuncExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.Operator;
import cn.edu.tsinghua.iginx.engine.shared.expr.UnaryExpression;
import cn.edu.tsinghua.iginx.engine.shared.function.Function;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.RowMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;

public class ExprUtils {

  private static final FunctionManager functionManager = FunctionManager.getInstance();

  public static Value calculateExpr(Row row, Expression expr) {
    switch (expr.getType()) {
      case Constant:
        return calculateConstantExpr((ConstantExpression) expr);
      case Base:
        return calculateBaseExpr(row, (BaseExpression) expr);
      case Function:
        return calculateFuncExpr(row, (FuncExpression) expr);
      case Bracket:
        return calculateBracketExpr(row, (BracketExpression) expr);
      case Unary:
        return calculateUnaryExpr(row, (UnaryExpression) expr);
      case Binary:
        return calculateBinaryExpr(row, (BinaryExpression) expr);
      default:
        throw new IllegalArgumentException(String.format("Unknown expr type: %s", expr.getType()));
    }
  }

  private static Value calculateConstantExpr(ConstantExpression constantExpr) {
    return new Value(constantExpr.getValue());
  }

  private static Value calculateBaseExpr(Row row, BaseExpression baseExpr) {
    String colName = baseExpr.getColumnName();
    int index = row.getHeader().indexOf(colName);
    if (index == -1) {
      return null;
    }
    return new Value(row.getValues()[index]);
  }

  private static Value calculateFuncExpr(Row row, FuncExpression funcExpr) {
    String colName = funcExpr.getColumnName();
    int index = row.getHeader().indexOf(colName);
    if (index == -1) {
      return calculateFuncExprNative(row, funcExpr);
    }
    return new Value(row.getValues()[index]);
  }

  private static Value calculateFuncExprNative(Row row, FuncExpression funcExpr) {
    Function function = functionManager.getFunction(funcExpr.getFuncName());
    if (!function.getMappingType().equals(MappingType.RowMapping)) {
      throw new RuntimeException("only row mapping function can be used in expr");
    }
    RowMappingFunction rowMappingFunction = (RowMappingFunction) function;
    FunctionParams params =
        new FunctionParams(
            funcExpr.getColumns(), funcExpr.getArgs(), funcExpr.getKvargs(), funcExpr.isDistinct());
    try {
      Row ret = rowMappingFunction.transform(row, params);
      int retValueSize = ret.getValues().length;
      if (retValueSize != 1) {
        throw new RuntimeException("the func in the expr can only have one return value");
      }
      return ret.getAsValue(0);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Value calculateBracketExpr(Row row, BracketExpression bracketExpr) {
    Expression expr = bracketExpr.getExpression();
    return calculateExpr(row, expr);
  }

  private static Value calculateUnaryExpr(Row row, UnaryExpression unaryExpr) {
    Expression expr = unaryExpr.getExpression();
    Operator operator = unaryExpr.getOperator();

    Value value = calculateExpr(row, expr);
    if (operator.equals(Operator.PLUS)) { // positive
      return value;
    }

    switch (value.getDataType()) { // negative
      case INTEGER:
        return new Value(-value.getIntV());
      case LONG:
        return new Value(-value.getLongV());
      case FLOAT:
        return new Value(-value.getFloatV());
      case DOUBLE:
        return new Value(-value.getDoubleV());
      default:
        return null;
    }
  }

  private static Value calculateBinaryExpr(Row row, BinaryExpression binaryExpr) {
    Expression leftExpr = binaryExpr.getLeftExpression();
    Expression rightExpr = binaryExpr.getRightExpression();
    Operator operator = binaryExpr.getOp();

    Value leftVal = calculateExpr(row, leftExpr);
    Value rightVal = calculateExpr(row, rightExpr);

    if (!leftVal.getDataType().equals(rightVal.getDataType())) { // 两值类型不同，但均为数值类型，转为double再运算
      if (DataTypeUtils.isNumber(leftVal.getDataType())
          && DataTypeUtils.isNumber(rightVal.getDataType())) {
        leftVal = ValueUtils.transformToDouble(leftVal);
        rightVal = ValueUtils.transformToDouble(rightVal);
      } else {
        return null;
      }
    }

    switch (operator) {
      case PLUS:
        return calculatePlus(leftVal, rightVal);
      case MINUS:
        return calculateMinus(leftVal, rightVal);
      case STAR:
        return calculateStar(leftVal, rightVal);
      case DIV:
        return calculateDiv(leftVal, rightVal);
      case MOD:
        return calculateMod(leftVal, rightVal);
      default:
        throw new IllegalArgumentException(String.format("Unknown operator type: %s", operator));
    }
  }

  private static Value calculatePlus(Value left, Value right) {
    switch (left.getDataType()) {
      case INTEGER:
        return new Value(left.getIntV() + right.getIntV());
      case LONG:
        return new Value(left.getLongV() + right.getLongV());
      case FLOAT:
        return new Value(left.getFloatV() + right.getFloatV());
      case DOUBLE:
        return new Value(left.getDoubleV() + right.getDoubleV());
      default:
        return null;
    }
  }

  private static Value calculateMinus(Value left, Value right) {
    switch (left.getDataType()) {
      case INTEGER:
        return new Value(left.getIntV() - right.getIntV());
      case LONG:
        return new Value(left.getLongV() - right.getLongV());
      case FLOAT:
        return new Value(left.getFloatV() - right.getFloatV());
      case DOUBLE:
        return new Value(left.getDoubleV() - right.getDoubleV());
      default:
        return null;
    }
  }

  private static Value calculateStar(Value left, Value right) {
    switch (left.getDataType()) {
      case INTEGER:
        return new Value(left.getIntV() * right.getIntV());
      case LONG:
        return new Value(left.getLongV() * right.getLongV());
      case FLOAT:
        return new Value(left.getFloatV() * right.getFloatV());
      case DOUBLE:
        return new Value(left.getDoubleV() * right.getDoubleV());
      default:
        return null;
    }
  }

  private static Value calculateDiv(Value left, Value right) {
    switch (left.getDataType()) {
      case INTEGER:
        return new Value(left.getIntV() / right.getIntV());
      case LONG:
        return new Value(left.getLongV() / right.getLongV());
      case FLOAT:
        return new Value(left.getFloatV() / right.getFloatV());
      case DOUBLE:
        return new Value(left.getDoubleV() / right.getDoubleV());
      default:
        return null;
    }
  }

  private static Value calculateMod(Value left, Value right) {
    switch (left.getDataType()) {
      case INTEGER:
        return new Value(left.getIntV() % right.getIntV());
      case LONG:
        return new Value(left.getLongV() % right.getLongV());
      case FLOAT:
        return new Value(left.getFloatV() % right.getFloatV());
      case DOUBLE:
        return new Value(left.getDoubleV() % right.getDoubleV());
      default:
        return null;
    }
  }
}
