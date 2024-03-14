package cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.function.Function;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.RowMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import java.util.ArrayList;
import java.util.List;

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
      case Multiple:
        return calculateMultipleExpr(row, (MultipleExpression) expr);
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


  public static Value calculateMultipleExpr(Row row, MultipleExpression multipleExpr){
    List<Expression> children = multipleExpr.getChildren();
    List<Operator> ops = multipleExpr.getOps();
    List<Value> values = new ArrayList<>();
    for (Expression expr : children) {
      values.add(calculateExpr(row, expr));
    }

    if(ops.get(0) == Operator.MINUS){
      values.set(0, calculateUnaryExpr(row, new UnaryExpression(Operator.MINUS, children.get(0))));
      ops.set(0, Operator.PLUS);
    }

    for (int i = 1; i < ops.size(); i++) {
      Operator op = ops.get(i);
      Value left = values.get(i-1);
      Value right = values.get(i);
      if (!left.getDataType().equals(right.getDataType())) {
        if (DataTypeUtils.isNumber(left.getDataType()) && DataTypeUtils.isNumber(right.getDataType())) {
          left = ValueUtils.transformToDouble(left);
          right = ValueUtils.transformToDouble(right);
        } else {
          return null;
        }
      }
      switch (op) {
        case PLUS:
          values.set(i, calculatePlus(left, right));
          break;
        case MINUS:
          values.set(i, calculateMinus(left, right));
          break;
        case STAR:
          values.set(i, calculateStar(left, right));
          break;
        case DIV:
          values.set(i, calculateDiv(left, right));
          break;
        case MOD:
          values.set(i, calculateMod(left, right));
          break;
        default:
          throw new IllegalArgumentException(String.format("Unknown operator type: %s", op));
      }
    }
    return values.get(values.size() - 1);
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

  public static List<String> getPathFromExpr(Expression expr) {
    switch (expr.getType()) {
      case Constant:
        return new ArrayList<>();
      case Base:
        List<String> ret = new ArrayList<>();
        ret.add(expr.getColumnName());
        return ret;
      case Function:
        return ((FuncExpression) expr).getColumns();
      case Bracket:
        return getPathFromExpr(((BracketExpression) expr).getExpression());
      case Unary:
        return getPathFromExpr(((UnaryExpression) expr).getExpression());
      case Binary:
        List<String> left = getPathFromExpr(((BinaryExpression) expr).getLeftExpression());
        List<String> right = getPathFromExpr(((BinaryExpression) expr).getRightExpression());
        left.addAll(right);
        return left;
      default:
        throw new IllegalArgumentException(String.format("Unknown expr type: %s", expr.getType()));
    }
  }

  /**
   * 把表达式的二叉树型转换为多叉树型
   * @param expr 表达式
   * @return 多叉树型表达式
   */
  public static Expression flattenExpression(Expression expr){
    switch(expr.getType()) {
      case Constant:
      case Base:
      case Function:
        return expr;
      case Bracket:
        BracketExpression bracketExpression = (BracketExpression) expr;
        bracketExpression.setExpression(flattenExpression(bracketExpression.getExpression()));
        return bracketExpression;
      case Unary:
        UnaryExpression unaryExpression = (UnaryExpression) expr;
        unaryExpression.setExpression(flattenExpression(unaryExpression.getExpression()));
        return unaryExpression;
      case Binary:
        BinaryExpression binaryExpression = (BinaryExpression) expr;
        // 如果是模运算，与其他运算不兼容，不拍平
        if(binaryExpression.getOp() == Operator.MOD){
          binaryExpression.setLeftExpression(flattenExpression(binaryExpression.getLeftExpression()));
          binaryExpression.setRightExpression(flattenExpression(binaryExpression.getRightExpression()));
          return binaryExpression;
        }

        List<Expression> children = new ArrayList<>();
        children.add(binaryExpression.getLeftExpression());
        children.add(binaryExpression.getRightExpression());
        Operator op = binaryExpression.getOp();
        List<Operator> ops = new ArrayList<>();
        ops.add(Operator.PLUS);
        ops.add(op);

        // 将二叉树子节点中同优先级运算符的节点上提拍平到当前多叉节点上，不同优先级的不拍平
        boolean fixedPoint = false;
        while(!fixedPoint){
          fixedPoint = true;
          for (int i = 0; i < children.size(); i++){
            if (children.get(i).getType() == Expression.ExpressionType.Binary){
              BinaryExpression childBiExpr = (BinaryExpression) children.get(i);
              if (Operator.hasSamePriority(op, childBiExpr.getOp())){
                children.set(i,childBiExpr.getLeftExpression());
                children.add(i+1, childBiExpr.getRightExpression());
                ops.add(i+1, childBiExpr.getOp());
                fixedPoint = false;
                break;
              }
            }else if (children.get(i).getType() == Expression.ExpressionType.Unary){
              // UnaryExpression就是前面是正负号的情况，也提取上来拍平
              UnaryExpression childUnaryExpr = (UnaryExpression) children.get(i);
              if(op == Operator.PLUS || op == Operator.MINUS){
                children.set(i, childUnaryExpr.getExpression());
                if(childUnaryExpr.getOperator() == Operator.MINUS) {
                  if(ops.get(i) == Operator.PLUS) {
                    ops.set(i, Operator.MINUS);
                  } else if(ops.get(i) == Operator.MINUS){
                    ops.set(i, Operator.PLUS);
                  }
                }
                fixedPoint = false;
                break;
              }
            }
          }
        }
        // 剩余的无需拍平，递归处理
        children.replaceAll(ExprUtils::flattenExpression);
        return new MultipleExpression(children, ops);
      case Multiple:
        MultipleExpression multipleExpression = (MultipleExpression) expr;
        multipleExpression.getChildren().replaceAll(ExprUtils::flattenExpression);
        return multipleExpression;
      default:
        throw new IllegalArgumentException(String.format("Unknown expr type: %s", expr.getType()));
    }
  }

  /**
   *
   */
  public static Expression foldMultipleExpression(MultipleExpression multipleExpression){
    List<Expression> children = multipleExpression.getChildren();
    Operator firstOp = multipleExpression.getOps().get(0);
    Value constantValue = new Value(0L);
    boolean isStarOrDiv = false;
    if(firstOp == Operator.STAR || firstOp ==  Operator.DIV){
      constantValue = new Value(1L);
      isStarOrDiv = true;
    }

    // 计算多叉树型中常量表达式的值
    for (int i = 0; i < children.size(); i++) {
      if (children.get(i).getType() == Expression.ExpressionType.Constant) {
        if (i == 0) {
          constantValue = calculateConstantExpr((ConstantExpression) children.get(i));
          if (multipleExpression.getOps().get(0) == Operator.MINUS){
            constantValue = calculateUnaryExpr(null, new UnaryExpression(Operator.MINUS, children.get(i)));
          }
        } else {
          cn.edu.tsinghua.iginx.engine.shared.expr.Operator op = multipleExpression.getOps().get(i);
          Value rightValue = calculateConstantExpr((ConstantExpression) children.get(i));
          if (!constantValue.getDataType().equals(rightValue.getDataType())) { // 两值类型不同，但均为数值类型，转为double再运算
            if (DataTypeUtils.isNumber(constantValue.getDataType())
                    && DataTypeUtils.isNumber(rightValue.getDataType())) {
              constantValue = ValueUtils.transformToDouble(constantValue);
              rightValue = ValueUtils.transformToDouble(rightValue);
            }else {
              throw new RuntimeException("the type of constant value is not number");
            }
          }
          switch (op) {
            case PLUS:
              constantValue = calculatePlus(constantValue, rightValue);
              break;
            case MINUS:
              constantValue = calculateMinus(constantValue, rightValue);
              break;
            case STAR:
              constantValue = calculateStar(constantValue, rightValue);
              break;
            case DIV:
              constantValue = calculateDiv(constantValue, rightValue);
              break;
            case MOD:
              constantValue = calculateMod(constantValue, rightValue);
              break;
          }
        }
      }
    }

    //把多叉树型中的常量表达式都删去，然后在开头加入计算好的常量值
    List<Expression> newChildren = new ArrayList<>();
    List<Operator> newOps = new ArrayList<>();
    for (int i = 0; i < children.size(); i++) {
      if (children.get(i).getType() != Expression.ExpressionType.Constant) {
        newChildren.add(children.get(i));
        if (i != 0) {
          newOps.add(multipleExpression.getOps().get(i));
        }else{
          if(isStarOrDiv){
            newOps.add(Operator.STAR);
          } else {
           newOps.add(Operator.PLUS);
          }
        }
      }
    }

    newChildren.add(0, new ConstantExpression(constantValue.getValue()));
    newOps.add(0, Operator.PLUS);
    multipleExpression.setChildren(newChildren);
    multipleExpression.setOps(newOps);

    if(newChildren.size() == 1){
      return newChildren.get(0);
    }
    return multipleExpression;
  }

}
