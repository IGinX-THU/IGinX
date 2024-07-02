package cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
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

  public static Value calculateMultipleExpr(Row row, MultipleExpression multipleExpr) {
    List<Expression> children = multipleExpr.getChildren();
    List<Operator> ops = multipleExpr.getOps();
    List<Value> values = new ArrayList<>();
    for (Expression expr : children) {
      values.add(calculateExpr(row, expr));
    }

    if (ops.get(0) == Operator.MINUS) {
      values.set(0, calculateUnaryExpr(row, new UnaryExpression(Operator.MINUS, children.get(0))));
      ops.set(0, Operator.PLUS);
    }

    for (int i = 1; i < ops.size(); i++) {
      Operator op = ops.get(i);
      Value left = values.get(i - 1);
      Value right = values.get(i);
      if (!left.getDataType().equals(right.getDataType())) {
        if (DataTypeUtils.isNumber(left.getDataType())
            && DataTypeUtils.isNumber(right.getDataType())) {
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
        return new Value((double) left.getIntV() / (double) right.getIntV());
      case LONG:
        return new Value((double) left.getLongV() / (double) right.getLongV());
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
      case Multiple:
        List<String> paths = new ArrayList<>();
        for (Expression child : ((MultipleExpression) expr).getChildren()) {
          paths.addAll(getPathFromExpr(child));
        }
        return paths;
      default:
        throw new IllegalArgumentException(String.format("Unknown expr type: %s", expr.getType()));
    }
  }

  /**
   * 把表达式的二叉树型转换为多叉树型
   *
   * @param expr 表达式
   * @return 多叉树型表达式
   */
  public static Expression flattenExpression(Expression expr) {
    switch (expr.getType()) {
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
        if (binaryExpression.getOp() == Operator.MOD) {
          binaryExpression.setLeftExpression(
              flattenExpression(binaryExpression.getLeftExpression()));
          binaryExpression.setRightExpression(
              flattenExpression(binaryExpression.getRightExpression()));
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
        int nMatches = 0;
        while (!fixedPoint && nMatches < 10000) {
          fixedPoint = true;
          for (int i = 0; i < children.size(); i++) {
            if (children.get(i).getType() == Expression.ExpressionType.Binary) {
              BinaryExpression childBiExpr = (BinaryExpression) children.get(i);
              if (Operator.hasSamePriority(op, childBiExpr.getOp())) {
                children.set(i, childBiExpr.getLeftExpression());
                children.add(i + 1, childBiExpr.getRightExpression());
                ops.add(i + 1, childBiExpr.getOp());
                fixedPoint = false;
                nMatches++;
                break;
              }
            } else if (children.get(i).getType() == Expression.ExpressionType.Multiple) {
              // 如果是多叉树且运算符优先级相同，也提取上来
              MultipleExpression childMultipleExpr = (MultipleExpression) children.get(i);
              if (Operator.hasSamePriority(op, childMultipleExpr.getOpType())) {
                children.addAll(i + 1, childMultipleExpr.getChildren());
                ops.addAll(
                    i + 2,
                    childMultipleExpr.getOps().subList(1, childMultipleExpr.getOps().size()));
                if (childMultipleExpr.getOps().get(0) == Operator.MINUS) {
                  children.set(
                      i + 1,
                      new UnaryExpression(Operator.MINUS, childMultipleExpr.getChildren().get(0)));
                }
              }
              children.remove(i);
              fixedPoint = false;
              nMatches++;
              break;
            } else if (children.get(i).getType() == Expression.ExpressionType.Unary) {
              // UnaryExpression就是前面是负号的情况，也提取上来拍平，一般用于变量前的负号和括号前的负号
              UnaryExpression childUnaryExpr = (UnaryExpression) children.get(i);
              if (childUnaryExpr.getOperator() != Operator.MINUS
                  || childUnaryExpr.getExpression().getType() == Expression.ExpressionType.Base) {
                continue;
              }
              if (op == Operator.PLUS || op == Operator.MINUS) {
                // 如果目前是加减法，反转当前UnaryExpression的符号
                children.set(i, childUnaryExpr.getExpression());
                ops.set(i, Operator.getOppositeOp(op));
                fixedPoint = false;
                nMatches++;
                break;
              } else if (op == Operator.DIV || op == Operator.STAR) {
                // 如果目前是乘除法，那就给目前表达式加上一个 *-1
                children.set(i, childUnaryExpr.getExpression());
                children.add(i + 1, new ConstantExpression(-1L));
                ops.add(i + 1, Operator.STAR);
                fixedPoint = false;
                nMatches++;
                break;
              }
            } else if (children.get(i).getType() == Expression.ExpressionType.Bracket) {
              // 如果是括号表达式，递归处理
              // 如果括号内不是二叉树或多叉树，直接去掉括号
              BracketExpression childBracketExpr = (BracketExpression) children.get(i);
              if (childBracketExpr.getExpression().getType() != Expression.ExpressionType.Binary
                  && childBracketExpr.getExpression().getType()
                      != Expression.ExpressionType.Multiple) {
                children.set(i, childBracketExpr.getExpression());
                fixedPoint = false;
                nMatches++;
                break;
              } else {
                Operator bracketOp = ops.get(i);
                if (childBracketExpr.getExpression().getType()
                    == Expression.ExpressionType.Binary) {
                  BinaryExpression childBiExpr =
                      (BinaryExpression) childBracketExpr.getExpression();
                  if (!Operator.hasSamePriority(op, childBiExpr.getOp())) {
                    continue;
                  }
                } else if (childBracketExpr.getExpression().getType()
                    == Expression.ExpressionType.Multiple) {
                  MultipleExpression childMultipleExpr =
                      (MultipleExpression) childBracketExpr.getExpression();
                  if (!Operator.hasSamePriority(op, childMultipleExpr.getOpType())) {
                    continue;
                  }
                }

                if (bracketOp == Operator.DIV || bracketOp == Operator.MINUS) {
                  // 如果是除或减，递归反转该子节点的符号
                  children.set(i, reverseOperator(childBracketExpr.getExpression(), bracketOp));
                  fixedPoint = false;
                  nMatches++;
                  break;
                } else if (bracketOp == Operator.PLUS || bracketOp == Operator.STAR) {
                  // 如果是加或乘，拆掉括号
                  children.set(i, childBracketExpr.getExpression());
                  fixedPoint = false;
                  nMatches++;
                  break;
                }
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

  /** 折叠多叉树型中的常量表达式 */
  public static Expression foldMultipleExpression(MultipleExpression multipleExpression) {
    List<Expression> children = multipleExpression.getChildren();
    Operator opType = multipleExpression.getOpType();
    Value constantValue = new Value(0L);
    boolean isStarOrDiv = false;
    if (opType == Operator.STAR || opType == Operator.DIV) {
      constantValue = new Value(1L);
      isStarOrDiv = true;
    }

    // 计算多叉树型中常量表达式的值
    for (int i = 0; i < children.size(); i++) {
      if (children.get(i).getType() == Expression.ExpressionType.Constant) {
        if (i == 0) {
          constantValue = calculateConstantExpr((ConstantExpression) children.get(i));
          if (multipleExpression.getOps().get(0) == Operator.MINUS) {
            constantValue =
                calculateUnaryExpr(null, new UnaryExpression(Operator.MINUS, children.get(i)));
          }
        } else {
          cn.edu.tsinghua.iginx.engine.shared.expr.Operator op = multipleExpression.getOps().get(i);
          Value rightValue = calculateConstantExpr((ConstantExpression) children.get(i));
          if (!constantValue
              .getDataType()
              .equals(rightValue.getDataType())) { // 两值类型不同，但均为数值类型，转为double再运算
            if (DataTypeUtils.isNumber(constantValue.getDataType())
                && DataTypeUtils.isNumber(rightValue.getDataType())) {
              constantValue = ValueUtils.transformToDouble(constantValue);
              rightValue = ValueUtils.transformToDouble(rightValue);
            } else {
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

    // 把多叉树型中的常量表达式都删去，然后在开头加入计算好的常量值
    List<Expression> newChildren = new ArrayList<>();
    List<Operator> newOps = new ArrayList<>();
    for (int i = 0; i < children.size(); i++) {
      if (children.get(i).getType() != Expression.ExpressionType.Constant) {
        newChildren.add(children.get(i));
        if (i != 0) {
          newOps.add(multipleExpression.getOps().get(i));
        } else {
          if (isStarOrDiv) {
            newOps.add(Operator.STAR);
          } else {
            newOps.add(Operator.PLUS);
          }
        }
      }
    }

    try {
      // 如果常量为0且是乘除法，直接返回0
      if (ValueUtils.compare(constantValue, new Value(0L)) == 0 && isStarOrDiv) {
        return new ConstantExpression(0);
      }
      // 如果常量为0且是加减法，常量为1且是乘法，直接返回剩下的表达式
      if (ValueUtils.compare(constantValue, new Value(0L)) == 0 && !isStarOrDiv) {
        return multipleExpression;
      }
      if (ValueUtils.compare(constantValue, new Value(1L)) == 0
          && newOps.size() > 0
          && newOps.get(0) == Operator.STAR) {
        return multipleExpression;
      }
    } catch (PhysicalException e) {
      e.printStackTrace();
    }

    newChildren.add(0, new ConstantExpression(constantValue.getValue()));
    newOps.add(0, Operator.PLUS);
    multipleExpression.setChildren(newChildren);
    multipleExpression.setOps(newOps);

    if (newChildren.size() == 1) {
      return newChildren.get(0);
    }
    return multipleExpression;
  }

  /**
   * 判断给定的Expression中是否有一个MultipleExpression，且其中包含多个常量表达式
   *
   * @param expression 给定的Expression
   * @return 如果给定的Expression中有一个MultipleExpression，且其中包含多个常量表达式，则返回true，否则返回false
   */
  public static boolean hasMultiConstantsInMultipleExpression(Expression expression) {
    switch (expression.getType()) {
      case Multiple:
        MultipleExpression multipleExpression = (MultipleExpression) expression;
        List<Expression> children = multipleExpression.getChildren();
        int constantCount = 0;
        for (Expression child : children) {
          if (child.getType() == Expression.ExpressionType.Constant) {
            constantCount++;
            if (constantCount > 1) {
              return true;
            }
          } else if (hasMultiConstantsInMultipleExpression(child)) {
            return true;
          }
        }
        return false;
      case Binary:
        return hasMultiConstantsInMultipleExpression(
                ((BinaryExpression) expression).getLeftExpression())
            || hasMultiConstantsInMultipleExpression(
                ((BinaryExpression) expression).getRightExpression());
      case Unary:
        return hasMultiConstantsInMultipleExpression(
            ((UnaryExpression) expression).getExpression());
      case Bracket:
        return hasMultiConstantsInMultipleExpression(
            ((BracketExpression) expression).getExpression());
      default:
        return false;
    }
  }

  public static Expression foldExpression(Expression expression) {
    switch (expression.getType()) {
      case Multiple:
        MultipleExpression multipleExpression = (MultipleExpression) expression;
        multipleExpression.getChildren().replaceAll(ExprUtils::foldExpression);
        return ExprUtils.foldMultipleExpression(multipleExpression);
      case Unary:
        UnaryExpression unaryExpression = (UnaryExpression) expression;
        Expression unaryFold = foldExpression(unaryExpression.getExpression());
        if (unaryFold.getType() == Expression.ExpressionType.Constant) {
          return unaryFold;
        }
        unaryExpression.setExpression(unaryFold);
        return unaryExpression;
      case Binary:
        BinaryExpression binaryExpression = (BinaryExpression) expression;
        binaryExpression.setLeftExpression(foldExpression(binaryExpression.getLeftExpression()));
        binaryExpression.setRightExpression(foldExpression(binaryExpression.getRightExpression()));
        return binaryExpression;
      case Bracket:
        BracketExpression bracketExpression = (BracketExpression) expression;
        Expression bracketFold = foldExpression(bracketExpression.getExpression());
        if (bracketFold.getType() == Expression.ExpressionType.Constant) {
          return bracketFold;
        }
        bracketExpression.setExpression(bracketFold);
        return bracketExpression;
      default:
        return expression;
    }
  }

  /**
   * 反转表达式中的运算符，仅反转给定优先级的运算符，碰到非给定优先级的运算符、非多子节点时停止
   *
   * @param expression
   * @return
   */
  public static Expression reverseOperator(Expression expression, Operator op) {
    switch (expression.getType()) {
      case Multiple:
        MultipleExpression multipleExpression = (MultipleExpression) expression;
        if (Operator.hasSamePriority(multipleExpression.getOpType(), op)) {
          for (int i = 1; i < multipleExpression.getOps().size(); i++) {
            multipleExpression
                .getOps()
                .set(i, Operator.getOppositeOp(multipleExpression.getOps().get(i)));
          }
        }
        multipleExpression.getChildren().replaceAll(e -> reverseOperator(e, op));
        return multipleExpression;
      case Binary:
        BinaryExpression binaryExpression = (BinaryExpression) expression;
        if (Operator.hasSamePriority(binaryExpression.getOp(), op)) {
          binaryExpression.setOp(Operator.getOppositeOp(binaryExpression.getOp()));
        } else {
          return expression;
        }
        binaryExpression.setLeftExpression(
            reverseOperator(binaryExpression.getLeftExpression(), op));
        binaryExpression.setRightExpression(
            reverseOperator(binaryExpression.getRightExpression(), op));
        return binaryExpression;
      default:
        return expression;
    }
  }

  public static Expression copy(Expression expression) {
    switch (expression.getType()) {
      case Constant:
        return new ConstantExpression(((ConstantExpression) expression).getValue());
      case Base:
        return new BaseExpression(((BaseExpression) expression).getColumnName());
      case Function:
        FuncExpression funcExpression = (FuncExpression) expression;
        return new FuncExpression(
            funcExpression.getFuncName(),
            funcExpression.getColumns(),
            funcExpression.getArgs(),
            funcExpression.getKvargs(),
            funcExpression.isDistinct());
      case Bracket:
        return new BracketExpression(copy(((BracketExpression) expression).getExpression()));
      case Unary:
        UnaryExpression unaryExpression = (UnaryExpression) expression;
        return new UnaryExpression(
            unaryExpression.getOperator(), copy(unaryExpression.getExpression()));
      case Binary:
        BinaryExpression binaryExpression = (BinaryExpression) expression;
        return new BinaryExpression(
            copy(binaryExpression.getLeftExpression()),
            copy(binaryExpression.getRightExpression()),
            binaryExpression.getOp());
      case Multiple:
        MultipleExpression multipleExpression = (MultipleExpression) expression;
        List<Expression> children = new ArrayList<>();
        for (Expression child : multipleExpression.getChildren()) {
          children.add(copy(child));
        }
        return new MultipleExpression(children, multipleExpression.getOps());
      default:
        throw new IllegalArgumentException(
            String.format("Unknown expr type: %s", expression.getType()));
    }
  }
}
