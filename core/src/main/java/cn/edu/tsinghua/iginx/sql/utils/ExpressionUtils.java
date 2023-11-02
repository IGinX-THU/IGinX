package cn.edu.tsinghua.iginx.sql.utils;

import cn.edu.tsinghua.iginx.sql.expression.BinaryExpression;
import cn.edu.tsinghua.iginx.sql.expression.BracketExpression;
import cn.edu.tsinghua.iginx.sql.expression.Expression;
import cn.edu.tsinghua.iginx.sql.expression.UnaryExpression;

public class ExpressionUtils {

  public static boolean isConstantArithmeticExpr(Expression expression) {
    switch (expression.getType()) {
      case Constant:
        return true;
      case Bracket:
        BracketExpression bracketExpression = (BracketExpression) expression;
        return isConstantArithmeticExpr(bracketExpression.getExpression());
      case Unary:
        UnaryExpression unaryExpression = (UnaryExpression) expression;
        return isConstantArithmeticExpr(unaryExpression.getExpression());
      case Binary:
        BinaryExpression binaryExpression = (BinaryExpression) expression;
        return isConstantArithmeticExpr(binaryExpression.getLeftExpression())
            && isConstantArithmeticExpr(binaryExpression.getRightExpression());
      default:
        return false;
    }
  }
}
