package cn.edu.tsinghua.iginx.engine.shared.expr;

public enum Operator {
  PLUS,
  MINUS,
  STAR,
  DIV,
  MOD;

  public static String operatorToString(Operator operator) {
    switch (operator) {
      case PLUS:
        return "+";
      case MINUS:
        return "-";
      case STAR:
        return "×";
      case DIV:
        return "÷";
      case MOD:
        return "%";
      default:
        return "";
    }
  }

  /** 判断两个op是否有相同的优先级 */
  public static boolean hasSamePriority(Operator op1, Operator op2) {
    return ((op1 == PLUS || op1 == MINUS) && (op2 == PLUS || op2 == MINUS))
        || ((op1 == STAR || op1 == DIV) && (op2 == STAR || op2 == DIV))
        || ((op1 == MOD) && (op2 == MOD));
  }

  /** 获取op的相反op */
  public static Operator getOppositeOp(Operator op) {
    switch (op) {
      case PLUS:
        return MINUS;
      case MINUS:
        return PLUS;
      case STAR:
        return DIV;
      case DIV:
        return STAR;
      case MOD:
        return MOD;
    }
    return null;
  }
}
