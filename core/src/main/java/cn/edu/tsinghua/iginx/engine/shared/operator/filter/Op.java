package cn.edu.tsinghua.iginx.engine.shared.operator.filter;

import cn.edu.tsinghua.iginx.sql.exception.SQLParserException;

public enum Op {
  // or op (default): [0, 9]
  GE(0),
  G,
  LE,
  L,
  E,
  NE,
  LIKE,
  // and op: [10, 19]
  GE_AND(10),
  G_AND,
  LE_AND,
  L_AND,
  E_AND,
  NE_AND,
  LIKE_AND;

  private int value;

  Op() {
    this(OpCounter.nextValue);
  }

  Op(int value) {
    this.value = value;
    OpCounter.nextValue = value + 1;
  }

  public int getValue() {
    return value;
  }

  private static class OpCounter {

    private static int nextValue = 0;
  }

  public static Op getOpposite(Op op) {
    switch (op) {
      case NE:
      case NE_AND:
        return E;
      case E:
      case E_AND:
        return NE;
      case GE:
      case GE_AND:
        return L;
      case LE:
      case LE_AND:
        return G;
      case G:
      case G_AND:
        return LE;
      case L:
      case L_AND:
        return GE;
      case LIKE:
      case LIKE_AND:
        return LIKE;
      default:
        return op;
    }
  }

  public static Op getDeMorganOpposite(Op op) {
    switch (op) {
      case NE:
        return E_AND;
      case E:
        return NE_AND;
      case GE:
        return L_AND;
      case LE:
        return G_AND;
      case G:
        return LE_AND;
      case L:
        return GE_AND;
      case LIKE:
        return LIKE_AND;
      case NE_AND:
        return E;
      case E_AND:
        return NE;
      case GE_AND:
        return L;
      case LE_AND:
        return G;
      case G_AND:
        return LE;
      case L_AND:
        return GE;
      case LIKE_AND:
        return LIKE;
      default:
        return op;
    }
  }

  public static Op getDirectionOpposite(Op op) {
    switch (op) {
      case GE:
        return LE;
      case LE:
        return GE;
      case G:
        return L;
      case L:
        return G;
      case GE_AND:
        return LE_AND;
      case LE_AND:
        return GE_AND;
      case G_AND:
        return L_AND;
      case L_AND:
        return G_AND;
      default:
        return op;
    }
  }

  public static Op str2Op(String op) {
    switch (op) {
      case "=":
      case "==":
      case "|=":
      case "|==":
        return E;
      case "!=":
      case "<>":
      case "|!=":
      case "|<>":
        return NE;
      case ">":
      case "|>":
        return G;
      case ">=":
      case "|>=":
        return GE;
      case "<":
      case "|<":
        return L;
      case "<=":
      case "|<=":
        return LE;
      case "like":
      case "|like":
        return LIKE;
      case "&=":
      case "&==":
        return E_AND;
      case "&!=":
      case "&<>":
        return NE_AND;
      case "&>":
        return G_AND;
      case "&>=":
        return GE_AND;
      case "&<":
        return L_AND;
      case "&<=":
        return LE_AND;
      case "&like":
        return LIKE_AND;
      default:
        throw new SQLParserException(String.format("Not support comparison operator %s", op));
    }
  }

  public static String op2Str(Op op) {
    switch (op) {
      case GE:
        return ">=";
      case G:
        return ">";
      case LE:
        return "<=";
      case L:
        return "<";
      case E:
        return "==";
      case NE:
        return "!=";
      case LIKE:
        return "like";
      case GE_AND:
        return "&>=";
      case G_AND:
        return "&>";
      case LE_AND:
        return "&<=";
      case L_AND:
        return "&<";
      case E_AND:
        return "&==";
      case NE_AND:
        return "&!=";
      case LIKE_AND:
        return "&like";
      default:
        return "";
    }
  }

  /**
   * 返回不带&的Op字符串(不需要去掉|，因为目前Op字符串中|<直接转换为<)
   *
   * @param op Op
   * @return 不带&的Op字符串
   */
  public static String op2StrWithoutAndOr(Op op) {
    String opStr = Op.op2Str(op);
    if (opStr.startsWith("&")) {
      return opStr.substring(1);
    }
    return opStr;
  }

  public static boolean isEqualOp(Op op) {
    return op.equals(E) || op.equals(E_AND);
  }

  public static boolean isLikeOp(Op op) {
    return op.equals(LIKE) || op.equals(LIKE_AND);
  }

  public static boolean isOrOp(Op op) {
    return op.value >= 0 && op.value <= 9;
  }

  public static boolean isAndOp(Op op) {
    return op.value >= 10 && op.value <= 19;
  }
}
