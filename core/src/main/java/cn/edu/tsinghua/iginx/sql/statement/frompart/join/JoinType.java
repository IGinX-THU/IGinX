package cn.edu.tsinghua.iginx.sql.statement.frompart.join;

public enum JoinType {
  CrossJoin(0),
  InnerJoin,
  LeftOuterJoin,
  RightOuterJoin,
  FullOuterJoin,
  SingleJoin,
  MarkJoin,

  // NaturalJoin[10,20)
  InnerNaturalJoin(10),
  LeftNaturalJoin,
  RightNaturalJoin,
  ;

  private int value;

  JoinType() {
    this(JoinType.OperatorTypeCounter.nextValue);
  }

  JoinType(int value) {
    this.value = value;
    JoinType.OperatorTypeCounter.nextValue = value + 1;
  }

  public int getValue() {
    return value;
  }

  private static class OperatorTypeCounter {

    private static int nextValue = 0;
  }

  public static boolean isNaturalJoin(JoinType type) {
    return type.value >= 10;
  }
}
