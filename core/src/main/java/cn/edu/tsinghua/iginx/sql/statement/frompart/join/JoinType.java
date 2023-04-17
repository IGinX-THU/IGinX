package cn.edu.tsinghua.iginx.sql.statement.frompart.join;

public enum JoinType {
    CrossJoin,
    InnerJoin,
    InnerNaturalJoin,
    LeftNaturalJoin,
    RightNaturalJoin,
    LeftOuterJoin,
    RightOuterJoin,
    FullOuterJoin,
    SingleJoin,
    MarkJoin;

    public static boolean isNaturalJoin(JoinType type) {
        return type == InnerNaturalJoin || type == LeftNaturalJoin || type == RightNaturalJoin;
    }
}
