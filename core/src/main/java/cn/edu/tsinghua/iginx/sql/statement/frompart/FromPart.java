package cn.edu.tsinghua.iginx.sql.statement.frompart;

import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;

public interface FromPart {

    FromPartType getType();

    String getPath();

    boolean isJoinPart();

    JoinCondition getJoinCondition();
}
