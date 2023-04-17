package cn.edu.tsinghua.iginx.sql.statement.frompart;

import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import java.util.List;

public interface FromPart {

    FromPartType getType();

    boolean hasSinglePrefix();

    List<String> getPatterns();

    String getPrefix();

    boolean isJoinPart();

    JoinCondition getJoinCondition();

    List<String> getFreeVariables();
}
