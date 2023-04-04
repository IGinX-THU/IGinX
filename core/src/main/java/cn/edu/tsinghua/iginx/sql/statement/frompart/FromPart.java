package cn.edu.tsinghua.iginx.sql.statement.frompart;

import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import java.util.List;

public interface FromPart {

    FromPartType getType();

    String getPath();

    boolean isJoinPart();

    JoinCondition getJoinCondition();

    List<String> getFreeVariables();
}
