package cn.edu.tsinghua.iginx.sql.statement.frompart;

import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import java.util.List;

public interface FromPart {

  FromPartType getType();

  String getAlias();

  boolean hasAlias();

  boolean hasSinglePrefix();

  List<String> getPatterns();

  String getPrefix();

  JoinCondition getJoinCondition();

  void setJoinCondition(JoinCondition joinCondition);

  List<String> getFreeVariables();
}
