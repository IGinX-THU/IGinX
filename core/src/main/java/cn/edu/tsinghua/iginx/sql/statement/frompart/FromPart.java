package cn.edu.tsinghua.iginx.sql.statement.frompart;

import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import java.util.List;
import java.util.Map;

public interface FromPart {

  FromPartType getType();

  Map<String, String> getAliasMap();

  boolean hasAlias();

  boolean hasSinglePrefix();

  List<String> getPatterns();

  String getOriginPrefix();

  String getPrefix();

  JoinCondition getJoinCondition();

  void setJoinCondition(JoinCondition joinCondition);

  List<String> getFreeVariables();
}
