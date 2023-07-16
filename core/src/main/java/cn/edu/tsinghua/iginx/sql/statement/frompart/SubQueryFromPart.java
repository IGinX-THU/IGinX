package cn.edu.tsinghua.iginx.sql.statement.frompart;

import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import java.util.List;

public class SubQueryFromPart implements FromPart {

  private final FromPartType type = FromPartType.SubQueryFromPart;
  private final SelectStatement subQuery;
  private final List<String> patterns;
  private final boolean isJoinPart;
  private JoinCondition joinCondition;
  private final String alias;

  public SubQueryFromPart(SelectStatement subQuery) {
    this(subQuery, "");
  }

  public SubQueryFromPart(SelectStatement subQuery, String alias) {
    this.subQuery = subQuery;
    this.patterns = subQuery.calculatePrefixSet();
    this.isJoinPart = false;
    this.alias = alias;
  }

  public SubQueryFromPart(SelectStatement subQuery, JoinCondition joinCondition) {
    this(subQuery, joinCondition, "");
  }

  public SubQueryFromPart(SelectStatement subQuery, JoinCondition joinCondition, String alias) {
    this.subQuery = subQuery;
    this.patterns = subQuery.calculatePrefixSet();
    this.isJoinPart = true;
    this.joinCondition = joinCondition;
    this.alias = alias;
  }

  public String getAlias() {
    return alias;
  }

  public boolean hasAlias() {
    return alias != null && !alias.equals("");
  }

  @Override
  public FromPartType getType() {
    return type;
  }

  @Override
  public boolean hasSinglePrefix() {
    return patterns.size() == 1;
  }

  @Override
  public List<String> getPatterns() {
    return patterns;
  }

  @Override
  public String getPrefix() {
    // 如果子查询没有一个公共的前缀，返回null
    if (hasSinglePrefix()) {
      if (patterns.get(0).endsWith(Constants.ALL_PATH_SUFFIX)) {
        return patterns.get(0).substring(0, patterns.get(0).length() - 2);
      } else {
        return patterns.get(0);
      }
    } else {
      return null;
    }
  }

  @Override
  public boolean isJoinPart() {
    return isJoinPart;
  }

  @Override
  public JoinCondition getJoinCondition() {
    return joinCondition;
  }

  public SelectStatement getSubQuery() {
    return subQuery;
  }

  @Override
  public List<String> getFreeVariables() {
    return subQuery.getFreeVariables();
  }
}
