package cn.edu.tsinghua.iginx.sql.statement.selectstatement.seslectstatementclause;

import cn.edu.tsinghua.iginx.sql.statement.selectstatement.UnarySelectStatement.QueryType;
import java.util.ArrayList;
import java.util.List;

public class GroupByClause {
  private boolean hasDownsample;
  private boolean hasGroupBy;
  private QueryType queryType;
  private final List<String> groupByPaths;

  public GroupByClause() {
    groupByPaths = new ArrayList<>();
    hasDownsample = false;
    hasGroupBy = false;
    queryType = QueryType.Unknown;
  }

  public void addGroupByPath(String path) {
    groupByPaths.add(path);
    hasGroupBy = true;
  }

  public List<String> getGroupByPaths() {
    return groupByPaths;
  }

  public boolean hasDownsample() {
    return hasDownsample;
  }

  public void setHasDownsample(boolean hasDownsample) {
    this.hasDownsample = hasDownsample;
  }

  public boolean hasGroupBy() {
    return hasGroupBy;
  }

  public void setHasGroupBy(boolean hasGroupBy) {
    this.hasGroupBy = hasGroupBy;
  }

  public void setQueryType(QueryType queryType) {
    this.queryType = queryType;
  }

  public QueryType getQueryType() {
    return queryType;
  }
}
