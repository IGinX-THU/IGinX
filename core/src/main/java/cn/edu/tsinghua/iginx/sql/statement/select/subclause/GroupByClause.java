package cn.edu.tsinghua.iginx.sql.statement.select.subclause;

import cn.edu.tsinghua.iginx.sql.statement.select.UnarySelectStatement.QueryType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GroupByClause {
  private boolean hasDownsample;
  private boolean hasGroupBy;
  private QueryType queryType;
  private final List<String> groupByPaths;
  private final Set<String> pathSet;
  private long precision;
  private long slideDistance;

  public GroupByClause() {
    groupByPaths = new ArrayList<>();
    pathSet = new HashSet<>();
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

  public Set<String> getPathSet() {
    return pathSet;
  }

  public void addPath(String path) {
    pathSet.add(path);
  }

  public long getPrecision() {
    return precision;
  }

  public void setPrecision(long precision) {
    this.precision = precision;
  }

  public long getSlideDistance() {
    return slideDistance;
  }

  public void setSlideDistance(long slideDistance) {
    this.slideDistance = slideDistance;
  }
}
