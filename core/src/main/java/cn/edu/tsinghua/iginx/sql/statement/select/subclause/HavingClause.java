package cn.edu.tsinghua.iginx.sql.statement.select.subclause;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.sql.statement.frompart.SubQueryFromPart;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HavingClause {
  private Filter havingFilter;
  private final List<SubQueryFromPart> havingSubQueryParts;

  private final Set<String> pathSet;

  public HavingClause() {
    this.havingSubQueryParts = new ArrayList<>();
    this.pathSet = new HashSet<>();
  }

  public Filter getHavingFilter() {
    return havingFilter;
  }

  public void setHavingFilter(Filter havingFilter) {
    this.havingFilter = havingFilter;
  }

  public List<SubQueryFromPart> getHavingSubQueryParts() {
    return havingSubQueryParts;
  }

  public void addHavingSubQueryPart(SubQueryFromPart subQueryFromPart) {
    havingSubQueryParts.add(subQueryFromPart);
  }

  public boolean hasSubQuery() {
    return !havingSubQueryParts.isEmpty();
  }

  public Set<String> getPathSet() {
    return pathSet;
  }

  public void addPath(String path) {
    pathSet.add(path);
  }
}
