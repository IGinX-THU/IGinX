package cn.edu.tsinghua.iginx.sql.statement.selectstatement.seslectstatementclause;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.sql.statement.frompart.SubQueryFromPart;
import java.util.ArrayList;
import java.util.List;

public class HavingClause {
  private Filter havingFilter;
  private final List<SubQueryFromPart> havingSubQueryParts;

  public HavingClause() {
    this.havingSubQueryParts = new ArrayList<>();
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
}
