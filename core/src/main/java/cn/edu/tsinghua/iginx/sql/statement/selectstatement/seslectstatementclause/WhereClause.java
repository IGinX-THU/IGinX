package cn.edu.tsinghua.iginx.sql.statement.selectstatement.seslectstatementclause;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.sql.statement.frompart.SubQueryFromPart;
import java.util.ArrayList;
import java.util.List;

public class WhereClause {
  private final List<SubQueryFromPart> whereSubQueryParts;

  private Filter filter;

  private TagFilter tagFilter;

  private boolean hasValueFilter;

  public WhereClause() {
    this.whereSubQueryParts = new ArrayList<>();
    this.hasValueFilter = false;
  }

  public List<SubQueryFromPart> getWhereSubQueryParts() {
    return whereSubQueryParts;
  }

  public void addWhereSubQueryPart(SubQueryFromPart subQueryFromPart) {
    whereSubQueryParts.add(subQueryFromPart);
  }

  public Filter getFilter() {
    return filter;
  }

  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  public TagFilter getTagFilter() {
    return tagFilter;
  }

  public void setTagFilter(TagFilter tagFilter) {
    this.tagFilter = tagFilter;
  }

  public boolean hasValueFilter() {
    return hasValueFilter;
  }

  public void setHasValueFilter(boolean hasValueFilter) {
    this.hasValueFilter = hasValueFilter;
  }
}
