package cn.edu.tsinghua.iginx.sql.statement.selectstatement.seslectstatementclause;

import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.sql.statement.frompart.SubQueryFromPart;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class WhereClause {
  private final List<SubQueryFromPart> whereSubQueryParts;

  private Filter filter;

  private TagFilter tagFilter;

  private boolean hasValueFilter;

  private final List<Expression> expressions;

  private long startKey;

  private long endKey;

  private final Set<String> pathSet;

  public WhereClause() {
    this.whereSubQueryParts = new ArrayList<>();
    this.hasValueFilter = false;
    this.expressions = new ArrayList<>();
    this.pathSet = new HashSet<>();
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

  public void addExpression(Expression expression) {
    expressions.add(expression);
  }

  public List<Expression> getExpressions() {
    return expressions;
  }

  public long getStartKey() {
    return startKey;
  }

  public void setStartKey(long startKey) {
    this.startKey = startKey;
  }

  public long getEndKey() {
    return endKey;
  }

  public void setEndKey(long endKey) {
    this.endKey = endKey;
  }

  public Set<String> getPathSet() {
    return pathSet;
  }

  public void addPath(String path) {
    pathSet.add(path);
  }
}
