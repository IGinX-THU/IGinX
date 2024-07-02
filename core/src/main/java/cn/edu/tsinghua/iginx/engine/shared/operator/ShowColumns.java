package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.sql.statement.ShowColumnsStatement;
import java.util.HashSet;
import java.util.Set;

public class ShowColumns extends AbstractUnaryOperator {

  private final Set<String> pathRegexSet;
  private final TagFilter tagFilter;
  private final int limit;
  private final int offset;

  public ShowColumns(
      GlobalSource source, Set<String> pathRegexSet, TagFilter tagFilter, int limit, int offset) {
    super(OperatorType.ShowColumns, source);
    this.pathRegexSet = pathRegexSet;
    this.tagFilter = tagFilter;
    this.limit = limit;
    this.offset = offset;
  }

  public ShowColumns(GlobalSource source, ShowColumnsStatement statement) {
    this(
        source,
        statement.getPathRegexSet(),
        statement.getTagFilter(),
        statement.getLimit(),
        statement.getOffset());
  }

  public Set<String> getPathRegexSet() {
    return pathRegexSet;
  }

  public TagFilter getTagFilter() {
    return tagFilter;
  }

  public int getLimit() {
    return limit;
  }

  public int getOffset() {
    return offset;
  }

  @Override
  public Operator copy() {
    return new ShowColumns(
        (GlobalSource) getSource().copy(),
        new HashSet<>(pathRegexSet),
        tagFilter.copy(),
        limit,
        offset);
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new ShowColumns(
        (GlobalSource) source, new HashSet<>(pathRegexSet), tagFilter.copy(), limit, offset);
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    if (pathRegexSet != null && !pathRegexSet.isEmpty()) {
      builder.append("Patterns: ");
      for (String regex : pathRegexSet) {
        builder.append(regex).append(",");
      }
      builder.deleteCharAt(builder.length() - 1);
    }
    if (tagFilter != null) {
      builder.append(", TagFilter: ").append(tagFilter);
    }
    if (limit != Integer.MAX_VALUE || offset != 0) {
      builder.append(", Limit: ").append(limit).append(", Offset: ").append(offset);
    }
    return builder.toString();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    ShowColumns that = (ShowColumns) object;
    return limit == that.limit
        && offset == that.offset
        && pathRegexSet.equals(that.pathRegexSet)
        && tagFilter.equals(that.tagFilter);
  }
}
