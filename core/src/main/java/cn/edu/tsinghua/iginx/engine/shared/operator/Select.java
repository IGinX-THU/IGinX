package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class Select extends AbstractUnaryOperator {

  private Filter filter;

  private TagFilter tagFilter;

  public Select(Source source, Filter filter, TagFilter tagFilter) {
    super(OperatorType.Select, source);
    if (filter == null) {
      throw new IllegalArgumentException("filter shouldn't be null");
    }
    this.filter = filter;
    this.tagFilter = tagFilter;
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

  @Override
  public Operator copy() {
    return new Select(
        getSource().copy(), filter.copy(), tagFilter == null ? null : tagFilter.copy());
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new Select(source, filter.copy(), tagFilter == null ? null : tagFilter.copy());
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    builder.append("Filter: ").append(filter.toString());
    if (tagFilter != null) {
      builder.append(", TagFilter: ").append(tagFilter.toString());
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
    if (!super.equals(object)) {
      return false;
    }
    Select select = (Select) object;
    return filter.equals(select.filter) && tagFilter.equals(select.tagFilter);
  }
}
