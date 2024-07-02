package cn.edu.tsinghua.iginx.engine.shared.operator.filter;

import java.util.Objects;

public class NotFilter implements Filter {

  private final FilterType type = FilterType.Not;

  private Filter child;

  public NotFilter(Filter child) {
    this.child = child;
  }

  public Filter getChild() {
    return child;
  }

  public void setChild(Filter child) {
    this.child = child;
  }

  @Override
  public void accept(FilterVisitor visitor) {
    visitor.visit(this);
    this.child.accept(visitor);
  }

  @Override
  public FilterType getType() {
    return type;
  }

  @Override
  public Filter copy() {
    return new NotFilter(child.copy());
  }

  @Override
  public String toString() {
    return "!(" + child.toString() + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NotFilter notFilter = (NotFilter) o;
    return type == notFilter.type && Objects.equals(child, notFilter.child);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, child);
  }
}
