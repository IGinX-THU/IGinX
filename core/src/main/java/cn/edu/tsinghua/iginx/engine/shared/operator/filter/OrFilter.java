package cn.edu.tsinghua.iginx.engine.shared.operator.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class OrFilter implements Filter {

  private final FilterType type = FilterType.Or;

  private final List<Filter> children;

  public OrFilter(List<Filter> children) {
    this.children = children;
  }

  public List<Filter> getChildren() {
    return children;
  }

  @Override
  public void accept(FilterVisitor visitor) {
    visitor.visit(this);
    this.children.forEach(child -> child.accept(visitor));
  }

  @Override
  public FilterType getType() {
    return type;
  }

  public void setChildren(List<Filter> children) {
    this.children.clear();
    this.children.addAll(children);
  }

  @Override
  public Filter copy() {
    List<Filter> newChildren = new ArrayList<>();
    children.forEach(e -> newChildren.add(e.copy()));
    return new OrFilter(newChildren);
  }

  @Override
  public String toString() {
    return children.stream().map(Object::toString).collect(Collectors.joining(" || ", "(", ")"));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OrFilter filter = (OrFilter) o;
    return type == filter.type && Objects.equals(children, filter.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, children);
  }
}
