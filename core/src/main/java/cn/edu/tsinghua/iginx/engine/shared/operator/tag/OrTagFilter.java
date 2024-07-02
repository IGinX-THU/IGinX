package cn.edu.tsinghua.iginx.engine.shared.operator.tag;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class OrTagFilter implements TagFilter {

  private final List<TagFilter> children;

  public OrTagFilter(List<TagFilter> children) {
    this.children = children;
  }

  public List<TagFilter> getChildren() {
    return children;
  }

  @Override
  public TagFilterType getType() {
    return TagFilterType.Or;
  }

  @Override
  public TagFilter copy() {
    List<TagFilter> newChildren = new ArrayList<>();
    children.forEach(e -> newChildren.add(e.copy()));
    return new OrTagFilter(newChildren);
  }

  @Override
  public String toString() {
    return children.stream().map(Object::toString).collect(Collectors.joining(" || ", "(", ")"));
  }
}
