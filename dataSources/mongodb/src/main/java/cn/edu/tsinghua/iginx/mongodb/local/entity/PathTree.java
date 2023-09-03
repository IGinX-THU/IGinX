package cn.edu.tsinghua.iginx.mongodb.local.entity;

import cn.edu.tsinghua.iginx.mongodb.tools.NameUtils;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

public class PathTree {

  private final Map<String, PathTree> children;

  private boolean leaf;

  public PathTree() {
    this.children = new HashMap<>();
    this.leaf = false;
  }

  public void put(Iterator<String> nodes) {
    if (!nodes.hasNext()) {
      leaf = true;
      return;
    }

    String node = nodes.next();
    if (NameUtils.isWildcard(node)) node = null;

    children.computeIfAbsent(node, k -> new PathTree()).put(nodes);
  }

  private void put(PathTree anotherTree) {
    if (anotherTree.leaf) {
      leaf = true;
    }

    for (Map.Entry<String, PathTree> child : anotherTree.children.entrySet()) {
      children.computeIfAbsent(child.getKey(), k -> new PathTree()).put(child.getValue());
    }
  }

  public Map<String, PathTree> getChildren() {
    return children;
  }

  public boolean isLeaf() {
    return leaf;
  }

  @Override
  public String toString() {
    return "PathTree{" + "children=" + children + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PathTree that = (PathTree) o;
    return Objects.equals(children, that.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(children);
  }

  public boolean hasWildcardChild() {
    return children.containsKey(null);
  }

  public void eliminateWildcardChild(Iterable<String> allNodes) {
    PathTree subTree = children.remove(null);
    if (subTree == null) return;

    PathTree wildcardSubTree = new PathTree();
    wildcardSubTree.children.put(null, subTree);

    for (String node : allNodes) {
      PathTree child = children.computeIfAbsent(node, k -> new PathTree());
      child.put(subTree);
      child.put(wildcardSubTree);
    }
  }
}
