/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.mongodb.dummy;

import cn.edu.tsinghua.iginx.mongodb.tools.NameUtils;
import java.util.*;

class PathTree {

  private final Map<String, PathTree> children;

  private boolean leaf;

  public PathTree() {
    this.children = new HashMap<>();
    this.leaf = false;
  }

  public static PathTree of(Iterable<String> patterns) {
    PathTree pathTree = new PathTree();
    if (patterns != null) {
      for (String pattern : patterns) {
        pathTree.put(Arrays.stream(pattern.split("\\.")).iterator());
      }
    }
    return pathTree;
  }

  public boolean match(ListIterator<String> nodes) {
    if (!nodes.hasNext()) {
      return leaf;
    }
    String node = nodes.next();
    try {
      if (children.containsKey(node)) {
        if (children.get(node).match(nodes)) {
          return true;
        }
      }
      if (children.containsKey(null)) {
        if (this.match(nodes)) {
          return true;
        }
        if (children.get(null).match(nodes)) {
          return true;
        }
      }
    } finally {
      nodes.previous();
    }
    return false;
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

  public void put(PathTree anotherTree) {
    if (anotherTree.leaf) {
      leaf = true;
    }

    for (Map.Entry<String, PathTree> child : anotherTree.children.entrySet()) {
      put(child.getKey(), child.getValue());
    }
  }

  public void put(String node, PathTree anotherTree) {
    children.computeIfAbsent(node, k -> new PathTree()).put(anotherTree);
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

  public void project(Collection<String> allNodes) {
    PathTree subTree = children.remove(null);
    children.keySet().retainAll(allNodes);
    if (subTree != null) {
      PathTree wildcardSubTree = new PathTree();
      wildcardSubTree.children.put(null, subTree);

      for (String node : allNodes) {
        PathTree child = children.computeIfAbsent(node, k -> new PathTree());
        child.put(subTree);
        child.put(wildcardSubTree);
      }
    }
  }
}
