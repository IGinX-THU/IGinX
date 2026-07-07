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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.metadata.field;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.metadata.field.tagkv.CompactInvertedTagsSet;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.metadata.field.tagkv.TypedCompactInvertedTagsSet;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.exception.TypeConflictedException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.Value;
import org.apache.arrow.util.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;

public class PrefixTagTreeIndex implements FieldIndex {

  private final PrefixTagNodeTree root = new PrefixTagNodeTree();
  private final boolean enableChildrenSharedTagsSet;

  protected PrefixTagTreeIndex(boolean enableChildrenSharedTagsSet) {
    this.enableChildrenSharedTagsSet = enableChildrenSharedTagsSet;
  }

  public PrefixTagTreeIndex() {
    this(true);
  }

  @Override
  public boolean contain(Field field) throws TypeConflictedException {
    try {
      NodeListField nodeListField = NodeListField.of(field);
      return root.contain(
          nodeListField.getNodes(), nodeListField.getTags(), nodeListField.getType());
    } catch (TypeConflictedException e) {
      throw new TypeConflictedException(
          field.getName() + field.getTags(), e.getType(), e.getOldType());
    }
  }

  @Override
  public Set<Field> find(List<String> patterns, @Nullable TagFilter filter) {
    List<List<String>> patternNodes = new ArrayList<>();
    for (String pattern : patterns) {
      List<String> nodes = Arrays.asList(pattern.split("\\."));
      patternNodes.add(nodes);
    }
    List<Field> fields = new ArrayList<>();
    root.find(patternNodes, filter, new ArrayList<>(), fields::add);
    return ImmutableSet.copyOf(fields);
  }

  @Override
  public void insert(Set<Field> fields) throws TypeConflictedException {
    List<NodeListField> nodeListFields =
        fields.stream().map(NodeListField::of).collect(ImmutableList.toImmutableList());
    root.add(nodeListFields, enableChildrenSharedTagsSet);
  }

  @Override
  public void remove(Set<Field> fields) throws TypeConflictedException {
    List<NodeListField> nodeListFields =
        fields.stream().map(NodeListField::of).collect(ImmutableList.toImmutableList());
    root.remove(nodeListFields);
  }

  @Override
  public void clear() {
    root.clear();
  }

  @Value
  private static class NodeListField {
    List<String> nodes;
    Map<String, String> tags;
    DataType type;

    public static NodeListField of(Field field) {
      List<String> nodes = Arrays.asList(field.getName().split("\\."));
      return new NodeListField(nodes, field.getTags(), field.getType());
    }
  }

  private static class PrefixTagNodeTree {
    private TypedCompactInvertedTagsSet pathEnd = null; // 非空表示存在以当前前缀为路径的列
    private CompactInvertedTagsSet childrenSharedTagsSet = null;
    private final Object2ObjectMap<String, PrefixTagNodeTree> children =
        new Object2ObjectOpenHashMap<>();

    private boolean isEmpty() {
      return pathEnd == null && children.isEmpty();
    }

    public boolean contain(List<String> nodes, Map<String, String> tags, DataType type)
        throws TypeConflictedException {
      if (nodes.isEmpty()) {
        if (pathEnd == null) {
          return false;
        }
        return pathEnd.contain(tags, type);
      }
      String head = nodes.get(0);
      PrefixTagNodeTree child = children.get(head);
      if (child == null) {
        return false;
      }
      if (childrenSharedTagsSet != null) {
        if (!childrenSharedTagsSet.contain(tags)) {
          return false;
        }
        tags = Collections.emptyMap();
      }
      return child.contain(nodes.subList(1, nodes.size()), tags, type);
    }

    public void find(
        List<List<String>> patternNodes,
        @Nullable TagFilter filter,
        List<String> prefix,
        Consumer<Field> fieldConsumer) {
      boolean reachLeaf = false;
      Map<String, List<List<String>>> groupedPatterns = new Object2ObjectOpenHashMap<>();
      for (List<String> pattern : patternNodes) {
        if (pattern.isEmpty()) {
          reachLeaf = true;
        } else {
          String head = pattern.get(0);
          groupedPatterns.computeIfAbsent(head, k -> new ArrayList<>()).add(pattern);
        }
      }

      if (reachLeaf) {
        if (pathEnd != null) {
          Set<Map<String, String>> tagsSet = pathEnd.find(filter);
          for (Map<String, String> tags : tagsSet) {
            Field field = new Field(String.join(".", prefix), pathEnd.getType(), tags);
            fieldConsumer.accept(field);
          }
        }
      }

      if (childrenSharedTagsSet != null) {
        Set<Map<String, String>> sharedTagsSet = childrenSharedTagsSet.find(filter);
        if (sharedTagsSet.isEmpty()) {
          return;
        }
        if (filter != null) {
          filter = null;
        }
        if (!sharedTagsSet.equals(Collections.singleton(Collections.emptyMap()))) {
          Consumer<Field> finalFieldConsumer = fieldConsumer;
          fieldConsumer =
              field -> {
                for (Map<String, String> tags : sharedTagsSet) {
                  Field newField = new Field(field.getName(), field.getType(), tags);
                  finalFieldConsumer.accept(newField);
                }
              };
        }
      }

      List<List<String>> wildcardPatterns = groupedPatterns.remove("*");

      for (Map.Entry<String, List<List<String>>> entry : groupedPatterns.entrySet()) {
        String childName = entry.getKey();
        List<List<String>> patternsForChild = entry.getValue();
        PrefixTagNodeTree child = children.get(childName);
        if (child != null) {
          List<List<String>> combinedPatterns = new ArrayList<>();
          for (List<String> pattern : patternsForChild) {
            combinedPatterns.add(pattern.subList(1, pattern.size()));
          }
          if (wildcardPatterns != null) {
            for (List<String> wildcardPattern : wildcardPatterns) {
              combinedPatterns.add(wildcardPattern);
              combinedPatterns.add(wildcardPattern.subList(1, wildcardPattern.size()));
            }
          }
          prefix.add(childName);
          child.find(combinedPatterns, filter, prefix, fieldConsumer);
          prefix.remove(prefix.size() - 1);
        }
      }

      if (wildcardPatterns != null) {
        for (Map.Entry<String, PrefixTagNodeTree> entry : children.entrySet()) {
          String childName = entry.getKey();
          PrefixTagNodeTree child = entry.getValue();
          if (groupedPatterns.containsKey(childName)) {
            continue;
          }
          List<List<String>> combinedPatterns = new ArrayList<>();
          for (List<String> wildcardPattern : wildcardPatterns) {
            combinedPatterns.add(wildcardPattern);
            combinedPatterns.add(wildcardPattern.subList(1, wildcardPattern.size()));
          }
          prefix.add(childName);
          child.find(combinedPatterns, filter, prefix, fieldConsumer);
          prefix.remove(prefix.size() - 1);
        }
      }
    }

    public void add(List<NodeListField> fields, boolean useChildrenSharedTagsSet)
        throws TypeConflictedException {
      List<NodeListField> childrenFields = new ArrayList<>();
      for (NodeListField field : fields) {
        if (field.getNodes().isEmpty()) {
          if (pathEnd == null) {
            pathEnd = new TypedCompactInvertedTagsSet(field.getType(), field.getTags());
          } else {
            pathEnd.add(field.getTags(), field.getType());
          }
        } else {
          childrenFields.add(field);
        }
      }

      if (childrenFields.isEmpty()) {
        return;
      }

      if (childrenSharedTagsSet != null || (useChildrenSharedTagsSet && children.isEmpty())) {
        Map<Map<String, String>, List<NodeListField>> fieldGroups =
            childrenFields.stream().collect(Collectors.groupingBy(NodeListField::getTags));
        if (fieldGroups.values().stream().distinct().count() == 1) {
          Map<String, List<NodeListField>> groupedChildFieldsWithoutTags =
              groupByFirstNode(fieldGroups.values().iterator().next(), false);
          if (children.isEmpty()) {
            add(groupedChildFieldsWithoutTags, false);
            fieldGroups.keySet().forEach(this::addChildrenSharedTags);
            return;
          }
          if (childrenSharedTagsSet != null
              && this.treeEqualsWithoutTags(groupedChildFieldsWithoutTags)) {
            fieldGroups.keySet().forEach(this::addChildrenSharedTags);
            return;
          }
        }
      }

      if (childrenSharedTagsSet != null) {
        Set<Map<String, String>> pushed = childrenSharedTagsSet.find(null);
        for (PrefixTagNodeTree child : children.values()) {
          child.pushDownSharedTags(pushed);
        }
        childrenSharedTagsSet = null;
      }

      Map<String, List<NodeListField>> groupedChildFields = groupByFirstNode(childrenFields, true);
      add(groupedChildFields, useChildrenSharedTagsSet);
    }

    private Map<String, List<NodeListField>> groupByFirstNode(
        List<NodeListField> fields, boolean keepTags) {
      Map<String, List<NodeListField>> grouped = new Object2ObjectOpenHashMap<>();
      for (NodeListField field : fields) {
        String childName = field.getNodes().get(0);
        Map<String, String> tags = keepTags ? field.getTags() : Collections.emptyMap();
        NodeListField childField =
            new NodeListField(
                field.getNodes().subList(1, field.getNodes().size()), tags, field.getType());
        grouped.computeIfAbsent(childName, k -> new ArrayList<>()).add(childField);
      }
      return grouped;
    }

    private void add(Map<String, List<NodeListField>> fields, boolean useChildrenSharedTagsSet)
        throws TypeConflictedException {
      for (Map.Entry<String, List<NodeListField>> entry : fields.entrySet()) {
        String childName = entry.getKey();
        List<NodeListField> childRequests = entry.getValue();
        PrefixTagNodeTree child = children.computeIfAbsent(childName, k -> new PrefixTagNodeTree());
        child.add(childRequests, useChildrenSharedTagsSet);
      }
    }

    private void pushDownSharedTags(Collection<Map<String, String>> tagsCollection) {
      Preconditions.checkArgument(tagsCollection != null && !tagsCollection.isEmpty());

      if (pathEnd != null) {
        Preconditions.checkState(pathEnd.size() == 1 && pathEnd.contain(Collections.emptyMap()));
        pathEnd = new TypedCompactInvertedTagsSet(pathEnd.getType(), tagsCollection);
      }
      if (!children.isEmpty()) {
        for (Map<String, String> tags : tagsCollection) {
          addChildrenSharedTags(tags);
        }
      }
    }

    private void addChildrenSharedTags(Map<String, String> tags) {
      if (childrenSharedTagsSet == null) {
        childrenSharedTagsSet = new CompactInvertedTagsSet(tags);
        return;
      }
      childrenSharedTagsSet.add(tags);
    }

    private boolean treeEqualsWithoutTags(Map<String, List<NodeListField>> fields) {
      if (!children.keySet().equals(fields.keySet())) {
        return false;
      }
      for (Map.Entry<String, List<NodeListField>> entry : fields.entrySet()) {
        String childName = entry.getKey();
        List<NodeListField> childFields = entry.getValue();
        PrefixTagNodeTree child = children.get(childName);
        if (child.childrenSharedTagsSet != null) {
          return false;
        }
        List<NodeListField> grandChildFields = new ArrayList<>();
        boolean reachPathEnd = false;
        for (NodeListField childField : childFields) {
          if (childField.getNodes().isEmpty()) {
            reachPathEnd = true;
            if (child.pathEnd == null) {
              return false;
            }
            if (!child.pathEnd.contain(Collections.emptyMap())) {
              return false;
            }
          } else {
            grandChildFields.add(childField);
          }
        }
        if (!reachPathEnd) {
          if (child.pathEnd != null && !child.pathEnd.isEmpty()) {
            return false;
          }
        } else {
          if (child.pathEnd.size() != 1) {
            return false;
          }
        }
        Map<String, List<NodeListField>> groupedGrandChildFields =
            child.groupByFirstNode(grandChildFields, false);
        if (!child.treeEqualsWithoutTags(groupedGrandChildFields)) {
          return false;
        }
      }
      return true;
    }

    public void remove(List<NodeListField> fields) throws TypeConflictedException {
      List<NodeListField> childrenFields = new ArrayList<>();
      for (NodeListField field : fields) {
        if (field.getNodes().isEmpty()) {
          if (pathEnd != null) {
            pathEnd.remove(field.getTags(), field.getType());
            if (pathEnd.isEmpty()) {
              pathEnd = null;
            }
          }
        } else {
          childrenFields.add(field);
        }
      }

      if (childrenFields.isEmpty()) {
        return;
      }

      if (childrenSharedTagsSet != null) {
        Map<Map<String, String>, List<NodeListField>> fieldGroups =
            childrenFields.stream().collect(Collectors.groupingBy(NodeListField::getTags));
        if (fieldGroups.values().stream().distinct().count() == 1) {
          Map<String, List<NodeListField>> groupedChildFieldsWithoutTags =
              groupByFirstNode(fieldGroups.values().iterator().next(), false);
          if (this.treeEqualsWithoutTags(groupedChildFieldsWithoutTags)) {
            fieldGroups.keySet().forEach(this::removeChildrenSharedTags);
            return;
          }
        }
        Set<Map<String, String>> pushed = childrenSharedTagsSet.find(null);
        for (PrefixTagNodeTree child : children.values()) {
          child.pushDownSharedTags(pushed);
        }
        childrenSharedTagsSet = null;
      }
      Map<String, List<NodeListField>> groupedChildFields = groupByFirstNode(childrenFields, true);
      remove(groupedChildFields);
    }

    private void removeChildrenSharedTags(Map<String, String> tags) {
      if (childrenSharedTagsSet != null) {
        childrenSharedTagsSet.remove(tags);
        if (childrenSharedTagsSet.isEmpty()) {
          children.clear();
          childrenSharedTagsSet = null;
        }
      }
    }

    private void remove(Map<String, List<NodeListField>> fields) throws TypeConflictedException {
      for (Map.Entry<String, List<NodeListField>> entry : fields.entrySet()) {
        String childName = entry.getKey();
        List<NodeListField> childRequests = entry.getValue();
        PrefixTagNodeTree child = children.get(childName);
        if (child != null) {
          child.remove(childRequests);
          if (child.isEmpty()) {
            children.remove(childName);
          }
        }
      }
    }

    public void clear() {
      pathEnd = null;
      childrenSharedTagsSet = null;
      children.clear();
    }
  }
}
