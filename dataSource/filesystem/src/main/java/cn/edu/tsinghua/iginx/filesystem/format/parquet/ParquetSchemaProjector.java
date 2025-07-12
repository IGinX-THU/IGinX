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
package cn.edu.tsinghua.iginx.filesystem.format.parquet;

import cn.edu.tsinghua.iginx.filesystem.common.IginxPaths;
import cn.edu.tsinghua.iginx.filesystem.common.Patterns;
import com.google.common.collect.Streams;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import shaded.iginx.org.apache.parquet.schema.GroupType;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.Type;

public class ParquetSchemaProjector {

  public static MessageType project(MessageType schema, Iterable<List<String>> paths) {
    List<Type> fields = project((GroupType) schema, paths).getFields();
    return new MessageType(schema.getName(), fields);
  }

  public static GroupType project(GroupType schema, Iterable<List<String>> paths) {
    Map<String, Collection<List<String>>> groupedPaths =
        Streams.stream(paths)
            .collect(
                Collectors.groupingBy(
                    path -> path.get(0),
                    Collectors.mapping(
                        path -> path.subList(1, path.size()),
                        Collectors.toCollection(ArrayList::new))));
    List<Type> types = new ArrayList<>();
    for (Map.Entry<String, Collection<List<String>>> entry : groupedPaths.entrySet()) {
      String fieldName = entry.getKey();
      Collection<List<String>> subPaths = entry.getValue();
      Type subType = schema.getType(fieldName);
      if (subType.isPrimitive()) {
        types.add(subType);
      } else {
        GroupType subGroupType = project(subType.asGroupType(), subPaths);
        types.add(subGroupType);
      }
    }
    return schema.withNewFields(types);
  }

  public static Map<String, String[]> findParquetLeafPathEachIginxPattern(
      MessageType schema, Iterable<String> patterns, @Nullable String prefix) {
    List<List<String>> patternsWithoutPrefix =
        Streams.stream(patterns)
            .flatMap(pattern -> Patterns.removePrefix(pattern, prefix).stream())
            .distinct()
            .map(IginxPaths::split)
            .map(Arrays::asList)
            .collect(Collectors.toList());
    Map<String, String[]> pathEachPattern = new HashMap<>();
    ParquetSchemaProjector projector =
        new ParquetSchemaProjector(
            (matchedIginxPath, matchedParquetPath) -> {
              pathEachPattern.put(
                  IginxPaths.join(prefix, IginxPaths.join(matchedIginxPath)),
                  matchedParquetPath.toArray(new String[0]));
            });
    projector.matchInGroup(schema, patternsWithoutPrefix);
    return pathEachPattern;
  }

  private final List<String> iginxPaths = new ArrayList<>();
  private final List<String> parquetPaths = new ArrayList<>();
  private final BiConsumer<List<String>, List<String>> onReachMatchedLeaf;

  public ParquetSchemaProjector(BiConsumer<List<String>, List<String>> onReachMatchedLeaf) {
    this.onReachMatchedLeaf = Objects.requireNonNull(onReachMatchedLeaf);
  }

  public void match(Type type, Iterable<List<String>> patternNodesList) {
    List<List<String>> subPatternNodesList = new ArrayList<>();

    // type.getName() 的字符串可能包含 “.” 字符
    String[] typeNameNodes = IginxPaths.split(type.getName());

    boolean typeIsMatched = false;
    outer:
    for (List<String> patternNodes : patternNodesList) {
      for (int i = 0; i < typeNameNodes.length && i < patternNodes.size(); i++) {
        String currentTypeNameNode = typeNameNodes[i];
        String currentPatternNode = patternNodes.get(i);
        if (!Patterns.match(currentPatternNode, currentTypeNameNode)) {
          continue outer;
        }
        if (Patterns.isAll(currentPatternNode)) {
          typeIsMatched = true;
          subPatternNodesList.add(patternNodes.subList(i, patternNodes.size()));
          if (i + 1 != typeNameNodes.length) { // 避免与后面添加的重复
            subPatternNodesList.add(patternNodes.subList(i + 1, patternNodes.size()));
          }
        }
      }
      // typeName 的所有节点都匹配上了 patternNodes 的前缀
      if (typeNameNodes.length <= patternNodes.size()) {
        typeIsMatched = true;
        subPatternNodesList.add(patternNodes.subList(typeNameNodes.length, patternNodes.size()));
      }
    }
    if (!typeIsMatched) {
      return;
    }

    iginxPaths.add(type.getName());
    parquetPaths.add(type.getName());
    try {
      switch (type.getRepetition()) {
        case REQUIRED:
        case OPTIONAL:
          matchDifferentType(type, subPatternNodesList);
          break;
        case REPEATED:
          matchRepetition(type, subPatternNodesList);
          break;
      }
    } finally {
      parquetPaths.remove(parquetPaths.size() - 1);
      iginxPaths.remove(iginxPaths.size() - 1);
    }
  }

  private void matchRepetition(Type type, Iterable<List<String>> patternNodesList) {
    for (List<String> patternNodes : patternNodesList) {
      if (patternNodes.isEmpty()) {
        continue;
      }
      String currentNode = patternNodes.get(0);
      boolean patternIsAll = Patterns.isAll(currentNode);
      if (patternIsAll || StringUtils.isNumeric(currentNode)) {
        List<List<String>> subPatternNodesList = new ArrayList<>();
        subPatternNodesList.add(patternNodes.subList(1, patternNodes.size()));
        if (patternIsAll) {
          subPatternNodesList.add(patternNodes.subList(0, patternNodes.size()));
        }
        iginxPaths.add(currentNode);
        try {
          matchDifferentType(type, subPatternNodesList);
        } finally {
          iginxPaths.remove(iginxPaths.size() - 1);
        }
      }
    }
  }

  private void matchDifferentType(Type type, Iterable<List<String>> patternNodesList) {
    if (type.isPrimitive()) {
      for (List<String> patternNodes : patternNodesList) {
        if (patternNodes.isEmpty()) {
          onReachMatchedLeaf.accept(iginxPaths, parquetPaths);
        }
      }
    } else {
      matchInGroup(type.asGroupType(), patternNodesList);
    }
  }

  public void matchInGroup(GroupType groupType, Iterable<List<String>> patternNodesList) {
    for (Type type : groupType.getFields()) {
      match(type, patternNodesList);
    }
  }
}
