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
package cn.edu.tsinghua.iginx.vectordb.support.impl;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.vectordb.support.PathSystem;
import cn.edu.tsinghua.iginx.vectordb.tools.Constants;
import cn.edu.tsinghua.iginx.vectordb.tools.NameUtils;
import cn.edu.tsinghua.iginx.vectordb.tools.TagKVUtils;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MilvusPathSystem implements PathSystem {

  private static final String END = "$$END$$";
  private static final String STAR = "*";

  private boolean inited = false;

  /** 存储所有路径 key 为完整路径，未转义，带TagKV及版本号，value 为TagKV */
  private final Map<String, Map<String, ?>> paths = new ConcurrentHashMap<>();

  /** 存储所有路径对应的列信息 key 为完整路径，未转义，带TagKV及版本号 */
  private final Map<String, Column> columns = new ConcurrentHashMap<>();

  private final String databaseName;

  /** 备份collection名称，escaped */
  //  private final Set<String> backupCollections = new ConcurrentHashSet<>();

  public MilvusPathSystem(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  // 添加路径到存储中
  public void addPath(String path, boolean isDummy, DataType type) {
    String[] parts = path.split("\\" + Constants.PATH_SEPARATOR);
    Map<String, Map<String, ?>> currentLevel = paths;
    Pair<String, Map<String, String>> pair = null;
    for (String part : parts) {
      Pair<String, String> p = NameUtils.getPathAndVersion(part);
      pair = TagKVUtils.splitFullName(p.getK());
      currentLevel =
          (Map<String, Map<String, ?>>)
              currentLevel.computeIfAbsent(pair.getK(), k -> new HashMap());
    }
    // 使用一个特殊的键来标记这是一个完整的路径
    ((Map<String, Map<String, ?>>) currentLevel.computeIfAbsent(END, k -> new HashMap()))
        .put(path, pair == null ? null : pair.getV());
    if (pair != null) {
      columns.put(
          path,
          new Column(
              TagKVUtils.splitFullName(NameUtils.getPathAndVersion(path).getK())
                  .getK()
                  .replaceAll("\\[\\[(\\d+)\\]\\]", ""),
              type,
              pair.getV(),
              isDummy));
    }
  }

  /**
   * 查找匹配给定模式的所有路径 返回所有符合条件的完整路径（含tagkv和版本号）
   *
   * @param pattern
   * @param tagFilter
   * @return
   */
  public List<String> findPaths(String pattern, TagFilter tagFilter) {
    String[] parts = pattern.split("\\" + Constants.PATH_SEPARATOR);
    return findRecursive(parts, 0, "", paths, tagFilter);
  }

  @Override
  public Column getColumn(String path) {
    return columns.get(path);
  }

  @Override
  public Map<String, Column> getColumns() {
    return columns;
  }

  private List<String> findRecursive(
      String[] parts,
      int index,
      String currentPath,
      Map<String, Map<String, ?>> currentLevel,
      TagFilter tagFilter) {
    if (index == parts.length) {
      return collectAllPaths(currentPath, currentLevel, tagFilter);
    }

    List<String> results = new ArrayList<>();
    String part = parts[index];
    if (STAR.equals(part)) {
      // 如果是通配符，则递归所有子节点
      for (Map.Entry<String, Map<String, ?>> entry : currentLevel.entrySet()) {
        String nextPart = entry.getKey();
        if (!END.equals(nextPart)) {
          results.addAll(
              findRecursive(
                  parts,
                  index + 1,
                  currentPath.isEmpty() ? nextPart : currentPath + "." + nextPart,
                  (Map<String, Map<String, ?>>) entry.getValue(),
                  tagFilter));
          results.addAll(
              findRecursive(
                  parts,
                  index,
                  currentPath.isEmpty() ? nextPart : currentPath + "." + nextPart,
                  (Map<String, Map<String, ?>>) entry.getValue(),
                  tagFilter));
        }
      }
    } else {
      // 如果是指定的部分，则仅递归匹配的子节点
      Pair<String, String> p = NameUtils.getPathAndVersion(part);
      Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(p.getK());
      Map<String, Map<String, ?>> nextLevel =
          (Map<String, Map<String, ?>>) currentLevel.get(pair.getK());
      if (nextLevel != null) {
        results.addAll(
            findRecursive(
                parts,
                index + 1,
                currentPath.isEmpty() ? part : currentPath + "." + part,
                nextLevel,
                tagFilter));
      }
    }
    return results;
  }

  private boolean isSubset(Map<?, ?> map1, Map<?, ?> map2) {
    for (Map.Entry<?, ?> entry : map2.entrySet()) {
      if (!map1.containsKey(entry.getKey())
          || !Constants.STAR.equals(entry.getValue())
              && !map1.get(entry.getKey()).equals(entry.getValue())) {
        return false;
      }
    }
    return true;
  }

  private boolean isEmpty(Map<?, ?> map) {
    return map == null || map.isEmpty();
  }

  private boolean isEqual(Map<?, ?> map1, Map<?, ?> map2) {
    if (isEmpty(map1) && isEmpty(map2)) {
      return true;
    }
    if (isEmpty(map1) || isEmpty(map2)) {
      return false;
    }
    if (map1.size() != map2.size()) {
      return false;
    }
    for (Map.Entry<?, ?> entry : map1.entrySet()) {
      if (!entry.getValue().equals(map2.get(entry.getKey()))) {
        return false;
      }
    }
    return true;
  }

  private List<String> collectAllPaths(
      String currentPath, Map<String, Map<String, ?>> currentLevel, TagFilter tagFilter) {
    List<String> allPaths = new ArrayList<>();
    if (currentLevel.containsKey(END)) {
      currentLevel
          .get(END)
          .forEach(
              (k, v) -> {
                if (tagFilter == null
                    || cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils.match(
                        (Map<String, String>) v, tagFilter)) {
                  allPaths.add(k);
                }
              });
    }
    for (Map.Entry<String, Map<String, ?>> entry : currentLevel.entrySet()) {
      String key = entry.getKey();
      if (!END.equals(key)) {
        allPaths.addAll(
            collectAllPaths(
                currentPath.isEmpty() ? key : currentPath + "." + key,
                (Map<String, Map<String, ?>>) entry.getValue(),
                tagFilter));
      }
    }
    return allPaths;
  }

  // 删除路径，支持*通配符
  public boolean deletePath(String pattern) {
    String[] parts = pattern.split("\\" + Constants.PATH_SEPARATOR);
    return deleteRecursive(parts, 0, paths);
  }

  private boolean deleteRecursive(
      String[] parts, int index, Map<String, Map<String, ?>> currentLevel) {
    if (index == parts.length) {
      // 到达路径的最后一层，删除 "END" 标记
      if (currentLevel.containsKey(END)) {
        String part = parts[index - 1];
        Pair<String, String> p = NameUtils.getPathAndVersion(part);
        Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(p.getK());
        Map<String, Map<String, String>> ps =
            (Map<String, Map<String, String>>) currentLevel.get(END);

        synchronized (MilvusPathSystem.class) {
          Iterator<Map.Entry<String, Map<String, String>>> iterator = ps.entrySet().iterator();
          while (iterator.hasNext()) {
            Map.Entry<String, Map<String, String>> entry = iterator.next();
            if (isEqual(entry.getValue(), pair.getV())) {
              iterator.remove();
            }
          }
          if (ps.size() == 0) {
            currentLevel.remove(END);
          }
        }
        // 如果当前层级变为空，返回 true 表示可以删除上一层的引用
        return currentLevel.isEmpty();
      } else {
        // 路径不存在
        return false;
      }
    }

    String part = parts[index];
    if (STAR.equals(part)) {
      // 如果是通配符，则递归所有子节点
      boolean anyDeleted = false;
      synchronized (MilvusPathSystem.class) {
        Iterator<Map.Entry<String, Map<String, ?>>> iterator = currentLevel.entrySet().iterator();
        while (iterator.hasNext()) {
          Map.Entry<String, Map<String, ?>> entry = iterator.next();
          String nextPart = entry.getKey();
          if (!END.equals(nextPart)) {
            if (deleteRecursive(parts, index + 1, (Map<String, Map<String, ?>>) entry.getValue())) {
              // 如果下一层变为空，删除当前层级的引用
              iterator.remove();
              anyDeleted = true;
            }
          }
        }
      }
      return anyDeleted && currentLevel.isEmpty();
    } else {
      // 如果是指定的部分，则仅递归匹配的子节点
      Pair<String, String> p = NameUtils.getPathAndVersion(part);
      Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(p.getK());
      Map<String, Map<String, ?>> nextLevel =
          (Map<String, Map<String, ?>>) currentLevel.get(pair.getK());
      if (nextLevel == null) {
        // 路径不存在
        return false;
      }

      // 递归删除下一层
      if (deleteRecursive(parts, index + 1, nextLevel)) {
        // 如果下一层变为空，删除当前层级的引用
        currentLevel.remove(pair.getK());
        // 返回当前层级是否也变为空
        return currentLevel.isEmpty();
      }

      return false;
    }
  }

  public String findPath(String path, Map<String, String> tags) {
    String[] parts = path.split("\\" + Constants.PATH_SEPARATOR);
    return findPathRecursive(parts, 0, "", paths, tags);
  }

  @Override
  public String findCollection(String path) {
    String[] parts = path.split("\\" + Constants.PATH_SEPARATOR);
    return findCollectionRecursive(parts, 0, "", paths);
  }

  @Override
  public boolean inited() {
    return inited;
  }

  public void setInited(boolean inited) {
    this.inited = inited;
  }

  private String findPathRecursive(
      String[] parts,
      int index,
      String currentPath,
      Map<String, Map<String, ?>> currentLevel,
      Map<String, String> tags) {
    if (index == parts.length) {
      if (currentLevel.containsKey(END)) {
        Map<String, Map<String, String>> ps =
            (Map<String, Map<String, String>>) currentLevel.get(END);
        for (Map.Entry<String, Map<String, String>> entry : ps.entrySet()) {
          //                    if (isSubset(entry.getValue(),tags)){
          if (isEqual(entry.getValue(), tags)) {
            return entry.getKey();
          }
        }
      }
    }

    if (index >= parts.length) {
      return null;
    }

    String part = parts[index];

    // 如果是指定的部分，则仅递归匹配的子节点
    Pair<String, String> p = NameUtils.getPathAndVersion(part);
    Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(p.getK());
    Map<String, Map<String, ?>> nextLevel =
        (Map<String, Map<String, ?>>) currentLevel.get(pair.getK());
    if (nextLevel != null) {
      return findPathRecursive(
          parts,
          index + 1,
          currentPath.isEmpty() ? part : currentPath + "." + part,
          nextLevel,
          tags);
    }
    return null;
  }

  private String findCollectionRecursive(
      String[] parts, int index, String currentPath, Map<String, Map<String, ?>> currentLevel) {
    if (index == parts.length) {
      for (Map.Entry<String, ?> entry : currentLevel.entrySet()) {
        if (entry.getKey().equals(END)) {
          continue;
        }
        Map<String, ?> node = (Map<String, ?>) entry.getValue();
        if (node.containsKey(END) && ((Map<String, String>) node.get(END)).size() > 0) {
          String path = ((Map<String, String>) node.get(END)).keySet().iterator().next();
          return path.substring(0, path.lastIndexOf(Constants.PATH_SEPARATOR));
        }
      }
    }

    if (index >= parts.length) {
      return null;
    }

    String part = parts[index];

    // 如果是指定的部分，则仅递归匹配的子节点
    Pair<String, String> p = NameUtils.getPathAndVersion(part);
    Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(p.getK());
    Map<String, Map<String, ?>> nextLevel =
        (Map<String, Map<String, ?>>) currentLevel.get(pair.getK());
    if (nextLevel != null) {
      return findCollectionRecursive(
          parts, index + 1, currentPath.isEmpty() ? part : currentPath + "." + part, nextLevel);
    }
    return null;
  }

  //  public Set<String> getBackupCollections() {
  //    return backupCollections;
  //  }

}
