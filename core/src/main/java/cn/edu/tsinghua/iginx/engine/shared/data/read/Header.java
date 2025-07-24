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
package cn.edu.tsinghua.iginx.engine.shared.data.read;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.KEY;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.RESERVED_COLS;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.*;
import java.util.regex.Pattern;

public final class Header {

  public static final Header EMPTY_HEADER = new Header(Collections.emptyList());

  public static final Header SHOW_COLUMNS_HEADER =
      new Header(
          Arrays.asList(new Field("Path", DataType.BINARY), new Field("Type", DataType.BINARY)));

  private final Field key;

  private final List<Field> fields;

  private final Map<String, Integer> indexMap;

  private final Map<String, List<Integer>> patternIndexCache;

  private List<DataType> types = null;

  public Header(List<Field> fields) {
    this(null, fields);
  }

  public Header(Field key, List<Field> fields) {
    this.key = key;
    this.fields = fields;
    this.indexMap = new HashMap<>();
    for (int i = 0; i < fields.size(); i++) {
      this.indexMap.put(fields.get(i).getFullName(), i);
    }
    this.patternIndexCache = new HashMap<>();
  }

  public Field getKey() {
    return key;
  }

  public List<Field> getFields() {
    return fields;
  }

  public Field getField(int index) {
    return fields.get(index);
  }

  public Field getFieldByName(String name) {
    int index = indexMap.getOrDefault(name, -1);
    if (index == -1) {
      return null;
    }
    return fields.get(index);
  }

  public int getFieldSize() {
    return fields.size();
  }

  public boolean hasKey() {
    return key != null;
  }

  public int indexOf(Field field) {
    String name = field.getFullName();
    int index = indexMap.getOrDefault(name, -1);
    if (index == -1) {
      return -1;
    }
    Field targetField = fields.get(index);
    if (targetField.equals(field)) {
      return index;
    } else {
      return -1;
    }
  }

  public int indexOf(String name) {
    return indexMap.getOrDefault(name, -1);
  }

  public List<Integer> patternIndexOf(String pattern) {
    if (patternIndexCache.containsKey(pattern)) {
      return patternIndexCache.get(pattern);
    } else {
      List<Integer> indexList = new ArrayList<>();
      fields.forEach(
          field -> {
            if (Pattern.matches(StringUtils.reformatPath(pattern), field.getName())) {
              indexList.add(indexOf(field.getFullName()));
            }
          });
      patternIndexCache.put(pattern, indexList);
      return indexList;
    }
  }

  public List<DataType> getDataTypes() {
    if (types != null) {
      return types;
    }
    types = new ArrayList<>();
    fields.forEach(field -> types.add(field.getType()));
    return types;
  }

  @Override
  public String toString() {
    return "Header{" + "key=" + key + ", fields=" + fields + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Header header = (Header) o;
    return Objects.equals(key, header.key)
        && Objects.equals(fields, header.fields)
        && Objects.equals(indexMap, header.indexMap);
  }

  /**
   * 根据Project算子的patterns和isRemainKey计算投影后的header
   *
   * @param patterns Project算子参数
   * @param isRemainKey Project算子参数
   * @return 投影后的header
   */
  public Header projectedHeader(List<String> patterns, boolean isRemainKey) {
    List<Field> targetFields = new ArrayList<>();
    for (Field field : fields) {
      if (isRemainKey && field.getName().endsWith("." + KEY)) {
        targetFields.add(field);
        continue;
      }
      for (String pattern : patterns) {
        if (!StringUtils.isPattern(pattern)) {
          if (pattern.equals(field.getName())) {
            targetFields.add(field);
          }
        } else {
          if (Pattern.matches(StringUtils.reformatPath(pattern), field.getName())) {
            targetFields.add(field);
          }
        }
      }
    }
    return new Header(key, targetFields);
  }

  /**
   * 根据Rename算子的aliasList和ignorePatterns计算重命名后的header和要升级成key列的普通列的下标
   *
   * @param aliasList Rename算子参数
   * @param ignorePatterns Rename算子参数
   * @return pair.k表示重命名后的header;pair.v表示要升级成key列的普通列的下标,为-1时表示没有列需要升级成key列
   */
  public Pair<Header, Integer> renamedHeader(
      List<Pair<String, String>> aliasList, List<String> ignorePatterns) throws PhysicalException {
    List<Field> newFields = new ArrayList<>();
    int size = getFieldSize();
    int colIndex = -1;
    scanFields:
    for (int i = 0; i < size; i++) {
      Field field = fields.get(i);
      // 如果列名在ignorePatterns中，对该列不执行rename
      boolean ignore = false;
      for (String ignorePattern : ignorePatterns) {
        if (StringUtils.match(field.getName(), ignorePattern)) {
          newFields.add(field);
          ignore = true;
          break;
        }
      }
      if (ignore) {
        continue;
      }
      String alias = "";
      for (Pair<String, String> pair : aliasList) {
        String oldPattern = pair.k;
        String newPattern = pair.v;
        if (oldPattern.equals("*") && newPattern.endsWith(".*")) {
          String newPrefix = newPattern.substring(0, newPattern.length() - 1);
          alias = newPrefix + field.getName();
        } else if (oldPattern.endsWith(".*") && newPattern.endsWith(".*")) {
          String oldPrefix = oldPattern.substring(0, oldPattern.length() - 1);
          String newPrefix = newPattern.substring(0, newPattern.length() - 1);
          if (field.getName().startsWith(oldPrefix)) {
            alias = field.getName().replaceFirst(oldPrefix, newPrefix);
          }
          break;
        } else if (oldPattern.equals(field.getName())) {
          if (newPattern.equals(Constants.KEY)) {
            if (colIndex != -1) {
              throw new PhysicalTaskExecuteFailureException(
                  "only one column can transform to key in each select");
            }
            colIndex = i;
            continue scanFields;
          }
          alias = newPattern;
          Set<Map<String, String>> tagSet = new HashSet<>();
          Field nextField = i < size - 1 ? fields.get(i + 1) : null;
          tagSet.add(field.getTags());
          // 处理同一列但不同tag的情况
          while (nextField != null
              && oldPattern.equals(nextField.getName())
              && !tagSet.contains(nextField.getTags())) {
            newFields.add(new Field(alias, field.getType(), field.getTags()));
            field = nextField;
            i++;
            nextField = i < size - 1 ? fields.get(i + 1) : null;
            tagSet.add(field.getTags());
          }
          aliasList.remove(pair);
          break;
        } else {
          if (StringUtils.match(field.getName(), oldPattern)) {
            if (newPattern.endsWith("." + oldPattern)) {
              String prefix = newPattern.substring(0, newPattern.length() - oldPattern.length());
              alias = prefix + field.getName();
            } else {
              alias = newPattern;
            }
            break;
          }
        }
      }
      if (alias.isEmpty()) {
        newFields.add(field);
      } else {
        newFields.add(new Field(alias, field.getType(), field.getTags()));
      }
    }

    Header newHeader;
    if (!hasKey() && colIndex != -1) {
      newHeader = new Header(Field.KEY, newFields);
    } else {
      newHeader = new Header(getKey(), newFields);
    }
    return new Pair<>(newHeader, colIndex);
  }

  public static class ReorderedHeaderWrapped {
    Header header;
    List<Field> targetFields;
    Map<Integer, Integer> reorderMap;

    public ReorderedHeaderWrapped(
        Header header, List<Field> targetFields, Map<Integer, Integer> reorderMap) {
      this.header = header;
      this.targetFields = targetFields;
      this.reorderMap = reorderMap;
    }

    public Header getHeader() {
      return header;
    }

    public List<Field> getTargetFields() {
      return targetFields;
    }

    public Map<Integer, Integer> getReorderMap() {
      return reorderMap;
    }
  }

  /**
   * 获取重新排序后的header和辅助结果，以排序数据本体
   *
   * @param patterns 需要保留的列名或列名模式
   * @param isPyUDFList 指示每列是否是udf返回的，是则不排序
   * @return 排序后的ReorderedHeaderWrapped类，包含header（排序后）、targetFields（保留的列的列表）、reorderMap（保留列新索引：旧索引）
   */
  public ReorderedHeaderWrapped reorderedHeaderWrapped(
      List<String> patterns, List<Boolean> isPyUDFList) {
    List<Field> targetFields = new ArrayList<>();
    Map<Integer, Integer> reorderMap = new HashMap<>();

    // 保留关键字列
    for (int i = 0; i < fields.size(); i++) {
      Field field = getField(i);
      if (RESERVED_COLS.contains(field.getName())) {
        reorderMap.put(targetFields.size(), i);
        targetFields.add(field);
      }
    }

    for (int index = 0; index < patterns.size(); index++) {
      String pattern = patterns.get(index);
      List<Pair<Field, Integer>> matchedFields = new ArrayList<>();
      if (StringUtils.isPattern(pattern)) {
        for (int i = 0; i < fields.size(); i++) {
          Field field = getField(i);
          if (StringUtils.match(field.getName(), pattern)) {
            matchedFields.add(new Pair<>(field, i));
          }
        }
      } else {
        for (int i = 0; i < fields.size(); i++) {
          Field field = getField(i);
          if (pattern.equals(field.getName())) {
            matchedFields.add(new Pair<>(field, i));
          }
        }
      }
      if (!matchedFields.isEmpty()) {
        // 不对同一个UDF里返回的多列进行重新排序
        if (!isPyUDFList.get(index)) {
          matchedFields.sort(Comparator.comparing(pair -> pair.getK().getFullName()));
        }
        matchedFields.forEach(
            pair -> {
              reorderMap.put(targetFields.size(), pair.getV());
              targetFields.add(pair.getK());
            });
      }
    }

    return new ReorderedHeaderWrapped(new Header(getKey(), targetFields), targetFields, reorderMap);
  }

  /**
   * 获取重新排序后的header
   *
   * @param patterns 需要保留的列名或列名模式
   * @param isPyUDFList 指示每列是否是udf返回的，是则不排序
   * @return 排序后的header
   */
  public Header reorderedHeader(List<String> patterns, List<Boolean> isPyUDFList) {
    return reorderedHeaderWrapped(patterns, isPyUDFList).getHeader();
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, fields, indexMap);
  }
}
