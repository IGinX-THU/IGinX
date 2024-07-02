package cn.edu.tsinghua.iginx.engine.shared.data.read;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.RESERVED_COLS;

import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.*;
import java.util.regex.Pattern;

public final class Header {

  public static final Header EMPTY_HEADER = new Header(Collections.emptyList());

  private final Field key;

  private final List<Field> fields;

  private final Map<String, Integer> indexMap;

  private final Map<String, List<Integer>> patternIndexCache;

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
            if (Pattern.matches(StringUtils.reformatPath(pattern), field.getFullName())) {
              indexList.add(indexOf(field.getFullName()));
            }
          });
      patternIndexCache.put(pattern, indexList);
      return indexList;
    }
  }

  @Override
  public String toString() {
    return "Header{" + "time=" + key + ", fields=" + fields + '}';
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

  public Header renamedHeader(Map<String, String> aliasMap, List<String> ignorePatterns) {
    List<Field> newFields = new ArrayList<>();
    fields.forEach(
        field -> {
          // 如果列名在ignorePatterns中，对该列不执行rename
          for (String ignorePattern : ignorePatterns) {
            if (StringUtils.match(field.getName(), ignorePattern)) {
              newFields.add(field);
              return;
            }
          }
          String alias = "";
          for (String oldPattern : aliasMap.keySet()) {
            String newPattern = aliasMap.get(oldPattern);
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
            } else if (oldPattern.equals(field.getFullName())) {
              alias = newPattern;
              break;
            } else {
              if (StringUtils.match(field.getName(), oldPattern)) {
                if (newPattern.endsWith("." + oldPattern)) {
                  String prefix =
                      newPattern.substring(0, newPattern.length() - oldPattern.length());
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
        });
    return new Header(getKey(), newFields);
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
