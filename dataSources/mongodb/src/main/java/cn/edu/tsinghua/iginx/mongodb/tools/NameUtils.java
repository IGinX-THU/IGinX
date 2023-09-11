package cn.edu.tsinghua.iginx.mongodb.tools;

import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.*;
import java.util.regex.Pattern;

public class NameUtils {

  // TODO 特殊名称的替换

  private static final String NAME_SEPARATOR = "/";

  private static final String KV_SEPARATOR = "=";

  private static final String TAG_SEPARATOR = "&";

  public static String getCollectionName(Field field) {
    StringJoiner tagsJoiner = new StringJoiner(TAG_SEPARATOR);
    SortedMap<String, String> sortedTags = new TreeMap<>(field.getTags());
    for (Map.Entry<String, String> tag : sortedTags.entrySet()) {
      tagsJoiner.add(tag.getKey() + KV_SEPARATOR + tag.getValue());
    }
    return field.getName() + NAME_SEPARATOR + field.getType() + NAME_SEPARATOR + tagsJoiner;
  }

  public static Field parseCollectionName(String collectionName) {
    String[] nameParts = collectionName.split(NAME_SEPARATOR);
    String name = nameParts[0];
    DataType type = DataType.valueOf(nameParts[1]);
    Map<String, String> tags = new HashMap<>();
    if (nameParts.length >= 3) {
      for (String tagName : nameParts[2].split(TAG_SEPARATOR)) {
        String[] kv = tagName.split(KV_SEPARATOR);
        tags.put(kv[0], kv[1]);
      }
    }
    return new Field(name, type, tags);
  }

  public static List<Field> match(
      Iterable<Field> fieldList, Iterable<String> patterns, TagFilter tagFilter) {
    List<Field> fields = new ArrayList<>();
    for (Field field : fieldList) {
      if (tagFilter != null && !TagKVUtils.match(field.getTags(), tagFilter)) {
        continue;
      }
      for (String pattern : patterns) {
        if (Pattern.matches(StringUtils.reformatPath(pattern), field.getName())) {
          fields.add(field);
          break;
        }
      }
    }
    return fields;
  }

  public static boolean isWildcard(String node) {
    return node.contains("*");
  }
}
