package cn.edu.tsinghua.iginx.mongodb.tools;

import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.*;
import java.util.regex.Pattern;

public class NameUtils {

  private static final String NAME_SEPARATOR = "/";

  private static final String KV_SEPARATOR = "=";

  private static final String TAG_SEPARATOR = "&";

  private static final char INVALID_CHAR = '$';

  private static final char REPLACE_CHAR = '!';

  public static String getCollectionName(Field field) {
    StringJoiner tagsJoiner = new StringJoiner(TAG_SEPARATOR);
    SortedMap<String, String> sortedTags = new TreeMap<>(field.getTags());
    for (Map.Entry<String, String> tag : sortedTags.entrySet()) {
      tagsJoiner.add(tag.getKey() + KV_SEPARATOR + tag.getValue());
    }

    StringJoiner nameJoiner = new StringJoiner(NAME_SEPARATOR, NAME_SEPARATOR, NAME_SEPARATOR);
    nameJoiner.add(field.getName()).add(field.getType().toString()).add(tagsJoiner.toString());
    return nameJoiner.toString().replace(INVALID_CHAR, REPLACE_CHAR);
  }

  public static Field parseCollectionName(String collectionName) {
    String name = collectionName.replace(REPLACE_CHAR, INVALID_CHAR);
    String[] nameParts = name.split(NAME_SEPARATOR);
    String path = nameParts[1];
    DataType type = DataType.valueOf(nameParts[2]);
    Map<String, String> tags = new HashMap<>();
    if (nameParts.length >= 4) {
      for (String tagName : nameParts[3].split(TAG_SEPARATOR)) {
        String[] kv = tagName.split(KV_SEPARATOR);
        tags.put(kv[0], kv[1]);
      }
    }
    return new Field(path, type, tags);
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
