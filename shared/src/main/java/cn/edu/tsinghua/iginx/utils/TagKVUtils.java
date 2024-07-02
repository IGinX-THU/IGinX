package cn.edu.tsinghua.iginx.utils;

import java.util.*;

public class TagKVUtils {

  /**
   * 此处的 - 是非法字符，因此不会出现在tagName中，避免了重复。 z
   * 是为了调整字典序，原先使用的中文字符比ascii字符字段序大，这样带TagKV的路径字段序会比不带TagKV的路径字段序大。 但 -
   * 字典序正好位于大写字母和小写字母之间，会导致带tag的路径字典序比不带tag的路径字典序小。 从而导致SHOW COLUMNS时，路径顺序与现有测例不一致，但是内容上是一致的。
   * 因此这里加一个 z 来调整字典序，以兼容现有测例。
   */
  public static final String tagNameAnnotation = "z-"; // "tagName@";

  /** 此处的 % 是非法字符，因此不会出现在tagName中，避免了重复。 */
  public static final String tagPrefix = "%"; // "tagPrefix#";

  /**
   * 此处的 A 是合法字符，但TagKV的后缀其实并不起标识作用，我们不需要知道TagKV什么时候结束。
   * 在实际的代码中，也只是通过直接删除最后2个字符的方式来删除后缀，并且IoTDB之外的数据库似乎没有使用后缀。
   * IoTDB在最后一级含非法字符的情况下会报错，因此推测这里使用后缀的原因是用来保护IoTDB路径的合法性，使最后一级不含非法字符。
   * 例如，TagKV的值含有IGinX合法、IoTDB非法的字符 @，如果没有后缀，@ 出现在最后一级的话，IoTDB会报错。 因此在这里我们使用一个合法字符 A
   * 作为后缀，实际上起到的是一个保护作用。
   */
  public static final String tagSuffix = "A"; // "#tagSuffix";

  public static String toPhysicalPath(String name, Map<String, String> tags) {
    StringBuilder builder = new StringBuilder();
    builder.append(name);
    builder.append('.').append(tagPrefix);
    if (tags != null && !tags.isEmpty()) {
      TreeMap<String, String> sortedTags = new TreeMap<>(tags);
      sortedTags.forEach(
          (tagKey, tagValue) ->
              builder
                  .append('.')
                  .append(tagNameAnnotation)
                  .append(tagKey)
                  .append('.')
                  .append(tagValue));
    }
    builder.append('.').append(tagSuffix);
    return builder.toString();
  }

  public static String toFullName(String name, Map<String, String> tags) {
    if (tags == null || tags.isEmpty()) {
      return name;
    } else {
      StringBuilder builder = new StringBuilder();
      builder.append(name);
      builder.append('{');
      TreeMap<String, String> treeMap = new TreeMap<>(tags);

      int cnt = 0;
      for (String key : treeMap.keySet()) {
        if (cnt != 0) {
          builder.append(',');
        }
        builder.append(key);
        builder.append("=");
        builder.append(treeMap.get(key));
        cnt++;
      }
      builder.append('}');
      return builder.toString();
    }
  }

  public static Pair<String, Map<String, String>> fromFullName(String fullName) {
    int index = fullName.indexOf('{');
    if (index == -1) {
      return new Pair<>(fullName, Collections.emptyMap());
    } else {
      String name = fullName.substring(0, index);
      String[] tagKVs = fullName.substring(index + 1, fullName.length() - 1).split(",");
      Map<String, String> tags = new HashMap<>();
      for (String tagKV : tagKVs) {
        String[] KV = tagKV.split("=", 2);
        tags.put(KV[0], KV[1]);
      }
      return new Pair<>(name, tags);
    }
  }

  public static void fillNameAndTagMap(String path, StringBuilder name, Map<String, String> ret) {
    int firstBrace = path.indexOf("{");
    int lastBrace = path.indexOf("}");
    if (firstBrace == -1 || lastBrace == -1) {
      name.append(path);
      return;
    }
    name.append(path, 0, firstBrace);
    String tagLists = path.substring(firstBrace + 1, lastBrace);
    String[] splitPaths = tagLists.split(",");
    for (String tag : splitPaths) {
      int equalPos = tag.indexOf("=");
      String tagKey = tag.substring(0, equalPos);
      String tagVal = tag.substring(equalPos + 1);
      ret.put(tagKey, tagVal);
    }
  }
}
