package cn.edu.tsinghua.iginx.postgresql.tools;

import static cn.edu.tsinghua.iginx.postgresql.tools.Constants.POSTGRESQL_SEPARATOR;

import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;

public class TagKVUtils {

  public static Pair<String, Map<String, String>> splitFullName(String fullName) {
    if (fullName.indexOf(POSTGRESQL_SEPARATOR) == -1) {
      return new Pair<>(fullName, null);
    }

    String[] parts = fullName.split("\\" + POSTGRESQL_SEPARATOR);
    String name = parts[0];

    Map<String, String> tags = new HashMap<>();
    for (int i = 1; i < parts.length; i++) {
      if (i % 2 != 0) {
        continue;
      }
      String tagKey = parts[i - 1];
      String tagValue = parts[i];
      tags.put(tagKey, tagValue);
    }
    return new Pair<>(name, tags);
  }

  public static String toFullName(String name, Map<String, String> tags) {
    if (tags != null && !tags.isEmpty()) {
      TreeMap<String, String> sortedTags = new TreeMap<>(tags);
      StringBuilder pathBuilder = new StringBuilder();
      sortedTags.forEach(
          (tagKey, tagValue) ->
              pathBuilder
                  .append(POSTGRESQL_SEPARATOR)
                  .append(tagKey)
                  .append(POSTGRESQL_SEPARATOR)
                  .append(tagValue));
      name += pathBuilder.toString();
    }
    return name;
  }
}
