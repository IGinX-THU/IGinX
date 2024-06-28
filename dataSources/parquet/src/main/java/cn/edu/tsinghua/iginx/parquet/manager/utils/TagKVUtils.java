package cn.edu.tsinghua.iginx.parquet.manager.utils;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.ColumnKeyTranslator;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.utils.Escaper;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TagKVUtils {
  private static final ColumnKeyTranslator COLUMN_KEY_TRANSLATOR =
      new ColumnKeyTranslator(',', '=', getEscaper());

  private static Escaper getEscaper() {
    Map<Character, Character> replacementMap = new HashMap<>();
    replacementMap.put('\\', '\\');
    replacementMap.put(',', ',');
    replacementMap.put('=', '=');
    return new Escaper('\\', replacementMap);
  }

  public static String toFullName(String name, Map<String, String> tags) {
    if (tags == null) {
      tags = Collections.emptyMap();
    }
    ColumnKey columnKey = new ColumnKey(name, tags);
    return COLUMN_KEY_TRANSLATOR.translate(columnKey);
  }

  public static ColumnKey splitFullName(String fullName) {
    try {
      return COLUMN_KEY_TRANSLATOR.translate(fullName);
    } catch (ParseException e) {
      throw new IllegalStateException("Failed to parse identifier: " + fullName, e);
    }
  }

  public static boolean match(ColumnKey columnKey, List<String> patterns, TagFilter tagFilter) {
    for (String pattern : patterns) {
      if (tagFilter == null) {
        if (StringUtils.match(columnKey.getPath(), pattern)) {
          return true;
        }
      } else {
        if (StringUtils.match(columnKey.getPath(), pattern)
            && cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils.match(
                columnKey.getTags(), tagFilter)) {
          return true;
        }
      }
    }
    return false;
  }
}
