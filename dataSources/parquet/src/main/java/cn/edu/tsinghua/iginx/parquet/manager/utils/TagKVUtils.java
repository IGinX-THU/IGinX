package cn.edu.tsinghua.iginx.parquet.manager.utils;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.ColumnKeyTranslator;
import cn.edu.tsinghua.iginx.utils.Escaper;
import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
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
}
