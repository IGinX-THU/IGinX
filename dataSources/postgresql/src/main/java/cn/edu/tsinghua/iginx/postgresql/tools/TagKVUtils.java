package cn.edu.tsinghua.iginx.postgresql.tools;

import static cn.edu.tsinghua.iginx.postgresql.tools.Constants.TAGKV_EQUAL;
import static cn.edu.tsinghua.iginx.postgresql.tools.Constants.TAGKV_SEPARATOR;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.ColumnKeyTranslator;
import cn.edu.tsinghua.iginx.utils.Escaper;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TagKVUtils {

  private static final ColumnKeyTranslator COLUMN_KEY_TRANSLATOR =
      new ColumnKeyTranslator(TAGKV_SEPARATOR, TAGKV_EQUAL, getEscaper());

  private static Escaper getEscaper() {
    Map<Character, Character> replacementMap = new HashMap<>();
    replacementMap.put('\\', '\\');
    replacementMap.put(TAGKV_SEPARATOR, TAGKV_SEPARATOR);
    replacementMap.put(TAGKV_EQUAL, TAGKV_EQUAL);
    return new Escaper('\\', replacementMap);
  }

  public static Pair<String, Map<String, String>> splitFullName(String fullName) {
    try {
      ColumnKey columnKey = COLUMN_KEY_TRANSLATOR.translate(fullName);
      return new Pair<>(columnKey.getPath(), columnKey.getTags());
    } catch (ParseException e) {
      throw new IllegalStateException("Failed to parse column key", e);
    }
  }

  public static String toFullName(String name, Map<String, String> tags) {
    if (tags == null) {
      tags = Collections.emptyMap();
    }
    return COLUMN_KEY_TRANSLATOR.translate(new ColumnKey(name, tags));
  }
}
