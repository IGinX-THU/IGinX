package cn.edu.tsinghua.iginx.mongodb.tools;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.ColumnKeyTranslator;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Escaper;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class NameUtils {

  private static final char NAME_SEPARATOR = '/';

  private static final ColumnKeyTranslator COLUMN_KEY_TRANSLATOR =
      new ColumnKeyTranslator(',', '=', getEscaper());

  private static Escaper getEscaper() {
    Map<Character, Character> replacementMap = new HashMap<>();
    replacementMap.put('\\', '\\');
    replacementMap.put(',', ',');
    replacementMap.put('=', '=');
    replacementMap.put('$', '!');
    replacementMap.put('\0', 'b');
    return new Escaper('\\', replacementMap);
  }

  public static String getCollectionName(Field field) {
    ColumnKey columnKey = new ColumnKey(field.getName(), field.getTags());
    String escapedName = COLUMN_KEY_TRANSLATOR.translate(columnKey);
    return NAME_SEPARATOR + escapedName + NAME_SEPARATOR + field.getType().name();
  }

  public static Field parseCollectionName(String collectionName) throws ParseException {
    int lastSepIndex = collectionName.lastIndexOf(NAME_SEPARATOR);
    if (collectionName.length() < 2) {
      throw new ParseException("Invalid length!", 0);
    }
    if (lastSepIndex == -1) {
      throw new ParseException("Missing separator!", 0);
    }
    if (lastSepIndex == 0) {
      throw new ParseException("Missing last separator!", 0);
    }
    if (collectionName.charAt(0) != NAME_SEPARATOR) {
      throw new IllegalArgumentException("Invalid prefix!");
    }
    String typeName = collectionName.substring(lastSepIndex + 1);
    DataType type = DataType.valueOf(typeName);
    String name = collectionName.substring(1, lastSepIndex);
    ColumnKey columnKey = COLUMN_KEY_TRANSLATOR.translate(name);
    return new Field(columnKey.getPath(), type, columnKey.getTags());
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
