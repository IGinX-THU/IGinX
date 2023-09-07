package cn.edu.tsinghua.iginx.mongodb.tools;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.mongodb.immigrant.tools.Base16m;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.*;
import java.util.stream.Collectors;

public class NameUtils {

  private static final String NAME_SEPARATOR = "/";

  private static final String KV_SEPARATOR = "=";

  private static final String TAG_SEPARATOR = "&";

  public static String getFullName(Field field) {
    StringJoiner tagsJoiner = new StringJoiner(TAG_SEPARATOR);
    for (Map.Entry<String, String> tag : field.getTags().entrySet()) {
      tagsJoiner.add(tag.getKey() + KV_SEPARATOR + tag.getValue());
    }
    return field.getName() + NAME_SEPARATOR + field.getType() + NAME_SEPARATOR + tagsJoiner;
  }

  public static Field parseFullName(String collectionName) {
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

  public static String encodeTagK(String tagK) {
    return Base16m.encode(tagK);
  }

  public static String decodeTagK(String tagK) {
    return Base16m.decode(tagK);
  }

  static final ThreadLocal<Map<String, String>> tlEncodeCache = new ThreadLocal<>();

  public static String encodePath(String path) {
    if (tlEncodeCache.get() == null) {
      tlEncodeCache.set(new HashMap<>());
    }
    return tlEncodeCache.get().computeIfAbsent(path, NameUtils::toEncodePath);
  }

  private static String toEncodePath(String path) {
    String[] nodes = path.split("\\.");
    StringJoiner joiner = new StringJoiner("_");
    for (String node : nodes) {
      if (isWildcard(node)) {
        joiner.add(Base16m.encode(node).replaceAll(Base16m.encode("*"), ".*"));
      } else {
        joiner.add(Base16m.encode(node));
      }
    }
    return joiner.toString();
  }

  static final ThreadLocal<Map<String, String>> tlDecodeCache = new ThreadLocal<>();

  public static String decodePath(String path) {
    if (tlDecodeCache.get() == null) {
      tlDecodeCache.set(new HashMap<>());
    }
    return tlDecodeCache.get().computeIfAbsent(path, NameUtils::toDecodePath);
  }

  private static String toDecodePath(String path) {
    return Arrays.stream(path.split("_")).map(Base16m::decode).collect(Collectors.joining("."));
  }

  public static boolean isWildcard(String path) {
    return path.contains("*");
  }

  public static boolean isWildcardAll(String path) {
    return path.chars().allMatch(c -> c == '*' || c == '.');
  }


}
