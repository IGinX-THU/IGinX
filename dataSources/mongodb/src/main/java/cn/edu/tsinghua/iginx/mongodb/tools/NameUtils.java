package cn.edu.tsinghua.iginx.mongodb.tools;

import java.util.*;
import java.util.stream.Collectors;

public class NameUtils {

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
