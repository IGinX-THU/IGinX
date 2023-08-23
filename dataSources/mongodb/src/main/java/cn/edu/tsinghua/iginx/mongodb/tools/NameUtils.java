package cn.edu.tsinghua.iginx.mongodb.tools;

import java.util.Arrays;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class NameUtils {

  public static String encodeTagK(String tagK) {
    return Base16m.encode(tagK);
  }

  public static String decodeTagK(String tagK) {
    return Base16m.decode(tagK);
  }

  public static String encodePath(String path) {
    String[] nodes = path.split("\\.");
    StringJoiner joiner = new StringJoiner("_");
    for (String node : nodes) {
      if (isWildcard(node)) {
        joiner.add(Base16m.encode(node).replaceAll(Base16m.encode("*"), "*"));
      } else {
        joiner.add(Base16m.encode(node));
      }
    }
    return joiner.toString();
  }

  public static String decodePath(String path) {
    return Arrays.stream(path.split("_")).map(Base16m::decode).collect(Collectors.joining("."));
  }

  public static boolean isWildcard(String path) {
    return path.contains("*");
  }

  public static boolean isWildcardAll(String path) {
    return path.chars().allMatch(c -> c == '*' || c == '.');
  }
}
