package cn.edu.tsinghua.iginx.mongodb.tools;

import java.util.StringJoiner;

public class NameUtils {

  public static String encodeTagK(String tagK) {
    return Base16m.encode(tagK);
  }

  public static String encodePath(String path) {
    String[] nodes = path.split("\\.");
    StringJoiner joiner = new StringJoiner("_");
    for (String node : nodes) {
      if (node.contains("*")) {
        joiner.add(node);
      } else {
        joiner.add(Base16m.encode(node));
      }
    }
    return joiner.toString();
  }
}
