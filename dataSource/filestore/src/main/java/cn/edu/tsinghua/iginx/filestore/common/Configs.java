package cn.edu.tsinghua.iginx.filestore.common;

import java.util.Map;

public class Configs {

  private Configs() {}

  public static Object put(Map<String, Object> map, Object value, String... path) {
    String joinedPath = String.join(".", path);
    return map.put(joinedPath, value);
  }

  public static String put(Map<String, String> map, String value, String... path) {
    String joinedPath = String.join(".", path);
    return map.put(joinedPath, value);
  }

  public static String putIfAbsent(Map<String, String> map, String value, String... path) {
    String joinedPath = String.join(".", path);
    return map.putIfAbsent(joinedPath, value);
  }
}
