package cn.edu.tsinghua.iginx.mongodb.dummy;

class NameUtils {
  static String getSuffix(String path, String prefix) {
    if (path.startsWith(prefix + ".")) {
      return path.substring(prefix.length() + 1);
    }
    throw new IllegalArgumentException(prefix + " is not prefix of " + path);
  }

  static boolean containNumberNode(String path) {
    for (String node : path.split("\\.")) {
      if (node.chars().allMatch(Character::isDigit)) {
        return true;
      }
    }
    return false;
  }
}
