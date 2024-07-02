package cn.edu.tsinghua.iginx.conf;

import java.io.File;

public class FileUtils {

  public static boolean isAbsolutePath(String path) {
    File file = new File(path);

    // 判断是否为绝对路径
    if (file.isAbsolute()) {
      return true;
    }

    // 如果不是绝对路径，再根据操作系统类型判断
    String os = System.getProperty("os.name").toLowerCase();
    if (os.contains("win")) {
      return path.matches("^([a-zA-Z]:\\\\|\\\\).*");
    } else {
      return path.startsWith("/");
    }
  }
}
