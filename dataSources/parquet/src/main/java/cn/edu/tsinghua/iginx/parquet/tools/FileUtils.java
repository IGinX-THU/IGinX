package cn.edu.tsinghua.iginx.parquet.tools;

import java.io.File;

public class FileUtils {

  public static boolean deleteFile(File file) {
    if (!file.exists()) {
      return false;
    }
    if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files != null) {
        for (File f : files) {
          deleteFile(f);
        }
      }
    }
    return file.delete();
  }

  public static String getLastDirName(String path) {
    String separator = System.getProperty("file.separator");
    if (!path.contains(separator)) {
      return path;
    } else if (path.endsWith(separator)) {
      String str = path.substring(0, path.lastIndexOf(separator));
      return str.substring(str.lastIndexOf(separator) + 1);
    } else {
      return path.substring(path.lastIndexOf(separator) + 1);
    }
  }
}
