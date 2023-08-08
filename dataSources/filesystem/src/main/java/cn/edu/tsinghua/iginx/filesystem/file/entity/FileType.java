package cn.edu.tsinghua.iginx.filesystem.file.entity;

import java.io.File;

public enum FileType {
  IGINX_FILE,
  NORMAL_FILE,
  UNKNOWN_FILE,
  DIR;

  public static FileType getFileType(File file) {
    String fileName = file.getName();
    if (file.isDirectory()) {
      return DIR;
    }
    if (fileName.contains(".iginx")) {
      return IGINX_FILE;
    } else if (fileName.endsWith(".txt")) {
      return NORMAL_FILE;
    } else {
      return UNKNOWN_FILE;
    }
  }
}
