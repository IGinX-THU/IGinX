package cn.edu.tsinghua.iginx.filesystem.file.tools;

import cn.edu.tsinghua.iginx.filesystem.file.DefaultFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.entity.FileMeta;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public enum FileType {
  IGINX_FILE,
  NORMAL_FILE,
  UNKNOWN_FILE,
  DIR;

  public static FileType getFileType(File file) throws IOException {
    String fileName = file.getName();
    if (file.isDirectory()) {
      return DIR;
    }
    if (fileName.contains(".iginx") && ifMatchMagicNumber(file)) {
      return IGINX_FILE;
    } else if (fileName.endsWith(".txt")) {
      return NORMAL_FILE;
    } else {
      return UNKNOWN_FILE;
    }
  }

  public static boolean ifMatchMagicNumber(File file) throws IOException {
    DefaultFileOperator operator = new DefaultFileOperator();
    FileMeta fileMeta = operator.getFileMeta(file);
    return Arrays.equals(fileMeta.getMagicNumber(), FileMeta.MAGIC_NUMBER);
  }
}
