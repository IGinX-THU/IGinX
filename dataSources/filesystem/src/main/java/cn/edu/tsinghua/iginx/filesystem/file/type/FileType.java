package cn.edu.tsinghua.iginx.filesystem.file.type;

import static cn.edu.tsinghua.iginx.filesystem.constant.Constant.MAGIC_NUMBER;

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
    if (file.isDirectory()) {
      return DIR;
    }
    if (ifMatchMagicNumber(file)) {
      return IGINX_FILE;
    }
    return NORMAL_FILE;
  }

  private static boolean ifMatchMagicNumber(File file) throws IOException {
    FileMeta fileMeta = DefaultFileOperator.getInstance().getFileMeta(file);
    return Arrays.equals(fileMeta.getMagicNumber(), MAGIC_NUMBER);
  }
}
