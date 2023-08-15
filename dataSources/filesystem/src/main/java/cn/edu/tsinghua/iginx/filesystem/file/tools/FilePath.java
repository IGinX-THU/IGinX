package cn.edu.tsinghua.iginx.filesystem.file.tools;

import java.io.File;

// 给出时序列，转换为文件系统的路径
public final class FilePath {
  private static String SEPARATOR = System.getProperty("file.separator");
  private String oriPath;
  private String fileName;
  public static String FILE_EXTENSION = ".iginx";
  public static String WILDCARD = "*";
  private static String FILE_PATH_FORMAT = "%s%s" + SEPARATOR + "%s.iginx";
  private static String DIR_PATH_FORMAT = "%s%s" + SEPARATOR + "%s" + SEPARATOR;

  public FilePath(String oriPath) {
    this.oriPath = oriPath;
    this.fileName = getFileNameFormPath(oriPath);
  }

  public static String toIginxPath(String root, String storageUnit, String series) {
    if (series == null && storageUnit == null || storageUnit.equals(WILDCARD)) {
      return root;
    }
    // 之后根据规则修改获取文件名的方法， may fix it
    if (series == null && storageUnit != null) {
      return String.format(DIR_PATH_FORMAT, root, storageUnit, "");
    }
    if (series.equals(WILDCARD)) {
      return String.format(FILE_PATH_FORMAT, root, storageUnit, WILDCARD);
    }
    String middlePath = series.substring(0, series.lastIndexOf("."));
    return String.format(
        FILE_PATH_FORMAT,
        root,
        storageUnit,
        middlePath.replace(".", SEPARATOR) + SEPARATOR + getFileNameFormPath(series));
  }

  public static String toNormalFilePath(String root, String series) {
    if (series == null) return root;
    return root + series.replace(".", SEPARATOR);
  }

  public static String getFileNameFormPath(String series) {
    return series.substring(series.lastIndexOf(".") + 1);
  }

  public static String convertAbsolutePathToPath(
      String root, String filePath, String fileName, String storageUnit) {
    String tmp;
    if (storageUnit != null) {
      if (storageUnit.equals(WILDCARD)) {
        tmp =
            filePath.substring(
                filePath.indexOf(SEPARATOR, filePath.indexOf(root) + root.length() + 1) + 1);
      } else {
        tmp = filePath.substring(filePath.indexOf(storageUnit) + storageUnit.length() + 1);
      }
    } else tmp = filePath.substring(filePath.indexOf(root) + root.length());
    if (tmp.isEmpty()) return SEPARATOR;
    if (tmp.contains(".iginx")) {
      tmp = tmp.substring(0, tmp.lastIndexOf(".iginx"));
    }

    return tmp.replace(SEPARATOR, ".");
  }

  public static String getRootFromArg(String argRoot) {
    return new File(argRoot).getAbsolutePath() + SEPARATOR;
  }
}
