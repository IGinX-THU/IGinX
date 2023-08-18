package cn.edu.tsinghua.iginx.filesystem.tools;

import java.io.File;

import static cn.edu.tsinghua.iginx.filesystem.constant.Constant.*;

public class FilePathUtils {

  public static String toIginxPath(String root, String storageUnit, String path) {
    if (path == null && storageUnit == null || storageUnit.equals(WILDCARD)) {
      return root;
    }
    // 之后根据规则修改获取文件名的方法， may fix it
    if (path == null) {
      return String.format(DIR_PATH_FORMAT, root, storageUnit, "");
    }
    if (path.equals(WILDCARD)) {
      return String.format(FILE_PATH_FORMAT, root, storageUnit, WILDCARD);
    }
    String middlePath = path.substring(0, path.lastIndexOf("."));
    return String.format(
        FILE_PATH_FORMAT,
        root,
        storageUnit,
        middlePath.replace(".", SEPARATOR) + SEPARATOR + getFileNameFormPath(path));
  }

  public static String toNormalFilePath(String root, String path) {
    if (path == null) return root;
    return root + path.replace(".", SEPARATOR);
  }

  public static String getFileNameFormPath(String path) {
    return path.substring(path.lastIndexOf(".") + 1);
  }

  public static String convertAbsolutePathToPath(
      String root, String filePath, String storageUnit) {
    String tmp;

    // 对iginx文件操作
    if (filePath.contains(FILE_EXTENSION)) {
      tmp = filePath.substring(0, filePath.lastIndexOf(FILE_EXTENSION));
      if (storageUnit != null) {
        if (storageUnit.equals(WILDCARD)) {
          tmp =
              tmp.substring(
                  tmp.indexOf(SEPARATOR, tmp.indexOf(root) + root.length() +1) + 1);
        } else {
          tmp = tmp.substring(tmp.indexOf(storageUnit) + storageUnit.length() + 1);
        }
      } else {
        tmp = tmp.substring(tmp.indexOf(root) + root.length());
      }
    } else { // 对普通文件操作
      tmp = filePath.substring(
          filePath.indexOf(root) + root.length());
    }
    if (tmp.isEmpty()) {
      return SEPARATOR;
    }
    return tmp.replace(SEPARATOR, ".");
  }

  public static String getRootFromArg(String argRoot) {
    return new File(argRoot).getAbsolutePath() + SEPARATOR;
  }
}
