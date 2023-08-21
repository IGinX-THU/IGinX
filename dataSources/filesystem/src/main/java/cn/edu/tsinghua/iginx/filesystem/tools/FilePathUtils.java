package cn.edu.tsinghua.iginx.filesystem.tools;

import static cn.edu.tsinghua.iginx.filesystem.constant.Constant.*;

import java.nio.file.Paths;

public class FilePathUtils {

  public static String toIginxPath(String root, String storageUnit, String path) {
    if (path == null && storageUnit == null || storageUnit.equals(WILDCARD)) {
      return root;
    }
    // 之后根据规则修改获取文件名的方法， may fix it
    if (path == null) {
      return Paths.get(root, storageUnit).toString();
    }
    if (path.equals(WILDCARD)) {
      return Paths.get(root, storageUnit, WILDCARD).toString();
    }
    String middlePath = path.substring(0, path.lastIndexOf("."));
    return Paths.get(
            root,
            storageUnit,
            middlePath.replace(".", SEPARATOR),
            middlePath.substring(middlePath.lastIndexOf(".") + 1) + ".iginx")
        .toString();
  }

  public static String toNormalFilePath(String root, String path) {
    if (path == null) {
      return root;
    }
    return Paths.get(root, path.replace(".", SEPARATOR)).toString();
  }

  public static String convertAbsolutePathToPath(String root, String filePath, String storageUnit) {
    String tmp;
    // 对iginx文件操作
    if (filePath.contains(FILE_EXTENSION)) {
      tmp = filePath.substring(0, filePath.lastIndexOf(FILE_EXTENSION));
      if (storageUnit != null) {
        if (storageUnit.equals(WILDCARD)) {
          tmp = tmp.substring(tmp.indexOf(SEPARATOR, tmp.indexOf(root) + root.length() + 1) + 1);
        } else {
          tmp = tmp.substring(tmp.indexOf(storageUnit) + storageUnit.length() + 1);
        }
      } else {
        tmp = tmp.substring(tmp.indexOf(root) + root.length());
      }
    } else { // 对普通文件操作
      tmp = filePath.substring(filePath.indexOf(root) + root.length());
    }
    if (tmp.isEmpty()) {
      return SEPARATOR;
    }
    return tmp.replace(SEPARATOR, ".");
  }
}
