package cn.edu.tsinghua.iginx.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class FileUtils {

  public static void exportByteStream(List<List<byte[]>> values, String[] columns) {
    for (int i = 0; i < columns.length; i++) {
      if (columns[i].isEmpty()) {
        continue;
      }
      try {
        File file = new File(columns[i]);
        FileOutputStream fos;
        if (!file.exists()) {
          String parentPath = file.getParent();
          if (parentPath != null) {
            // recursively create dir
            if (!createDirectory(parentPath)) {
              throw new RuntimeException("can't create file: " + columns[i]);
            }
          }
          Files.createFile(Paths.get(file.getPath()));
          fos = new FileOutputStream(file);
        } else {
          fos = new FileOutputStream(file, true);
        }

        int finalI = i;
        values.forEach(
            value -> {
              try {
                fos.write(value.get(finalI));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });

        fos.close();
      } catch (IOException e) {
        throw new RuntimeException(
            "Encounter an error when writing file " + columns[i] + ", because " + e.getMessage());
      }
    }
  }

  // swap two char in string
  public static String swapChar(String str, char a, char b) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i<str.length(); i++) {
      if (str.charAt(i) == a) {
        sb.append(b);
      } else if (str.charAt(i) == b) {
        sb.append(a);
      } else {
        sb.append(str.charAt(i));
      }
    }
    return sb.toString();
  }

  // create dir recursively
  private static boolean createDirectory(String directoryPath) {
    File directory = new File(directoryPath);
    if (directory.exists()) {
      return true;
    }
    String parentPath = directory.getParent();
    if (parentPath != null) {
      if (!createDirectory(parentPath)) {
        return false;
      }
    }
    return directory.mkdir();
  }
}
