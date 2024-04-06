package cn.edu.tsinghua.iginx.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class FileUtils {

  // decide whether a file is a text file based on a few bytes
  public static boolean isTextFile(byte[] bytes, int checkLen) {
    int pos = checkLen > 0 ? checkLen : bytes.length;
    for (int i = 0; i < pos; i++)
      // ASCII thresholds: tab, carriage, space, delete
      if (bytes[i] < 0x09 || (bytes[i] > 0x0D && bytes[i] < 0x20) || bytes[i] == 0x7F) return false;

    return true;
  }

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
            File parent = new File(parentPath);
            if (!parent.exists() && !parent.mkdirs()) {
              throw new RuntimeException("Cannot create dir: " + parentPath);
            } else if (parent.exists() && parent.isFile()) {
              throw new RuntimeException("Parent dir path " + parentPath + " is a file.");
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
}
