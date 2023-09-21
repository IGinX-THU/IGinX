package cn.edu.tsinghua.iginx.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
        OutputStream os;
        if (!file.exists()) {
          Files.createFile(Paths.get(file.getPath()));
          os = Files.newOutputStream(file.toPath());
        } else {
          os = new FileOutputStream(file, true);
        }
        // 设置1M读写缓冲区
        os = new BufferedOutputStream(os, 1024 * 1024);

        int finalI = i;
        OutputStream finalOs = os;
        values.forEach(
            value -> {
              try {
                finalOs.write(value.get(finalI));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });

        os.flush();
        os.close();
      } catch (IOException e) {
        throw new RuntimeException(
            "Encounter an error when writing file " + columns[i] + ", because " + e.getMessage());
      }
    }
  }
}
