package cn.edu.tsinghua.iginx.utils;

import cn.edu.tsinghua.iginx.exceptions.IginxRuntimeException;
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
          Files.createFile(Paths.get(file.getPath()));
          fos = new FileOutputStream(file);
        } else {
          fos = new FileOutputStream(file, true);
        }

        for (List<byte[]> value : values) {
          fos.write(value.get(i));
        }

        fos.close();
      } catch (IOException e) {
        throw new IginxRuntimeException(
            "Encounter an error when writing file " + columns[i] + ", because " + e.getMessage(),
            e);
      }
    }
  }
}
