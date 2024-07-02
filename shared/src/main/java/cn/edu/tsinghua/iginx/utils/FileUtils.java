package cn.edu.tsinghua.iginx.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
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

  public static void copyFileOrDir(File source, File target) throws IOException {
    Files.walkFileTree(
        source.toPath(),
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs)
              throws IOException {
            Files.createDirectories(target.toPath().resolve(source.toPath().relativize(dir)));
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs)
              throws IOException {
            Files.copy(
                file,
                target.toPath().resolve(source.toPath().relativize(file)),
                StandardCopyOption.REPLACE_EXISTING);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  public static void deleteFileOrDir(File file) throws IOException {
    if (file.isFile()) {
      if (!file.delete()) {
        throw new IOException("Failed to delete file: " + file);
      }
      return;
    }
    deleteFolder(file);
  }

  public static void deleteFolder(File folder) throws IOException {
    org.apache.commons.io.FileUtils.deleteDirectory(folder);
  }

  public static void moveFile(File sourceFile, File targetFile) throws IOException {
    org.apache.commons.io.FileUtils.moveFile(sourceFile, targetFile);
  }

  // contents will be written one per line, cover original file
  public static void writeFile(File file, String... contents) throws IOException {
    writeFileOpe(false, file, contents);
  }

  // contents will be written one per line, append to original file
  public static void appendFile(File file, String... contents) throws IOException {
    writeFileOpe(true, file, contents);
  }

  // module: 0:cover; 1:append
  private static void writeFileOpe(boolean mode, File file, String... contents) throws IOException {
    FileWriter fw = new FileWriter(file, mode);
    for (String line : contents) {
      fw.write(line);
      fw.write("\n");
    }
    fw.flush();
    fw.close();
  }
}
