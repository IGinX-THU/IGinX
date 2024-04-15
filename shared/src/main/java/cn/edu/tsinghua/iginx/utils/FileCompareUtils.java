package cn.edu.tsinghua.iginx.utils;

import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileCompareUtils {

  public static boolean compareFile(File file1, File file2) throws IOException {
    if (file1.exists() && file2.exists() && file1.isFile() && file2.isFile()) {
      try (BufferedReader br1 = new BufferedReader(new java.io.FileReader(file1));
           BufferedReader br2 = new BufferedReader(new FileReader(file2))) {
        return IOUtils.contentEqualsIgnoreEOL(br1, br2);
      }
    } else {
      throw new IOException(String.format("files not exist or are not files: %s & %s.", file1.getPath(), file2.getPath()));
    }
  }

  public static boolean compareFolder(File folder1, File folder2) throws IOException {
    if (folder1.exists() && folder1.isDirectory() && folder2.exists() && folder2.isDirectory()) {
      try (Stream<Path> stream1 = Files.walk(folder1.toPath());
           Stream<Path> stream2 = Files.walk(folder2.toPath())) {
        List<Path> list1 = stream1.collect(Collectors.toList());
        List<Path> list2 = stream2.collect(Collectors.toList());

        // Check if the two directories have the same number of files/directories
        if (list1.size() != list2.size()) {
          return false;
        }

        for (int i = 0; i < list1.size(); i++) {
          Path path1 = list1.get(i);
          Path path2 = list2.get(i);
          Path relativePath1 = folder1.toPath().relativize(path1);
          Path relativePath2 = folder2.toPath().relativize(path2);

          // Compare paths to ensure they are pointing to the same relative location
          if (!relativePath1.equals(relativePath2) ||
                  Files.isDirectory(path1) != Files.isDirectory(path2) ||
                  Files.isRegularFile(path1) != Files.isRegularFile(path2)) {
            return false;
          }

          // If both are regular files, compare their contents
          if (Files.isRegularFile(path1) && Files.isRegularFile(path2)) {
            if (!compareFile(path1.toFile(), path2.toFile())) {
              return false;
            }
          }
        }
      }
      return true;
    } else {
      throw new IOException(String.format("Dirs not exist or are not dirs: %s & %s.", folder1.getPath(), folder2.getPath()));
    }
  }
}
