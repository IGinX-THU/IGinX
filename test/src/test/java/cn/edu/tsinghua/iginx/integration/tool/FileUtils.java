/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.integration.tool;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;
import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);

  private static final int HTTP_OK = 200;

  /**
   * 复制文件夹
   *
   * @param from 源文件夹路径
   * @param to 目标文件夹路径
   * @param extension 文件扩展名
   * @throws IOException 如果发生I/O错误
   */
  public static void copyFiles(Path from, Path to, String extension) throws IOException {
    try (Stream<Path> stream = Files.walk(from)) {
      stream
          .filter(Files::isRegularFile) // 只处理普通文件
          .forEach(
              source -> {
                try {
                  // 构建目标路径
                  Path dest = to.resolve(from.relativize(source));
                  // 创建目标文件的父目录
                  Path parent = dest.getParent();
                  Files.createDirectories(parent);
                  // 添加扩展名
                  String newFileName = dest.getFileName().toString() + extension;
                  dest = parent.resolve(newFileName);
                  // 复制文件
                  Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                  throw new RuntimeException("fail to copy file: " + source, e);
                }
              });
    }
  }

  /**
   * 模拟 wget 下载文件
   *
   * @param fileURL 要下载的文件的URL
   * @param saveDir 文件保存的目录
   * @param fileName 保存的文件名
   * @throws IOException 如果发生I/O错误
   * @return 下载的文件路径
   */
  public static String downloadFile(String fileURL, String saveDir, String fileName)
      throws IOException {
    Path path = Paths.get(saveDir);
    File file = new File(path.resolve(fileName).toAbsolutePath().toString());
    if (file.exists()) {
      return file.getAbsolutePath();
    }

    URL url = new URL(fileURL);
    HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
    int responseCode = httpConn.getResponseCode();
    String ret;

    if (responseCode == HTTP_OK) {
      Path savePath = Paths.get(saveDir);
      if (!Files.exists(savePath)) {
        Files.createDirectories(savePath);
      }

      try (InputStream inputStream = httpConn.getInputStream();
          FileOutputStream outputStream =
              new FileOutputStream(savePath.resolve(fileName).toFile())) {

        byte[] buffer = new byte[4096];
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) != -1) {
          outputStream.write(buffer, 0, bytesRead);
        }
      }
      ret = savePath.resolve(fileName).toAbsolutePath().toString();
      LOGGER.info("文件下载成功: {}", ret);
    } else {
      throw new IOException("No file to download. Server replied HTTP code: " + responseCode);
    }
    httpConn.disconnect();
    return ret;
  }

  /**
   * 解压 7z 文件到指定目录
   *
   * @param archivePath 7z 压缩包的路径
   * @param destinationPath 解压后文件的目标目录
   * @throws IOException 如果发生I/O错误或解压失败
   */
  public static void extract7zFile(String archivePath, String destinationPath) throws IOException {
    File destinationDir = new File(destinationPath);
    if (!destinationDir.exists() && !destinationDir.mkdirs()) {
      throw new IOException("Failed to create destination directory: " + destinationPath);
    }

    try (SevenZFile sevenZFile = new SevenZFile(new File(archivePath))) {
      SevenZArchiveEntry entry;
      while ((entry = sevenZFile.getNextEntry()) != null) {
        // normalize() 会移除路径中的 ".." 和 "."，并处理不同操作系统的分隔符。
        String normalizedPath = FilenameUtils.normalize(entry.getName());
        if (normalizedPath == null) {
          throw new IOException(
              "Malicious path detected in archive, attempting path traversal: " + entry.getName());
        }

        File destinationFile = new File(destinationDir, normalizedPath);

        if (entry.isDirectory()) {
          if (!destinationFile.exists() && !destinationFile.mkdirs()) {
            throw new IOException("Failed to create directory within archive: " + destinationFile);
          }
        } else {
          File parent = destinationFile.getParentFile();
          if (parent != null && !parent.exists()) {
            parent.mkdirs();
          }

          try (FileOutputStream outputStream = new FileOutputStream(destinationFile)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = sevenZFile.read(buffer)) != -1) {
              outputStream.write(buffer, 0, bytesRead);
            }
          }
          LOGGER.info("Extracted file: {}", destinationFile.getAbsolutePath());
        }
      }
    }
    LOGGER.info("7z file extracted successfully to: {}", destinationPath);
  }
}
