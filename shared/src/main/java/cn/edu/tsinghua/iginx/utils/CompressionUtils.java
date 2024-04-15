package cn.edu.tsinghua.iginx.utils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.zip.*;

public class CompressionUtils {
  public static ByteBuffer zipToByteBuffer(File fileOrFolder) throws IOException {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    ZipOutputStream zipStream = new ZipOutputStream(byteStream);

    compressFile(fileOrFolder, fileOrFolder.getName(), zipStream);
    zipStream.close();
    byteStream.close();

    byte[] bytes = byteStream.toByteArray();
    return ByteBuffer.wrap(bytes);
  }

  private static void compressFile(File file, String path, ZipOutputStream zipOut) throws IOException {
    if (file.isDirectory()) {
      if (path.endsWith("/")) {
        zipOut.putNextEntry(new ZipEntry(path));
        zipOut.closeEntry();
      } else {
        zipOut.putNextEntry(new ZipEntry(path + "/"));
        zipOut.closeEntry();
      }
      File[] children = file.listFiles();
      if (children != null) {
        for (File childFile : children) {
          compressFile(childFile, path + "/" + childFile.getName(), zipOut);
        }
      }
      return;
    }
    FileInputStream fis = new FileInputStream(file);
    ZipEntry zipEntry = new ZipEntry(path);
    zipOut.putNextEntry(zipEntry);
    byte[] bytes = new byte[1024];
    int length;
    while ((length = fis.read(bytes)) >= 0) {
      zipOut.write(bytes, 0, length);
    }
    fis.close();
    zipOut.closeEntry();
  }

  public static void unzipFromByteBuffer(ByteBuffer buffer, File destination) throws IOException {
    InputStream inputStream = new ByteArrayInputStream(buffer.array());
    ZipInputStream zipIn = new ZipInputStream(inputStream);

    ZipEntry entry = zipIn.getNextEntry();
    while (entry != null) {
      File fileDest = new File(destination, entry.getName());
      if (entry.isDirectory()) {
        fileDest.mkdirs();
      } else {
        fileDest.getParentFile().mkdirs();
        extractFile(zipIn, fileDest);
      }
      zipIn.closeEntry();
      entry = zipIn.getNextEntry();
    }
    zipIn.close();
    inputStream.close();
  }

  private static void extractFile(ZipInputStream zipIn, File file) throws IOException {
    if (!file.exists() && !file.createNewFile()) {
      throw new IOException("cannot create file: " + file.getPath());
    }
    BufferedOutputStream bos = new BufferedOutputStream(Files.newOutputStream(file.toPath()));
    byte[] bytesIn = new byte[1024];
    int read = 0;
    while ((read = zipIn.read(bytesIn)) != -1) {
      bos.write(bytesIn, 0, read);
    }
    bos.close();
  }
}
