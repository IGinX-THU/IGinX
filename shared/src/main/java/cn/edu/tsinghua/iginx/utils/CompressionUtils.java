package cn.edu.tsinghua.iginx.utils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.zip.*;

/** tools for file compression and decompression. */
public class CompressionUtils {
  /**
   * compress a file or a folder into ByteBuffer
   *
   * @param fileOrFolder a file or a folder to be compressed
   * @return compressed ByteBuffer
   */
  public static ByteBuffer zipToByteBuffer(File fileOrFolder) throws IOException {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    ZipOutputStream zipStream = new ZipOutputStream(byteStream);

    compressFile(fileOrFolder, fileOrFolder.getName(), zipStream);
    zipStream.close();
    byteStream.close();

    byte[] bytes = byteStream.toByteArray();
    return ByteBuffer.wrap(bytes);
  }

  private static void compressFile(File file, String path, ZipOutputStream zipOut)
      throws IOException {
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

  /**
   * decompress a file or a folder from ByteBuffer
   *
   * @param buffer ByteBuffer that holds the file content
   * @param destination a folder to place the decompressed file or folder. The file or folder uses
   *     original name. The write permission of destination must be checked before this method is
   *     called
   */
  public static void unzipFromByteBuffer(ByteBuffer buffer, File destination) throws IOException {
    assert !destination.getPath().contains("..");
    InputStream inputStream = new ByteArrayInputStream(buffer.array());
    ZipInputStream zipIn = new ZipInputStream(inputStream);

    ZipEntry entry = zipIn.getNextEntry();
    while (entry != null) {
      File fileDest = new File(destination, entry.getName());
      if (!fileDest.toPath().normalize().startsWith(destination.toPath()))
        throw new IOException("Illegal file path found during unzipping operation.");
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
