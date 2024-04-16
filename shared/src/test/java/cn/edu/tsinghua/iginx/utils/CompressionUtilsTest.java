package cn.edu.tsinghua.iginx.utils;

import static org.junit.Assert.fail;

import java.io.*;
import java.nio.ByteBuffer;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressionUtilsTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(CompressionUtilsTest.class);

  private static final String resourcePath =
      String.join(
          File.separator,
          System.getProperty("user.dir"),
          "src",
          "test",
          "resources",
          "compressionTest");

  @AfterClass
  public static void deleteTestFolder() {
    try {
      FileUtils.deleteFolder(new File(resourcePath));
    } catch (IOException e) {
      LOGGER.error("failed to delete test folder:{}.", resourcePath, e);
    }
  }

  // test compress file and decompress
  @Test
  public void testCompressFile() throws IOException {
    String testName = "compressionFileTest";
    File sourceFile = new File(String.join(File.separator, resourcePath, testName, "source"));
    File destFolder = new File(String.join(File.separator, resourcePath, testName, "dest"));

    if (!sourceFile.exists()
        && !sourceFile.getParentFile().mkdirs()
        && !sourceFile.createNewFile()) {
      fail();
    }

    FileUtils.writeFile(sourceFile, "testing...中文", "new行");
    ByteBuffer buffer = CompressionUtils.zipToByteBuffer(sourceFile);
    CompressionUtils.unzipFromByteBuffer(buffer, destFolder);

    File destFile = new File(String.join(File.separator, resourcePath, testName, "dest", "source"));
    try {
      if (!FileCompareUtils.compareFile(sourceFile, destFile)) {
        fail("unzipped file doesn't match original file");
      }
    } catch (IOException e) {
      LOGGER.error(
          "Failed to compare contents of file: {} and file: {}",
          sourceFile.getPath(),
          destFile.getPath());
    }

    FileUtils.deleteFolder(new File(String.join(File.separator, resourcePath, testName)));
  }

  // test compress folder and decompress
  @Test
  public void testCompressFolder() throws IOException {
    String testName = "compressionFolderTest";
    String sourceFolderPath = String.join(File.separator, resourcePath, testName, "source");
    String destFolderPath = String.join(File.separator, resourcePath, testName, "dest");
    File sourceFolder = new File(sourceFolderPath);
    File destFolder = new File(destFolderPath);

    File sourceFile1 = new File(String.join(File.separator, sourceFolderPath, "file1"));
    File sourceFile2 = new File(String.join(File.separator, sourceFolderPath, "folder1", "file2"));
    try {
      if (!sourceFile1.exists()
          && !sourceFile1.getParentFile().mkdirs()
          && !sourceFile1.createNewFile()) {
        fail();
      }
      if (!sourceFile2.exists()
          && !sourceFile2.getParentFile().mkdirs()
          && !sourceFile2.createNewFile()) {
        fail();
      }
    } catch (IOException e) {
      LOGGER.error("can't create file.", e);
      fail();
    }

    FileUtils.writeFile(sourceFile1, "testing...中文111", "new行11111");
    FileUtils.writeFile(sourceFile2, "testing...中文222", "new行22222");
    ByteBuffer buffer = CompressionUtils.zipToByteBuffer(sourceFolder);
    CompressionUtils.unzipFromByteBuffer(buffer, destFolder);

    if (!FileCompareUtils.compareFolder(
        sourceFolder, new File(String.join(File.separator, destFolderPath, "source")))) {
      fail("unzipped folder doesn't match original folder");
    }

    FileUtils.deleteFolder(new File(String.join(File.separator, resourcePath, testName)));
  }

  // test compress empty folder and decompress
  @Test
  public void testCompressEmptyFolder() throws IOException {
    String testName = "compressionFolderTest";
    String sourceFolderPath = String.join(File.separator, resourcePath, testName, "source");
    String destFolderPath = String.join(File.separator, resourcePath, testName, "dest");
    File sourceFolder = new File(sourceFolderPath);
    File destFolder = new File(destFolderPath);

    if (!sourceFolder.exists() && !sourceFolder.mkdirs()) {
      fail();
    }
    if (!destFolder.exists() && !destFolder.mkdirs()) {
      fail();
    }
    ByteBuffer buffer = CompressionUtils.zipToByteBuffer(sourceFolder);
    CompressionUtils.unzipFromByteBuffer(buffer, destFolder);

    if (!FileCompareUtils.compareFolder(
        sourceFolder, new File(String.join(File.separator, destFolderPath, "source")))) {
      fail("unzipped folder doesn't match original folder");
    }

    FileUtils.deleteFolder(new File(String.join(File.separator, resourcePath, testName)));
  }
}
