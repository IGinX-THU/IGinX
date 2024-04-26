package cn.edu.tsinghua.iginx.integration.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.junit.Test;

public class ExportFileIT {

  @Test
  public void checkExportByteStream() {
    Path dir = Paths.get("src", "test", "resources", "fileReadAndWrite", "byteStream");
    checkFiles(dir, "test", "");
    dir = Paths.get("src", "test", "resources", "fileReadAndWrite", "byteStreamExport");
    checkFiles(dir, "byteDummy.test", ".ext");
  }

  public void checkFiles(Path dir, String prefix, String extension) {
    File dirFile = dir.toFile();

    assertTrue(dirFile.exists());
    assertTrue(dirFile.isDirectory());
    List<String> filenames = Arrays.asList(Objects.requireNonNull(dirFile.list()));
    assertEquals(filenames.size(), 4);

    filenames.sort(String::compareTo);

    long[] lengths = new long[] {40, 40, 5, 15};
    for (int i = 1; i <= 4; i++) {
      String expectedFilename = String.format("%s.s%d%s", prefix, i, extension);
      assertEquals(expectedFilename, filenames.get(i - 1));
      File file = new File(Paths.get(dir.toString(), expectedFilename).toString());
      assertEquals(file.length(), lengths[i - 1]);
    }
  }

  @Test
  public void checkExportCsv() {
    Path path = Paths.get("src", "test", "resources", "fileReadAndWrite", "csv", "test.csv");
    File csvFile = path.toFile();

    assertTrue(csvFile.exists());
    assertTrue(csvFile.isFile());
    assertEquals("test.csv", csvFile.getName());
    assertEquals(87, csvFile.length());
  }

  @Test
  public void checkExportImage() {
    Path dir = Paths.get("src", "test", "resources", "fileReadAndWrite", "img_outfile");
    File dirFile = dir.toFile();

    assertTrue(dirFile.exists());
    assertTrue(dirFile.isDirectory());
    List<String> filenames = Arrays.asList(Objects.requireNonNull(dirFile.list()));
    assertEquals(filenames.size(), 1);

    filenames.sort(String::compareTo);

    String expectedFilename = "downloads.large_img_jpg";
    assertEquals(expectedFilename, filenames.get(0));
    File file = new File(Paths.get(dir.toString(), expectedFilename).toString());
    assertEquals(file.length(), 2928640);
  }
}
