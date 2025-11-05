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
package cn.edu.tsinghua.iginx.integration.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ClientLauncher;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.FileLoader;
import cn.edu.tsinghua.iginx.integration.tool.FileUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientIT.class);

  private static final Path DOWNLOADS_DIR_PATH = Paths.get("src", "test", "resources", "downloads");

  private static final String LARGE_IMAGE_URL =
      "https://raw.githubusercontent.com/IGinX-THU/IGinX-resources/main/iginx-python-example/largeImg/large_img.jpg";

  private static final String LARGE_CSV_URL =
      "https://github.com/IGinX-THU/IGinX-resources/raw/main/resources/bigcsv.7z";

  protected static String runningEngine;

  private String csvPath;

  ClientLauncher client;

  public ClientIT() throws IOException {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    // TODO:提交前修改
    // runningEngine = conf.getStorageType();
    runningEngine = "FileSystem";
    if (Files.notExists(DOWNLOADS_DIR_PATH)) {
      Files.createDirectories(DOWNLOADS_DIR_PATH);
    }
  }

  @Before
  public void setUp() {
    client = new ClientLauncher();
  }

  @After
  public void tearDown() {
    client.close();
  }

  @Test
  public void testRecognizeDelimiter() {
    // 检测分号在注释或引号里能否被正确识别
    client.readLine("select * /*comment;*/ from * where a=\"`;'\"; -- com;ment");
    assertTrue(
        client.expectedOutputContains("ResultSets") && !client.getResult().contains("Parse Error"));

    // 模拟用户逐行输入
    client.readLine("show -- comment");
    client.readLine("/*");
    client.readLine("comment;");
    client.readLine("*/cluster");
    client.readLine("info;");
    assertTrue(
        client.expectedOutputContains("IginX infos")
            && !client.getResult().contains("Parse Error"));

    // 检测有语句残留在 buffer 中
    client.readLine("select *");
    client.readLine("from (show columns); select a.*");
    assertTrue(
        client.expectedOutputContains("ResultSets") && !client.getResult().contains("Parse Error"));
    client.readLine("from *;");
    assertTrue(
        client.expectedOutputContains("ResultSets") && !client.getResult().contains("Parse Error"));
  }

  @Test
  public void testExpansion() {
    client.readLine("SHOW CLUSTER INFO;");
    assertTrue(client.expectedOutputContains("IginX infos"));

    // 测试!!是否会匹配上一条命令
    client.readLine("!!;");
    // 如果看到 "IginX infos"，说明 !! 被展开成了上一条命令
    assertFalse(client.getResult().contains("IginX infos"));
    // !!按照正常的字符串处理，不会进行expansion（预期行为）
    assertTrue(client.expectedOutputContains("Parse Error"));
  }

  @Test
  public void testOutfileAndInfile() throws IOException {
    // test outfile
    prepareDataForOutfile();
    testExportByteStream();
    testExportCsv();
    testUseExportedDirAsDummyStorage();
    testLoadLargeImage();
    clearData(true);

    // test infile
    testImportNormalCsv();
    testImportFileAsCsv();
    testImportLargeCsv();
    clearData(false);
  }

  private void prepareDataForOutfile() {
    client.readLine("clear data;");
    assertTrue(client.expectedOutputContains("success"));

    client.readLine("insert into test(key, s1) values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4);");
    assertTrue(client.expectedOutputContains("success"));

    client.readLine(
        "insert into test(key, s2) values (0, 0.5), (1, 1.5), (2, 2.5), (3, 3.5), (4, 4.5);");
    assertTrue(client.expectedOutputContains("success"));

    client.readLine(
        "insert into test(key, s3) values (0, true), (1, false), (2, true), (3, false), (4, true);");
    assertTrue(client.expectedOutputContains("success"));

    client.readLine(
        "insert into test(key, s4) values (0, \"aaa\"), (1, \"bbb\"), (2, \"ccc\"), (3, \"ddd\"), (4, \"eee\");");
    assertTrue(client.expectedOutputContains("success"));
  }

  private void testExportByteStream() throws IOException {
    Path dir = Paths.get("src", "test", "resources", "fileReadAndWrite", "byteStream");
    if (Files.notExists(dir)) {
      Files.createDirectories(dir);
    }
    String dirPath = dir.toAbsolutePath().toString();

    String statement = String.format("select * from test into outfile \"%s\" as stream;", dirPath);
    String expected = String.format("Successfully write 4 file(s) to directory: \"%s\".", dirPath);
    client.readLine(statement);
    assertTrue(client.expectedOutputContains(expected));

    checkFiles(dir, "test", "");
  }

  private void testExportCsv() throws IOException {
    Path dir = Paths.get("src", "test", "resources", "fileReadAndWrite", "csv");
    if (Files.notExists(dir)) {
      Files.createDirectories(dir);
    }
    Path path = Paths.get("src", "test", "resources", "fileReadAndWrite", "csv", "test.csv");
    csvPath = path.toAbsolutePath().toString();

    String statement = String.format("select * from test into outfile \"%s\" as csv;", csvPath);
    String expected = String.format("Successfully write csv file: \"%s\".", csvPath);
    client.readLine(statement);
    assertTrue(client.expectedOutputContains(expected));

    File csvFile = path.toFile();
    assertTrue(csvFile.exists());
    assertTrue(csvFile.isFile());
    assertEquals("test.csv", csvFile.getName());
    assertEquals(87, csvFile.length());
  }

  private void testUseExportedDirAsDummyStorage() throws IOException {
    Path dirExported = Paths.get("src", "test", "resources", "fileReadAndWrite", "byteStream");
    Path dirDummy = Paths.get("src", "test", "resources", "fileReadAndWrite", "byteDummy");
    if (Files.notExists(dirDummy)) {
      Files.createDirectories(dirDummy);
    }
    FileUtils.copyFiles(dirExported, dirDummy, ".ext");
    String dirDummyPath = dirDummy.toAbsolutePath().toString();
    String statement =
        String.format(
            "ADD STORAGEENGINE (\"127.0.0.1\", 6670, \"filesystem\", \"dummy_dir=%s,iginx_port=6888,has_data=true,is_read_only=true\");",
            dirDummyPath);
    client.readLine(statement);
    assertTrue(client.expectedOutputContains("success"));

    Path dir = Paths.get("src", "test", "resources", "fileReadAndWrite", "byteStreamExport");
    if (Files.notExists(dir)) {
      Files.createDirectories(dir);
    }
    String dirPath = dir.toAbsolutePath().toString();
    statement = String.format("select * from byteDummy into outfile \"%s\" as stream;", dirPath);
    String expected = String.format("Successfully write 4 file(s) to directory: \"%s\".", dirPath);
    client.readLine(statement);
    assertTrue(client.expectedOutputContains(expected));

    checkFiles(dir, "byteDummy.test", ".ext");
  }

  public void checkFiles(Path dir, String prefix, String extension) {
    File dirFile = dir.toFile();

    assertTrue(dirFile.exists());
    assertTrue(dirFile.isDirectory());
    List<String> filenames = Arrays.asList(Objects.requireNonNull(dirFile.list()));
    assertEquals(4, filenames.size());

    filenames.sort(String::compareTo);

    long[] lengths = new long[] {40, 40, 5, 15};
    for (int i = 1; i <= 4; i++) {
      String expectedFilename = String.format("%s.s%d%s", prefix, i, extension);
      assertEquals(expectedFilename, filenames.get(i - 1));
      File file = new File(Paths.get(dir.toString(), expectedFilename).toString());
      assertEquals(file.length(), lengths[i - 1]);
    }
  }

  private void testLoadLargeImage() throws IOException {
    // large image export only tested in FileSystem
    Assume.assumeTrue(runningEngine.equals("FileSystem"));

    String dirPath = DOWNLOADS_DIR_PATH.toAbsolutePath().toString();
    String path = FileUtils.downloadFile(LARGE_IMAGE_URL, dirPath, "large_img.jpg");
    FileLoader.loadFile(path);

    Path dir = Paths.get("src", "test", "resources", "fileReadAndWrite", "img_outfile");
    if (Files.notExists(dir)) {
      Files.createDirectories(dir);
    }
    dirPath = dir.toAbsolutePath().toString();
    String statement =
        String.format(
            "select large_img_jpg from downloads into outfile \"%s\" as stream;", dirPath);
    String expected = String.format("Successfully write 1 file(s) to directory: \"%s\".", dirPath);
    client.readLine(statement);
    assertTrue(client.expectedOutputContains(expected));
  }

  private void testImportNormalCsv() {
    String statement =
        String.format("LOAD DATA FROM INFILE \"%s\" AS CSV INTO t(key, d, b, c, a);", csvPath);
    client.readLine(statement);
    String expected = "Successfully write 5 record(s) to: [t.a, t.b, t.c, t.d]";
    assertTrue(client.expectedOutputContains(expected));

    statement = "SELECT * FROM t;";
    client.readLine(statement);
    expected =
        "ResultSets:\n"
            + "+---+---+---+-----+---+\n"
            + "|key|t.a|t.b|  t.c|t.d|\n"
            + "+---+---+---+-----+---+\n"
            + "|  0|aaa|0.5| true|0.0|\n"
            + "|  1|bbb|1.5|false|1.0|\n"
            + "|  2|ccc|2.5| true|2.0|\n"
            + "|  3|ddd|3.5|false|3.0|\n"
            + "|  4|eee|4.5| true|4.0|\n"
            + "+---+---+---+-----+---+\n"
            + "Total line number = 5";
    expected = expected.replace("\n", System.lineSeparator());
    assertTrue(client.expectedOutputContains(expected));
  }

  private void testImportFileAsCsv() throws IOException {
    Path source = Paths.get(csvPath);
    Path target = Paths.get("src", "test", "resources", "fileReadAndWrite", "csv", "test1");
    Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
    String targetPath = target.toAbsolutePath().toString();

    String header = "key,d m,b,[c],a";
    List<String> lines = Files.readAllLines(target);
    lines.add(0, header);
    Files.write(target, lines);

    String statement =
        String.format("LOAD DATA FROM INFILE \"%s\" AS CSV INTO t1 AT 10;", targetPath);
    client.readLine(statement);
    String expected = "Successfully write 5 record(s) to: [t1._c_, t1.a, t1.b, t1.d_m]";
    assertTrue(client.expectedOutputContains(expected));

    statement = "SELECT * FROM t1;";
    client.readLine(statement);
    expected =
        "ResultSets:\n"
            + "+---+------+----+----+------+\n"
            + "|key|t1._c_|t1.a|t1.b|t1.d_m|\n"
            + "+---+------+----+----+------+\n"
            + "| 10|  true| aaa| 0.5|   0.0|\n"
            + "| 11| false| bbb| 1.5|   1.0|\n"
            + "| 12|  true| ccc| 2.5|   2.0|\n"
            + "| 13| false| ddd| 3.5|   3.0|\n"
            + "| 14|  true| eee| 4.5|   4.0|\n"
            + "+---+------+----+----+------+\n"
            + "Total line number = 5";
    expected = expected.replace("\n", System.lineSeparator());
    assertTrue(client.expectedOutputContains(expected));
  }

  private void testImportLargeCsv() throws IOException {
    String downloadsDir = DOWNLOADS_DIR_PATH.toAbsolutePath().toString();
    String zipPath = FileUtils.downloadFile(LARGE_CSV_URL, downloadsDir, "bigcsv.7z");
    FileUtils.extract7zFile(zipPath, downloadsDir);
    String bigCsvPath = Paths.get(downloadsDir, "test_bigcsv.csv").toAbsolutePath().toString();

    String statement =
        String.format("LOAD DATA FROM INFILE \"%s\" AS CSV INTO bigcsv;", bigCsvPath);
    client.readLine(statement, 1000 * 300); // 设置5分钟超时时间
    String expected = "Successfully write 120000 record(s) to: [bigcsv.test_c0, bigcsv.test_c1,";
    assertTrue(client.expectedOutputContains(expected));

    statement = "SHOW COLUMNS bigcsv.*;";
    client.readLine(statement);
    expected = "Total line number = 100";
    assertTrue(client.expectedOutputContains(expected));

    statement = "SELECT COUNT(test_c33) FROM bigcsv;";
    client.readLine(statement);
    expected =
        "ResultSets:\n"
            + "+----------------------+\n"
            + "|count(bigcsv.test_c33)|\n"
            + "+----------------------+\n"
            + "|                120000|\n"
            + "+----------------------+\n"
            + "Total line number = 1\n";
    expected = expected.replace("\n", System.lineSeparator());
    assertTrue(client.expectedOutputContains(expected));

    statement = "SELECT test_c0, test_c99 FROM bigcsv WHERE key > 119996;";
    client.readLine(statement);
    expected =
        "ResultSets:\n"
            + "+------+--------------+---------------+\n"
            + "|   key|bigcsv.test_c0|bigcsv.test_c99|\n"
            + "+------+--------------+---------------+\n"
            + "|119997|    gHH3VRCeqV|     JwBz3cs51P|\n"
            + "|119998|    9kKtsslw5L|     ja5wByfKIu|\n"
            + "|119999|    m9DGS5q36W|     UY5geS31Nu|\n"
            + "+------+--------------+---------------+\n"
            + "Total line number = 3\n";
    expected = expected.replace("\n", System.lineSeparator());
    assertTrue(client.expectedOutputContains(expected));
  }

  private void clearData(boolean removeStorage) {
    if (removeStorage) {
      client.readLine("REMOVE STORAGEENGINE (\"127.0.0.1\", 6670, \"\", \"\");");
      assertTrue(client.expectedOutputContains("success"));
    }

    client.readLine("clear data;");
    assertTrue(client.expectedOutputContains("success"));
  }
}
