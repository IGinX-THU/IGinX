/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.integration.expansion.filestore;

import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.*;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.io.MoreFiles;
import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStoreHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileStoreHistoryDataGenerator.class);

  public FileStoreHistoryDataGenerator() {}

  @Override
  public void writeHistoryData(
      int port,
      List<String> pathList,
      List<DataType> dataTypeList,
      List<Long> keyList,
      List<List<Object>> valuesList) {
    // 创建并写入文件
    createFileAndWriteValues(pathList, valuesList);
    // 仅用于扩容文件系统后查询文件
    writeSpecificDirectoriesAndFiles();
  }

  @Override
  public void writeHistoryData(
      int port, List<String> pathList, List<DataType> dataTypeList, List<List<Object>> valuesList) {
    writeHistoryData(port, pathList, dataTypeList, new ArrayList<>(), valuesList);
  }

  @Override
  public void clearHistoryDataForGivenPort(int port) {
    Path rootPath;
    for (int i = 0; i < 2; i++) {
      if (i == 0) {
        rootPath = Paths.get(PORT_TO_ROOT.get(port));
      } else {
        rootPath = Paths.get(IGINX_DATA_PATH_PREFIX_NAME + PORT_TO_ROOT.get(port));
      }
      LOGGER.info("clear path {}", rootPath.toFile().getAbsolutePath());
      if (!Files.exists(rootPath)) {
        LOGGER.info("path {} does not exist", rootPath.toFile().getAbsolutePath());
        continue;
      }
      try (Stream<Path> walk = Files.walk(rootPath)) {
        walk.sorted(Comparator.reverseOrder()).forEach(this::deleteDirectoryStream);
      } catch (IOException e) {
        LOGGER.error("delete {} failure", rootPath);
      }
    }
  }

  private void createFileAndWriteValues(List<String> pathList, List<List<Object>> valuesList) {
    String separator = System.getProperty("file.separator");
    List<List<Object>> reversedValuesList = new ArrayList<>();
    for (int i = 0; i < valuesList.get(0).size(); i++) {
      reversedValuesList.add(new ArrayList<>());
    }
    for (List<Object> values : valuesList) {
      for (int i = 0; i < values.size(); i++) {
        reversedValuesList.get(i).add(values.get(i));
      }
    }
    for (int i = 0; i < pathList.size(); i++) {
      String realFilePath = pathList.get(i).replace(".", separator);
      File file = new File(realFilePath);
      file.getParentFile().mkdirs();
      LOGGER.info("create file {} success", file.getAbsolutePath());
      try (OutputStream out = Files.newOutputStream(file.toPath())) {
        for (Object value : reversedValuesList.get(i)) {
          out.write(value.toString().getBytes());
        }
      } catch (IOException e) {
        LOGGER.error("write file {} failure", file.getAbsolutePath());
      }
    }
  }

  private void deleteDirectoryStream(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      LOGGER.error("delete {} failure", path);
    }
  }

  private void writeSpecificDirectoriesAndFiles() {
    // a
    // ├── b
    // │   └── c
    // │       └── d
    // │           └── 1.txt
    // ├── e
    // │   └── 2.txt
    // ├── f
    // │   └── g
    // │       └── 3.txt
    // ├── Iris.parquet
    // ├── floatTest.parquet
    // └── other
    //     ├── MT cars.parquet
    //     └── price.parquet
    StringBuilder content1 = new StringBuilder();
    StringBuilder content2 = new StringBuilder();
    StringBuilder content3 = new StringBuilder();
    for (int i = 0; i < 26; i++) {
      content1.append('a' + i);
    }
    for (int i = 0; i < 26; i++) {
      content2.append('A' + i);
    }
    for (int i = 0; i < 26; i++) {
      content3.append(i);
    }

    createAndWriteFile(content1.toString().getBytes(), "test", "a", "b", "c", "d", "1.txt");
    createAndWriteFile(content2.toString().getBytes(), "test", "a", "e", "2.txt");
    createAndWriteFile(content3.toString().getBytes(), "test", "a", "f", "g", "3.txt");

    String parquetResourceDir = "dummy/parquet/";
    copyFileFromResource(
        parquetResourceDir + "Iris.parquet", Paths.get("test", "a", "Iris.parquet"));
    copyFileFromResource(
        parquetResourceDir + "floatTest.parquet", Paths.get("test", "a", "floatTest.parquet"));
    copyFileFromResource(
        parquetResourceDir + "MT cars.parquet", Paths.get("test", "a", "other", "MT cars.parquet"));
    copyFileFromResource(
        parquetResourceDir + "price.parquet", Paths.get("test", "a", "other", "price.parquet"));
  }

  private static void copyFileFromResource(String resourcePath, Path targetPath) {
    try {
      MoreFiles.createParentDirectories(targetPath);
    } catch (IOException e) {
      LOGGER.error("create parent directories for {} failed", targetPath);
      return;
    }
    try (InputStream is =
        FileStoreHistoryDataGenerator.class.getClassLoader().getResourceAsStream(resourcePath)) {
      if (is == null) {
        LOGGER.error("resource {} not found", resourcePath);
        return;
      }
      Files.copy(is, targetPath, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      LOGGER.error("copy file from resource {} to {} failed", resourcePath, targetPath);
    }
  }

  private void createAndWriteFile(byte[] content, String first, String... more) {
    File file = new File(Paths.get(first, more).toString());
    try {
      MoreFiles.createParentDirectories(file.toPath());
      try (FileOutputStream fos = new FileOutputStream(file)) {
        fos.write(content);
      }
    } catch (IOException e) {
      LOGGER.error("createAndWriteFile failed first {} more {}", first, more);
    }
  }
}
