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

import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.IGINX_DATA_PATH_PREFIX_NAME;
import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.PORT_TO_ROOT;

import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.filestore.common.FileStoreException;
import cn.edu.tsinghua.iginx.filestore.service.FileStoreConfig;
import cn.edu.tsinghua.iginx.filestore.service.storage.StorageConfig;
import cn.edu.tsinghua.iginx.filestore.service.storage.StorageService;
import cn.edu.tsinghua.iginx.filestore.test.DataViewGenerator;
import cn.edu.tsinghua.iginx.filestore.thrift.DataUnit;
import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStoreHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileStoreHistoryDataGenerator.class);

  public FileStoreHistoryDataGenerator() {}

  @Override
  public void writeHistoryData(
      int port, List<String> pathList, List<DataType> dataTypeList, List<List<Object>> valuesList) {
    List<Long> keyList = new ArrayList<>();
    for (int i = 0; i < valuesList.size(); i++) {
      keyList.add((long) i);
    }
    writeHistoryData(port, pathList, dataTypeList, keyList, valuesList);
  }

  @Override
  public void writeHistoryData(
      int port,
      List<String> pathList,
      List<DataType> dataTypeList,
      List<Long> keyList,
      List<List<Object>> valuesList) {
    LOGGER.debug("write history data {} to port {} ", pathList, port);
    if (!keyList.isEmpty()) {
      LOGGER.debug(
          "write history data with keys from {} to {}",
          keyList.get(0),
          keyList.get(keyList.size() - 1));
    }

    StorageConfig config = new StorageConfig();
    config.setRoot(PORT_TO_ROOT.get(port));
    config.setStruct(FileStoreConfig.DEFAULT_DATA_STRUCT);

    LOGGER.debug("config root {}", Paths.get(config.getRoot()).toFile().getAbsolutePath());

    DataUnit dataUnit = new DataUnit();
    dataUnit.setDummy(false);
    dataUnit.setName("test0001");

    DataView dataView =
        DataViewGenerator.genRowDataView(pathList, dataTypeList, keyList, valuesList);

    try (StorageService storageService = new StorageService(config, null)) {
      storageService.insert(dataUnit, dataView);
    } catch (FileStoreException e) {
      LOGGER.error("create storage service failure", e);
    }

    Path rootPath = Paths.get(PORT_TO_ROOT.get(port));
    LOGGER.debug("walk start from {}", rootPath.toFile().getAbsolutePath());
    try (Stream<Path> pathStream = Files.walk(rootPath)) {
      pathStream.forEach(path -> LOGGER.debug(path.toFile().getAbsolutePath()));
    } catch (IOException e) {
      LOGGER.error("walk path {} failure", rootPath, e);
    }
    LOGGER.debug("walk end");
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
      if (!rootPath.toFile().exists()) {
        continue;
      }
      try {
        MoreFiles.deleteRecursively(rootPath, RecursiveDeleteOption.ALLOW_INSECURE);
      } catch (IOException e) {
        LOGGER.error("delete {} failure", rootPath, e);
      }
    }
  }

  @Override
  public void writeSpecialHistoryData() {
    writeSpecificDirectoriesAndFiles();
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
      LOGGER.error("copy file from resource {} to {} failed", resourcePath, targetPath, e);
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
      LOGGER.error("createAndWriteFile failed first {} more {}", first, more, e);
    }
  }
}
