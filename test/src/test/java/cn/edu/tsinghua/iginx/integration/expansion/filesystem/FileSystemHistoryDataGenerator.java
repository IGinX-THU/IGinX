package cn.edu.tsinghua.iginx.integration.expansion.filesystem;

import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.*;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FileSystemHistoryDataGenerator.class);

  public FileSystemHistoryDataGenerator() {}

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
    Path rootPath = Paths.get(PORT_TO_ROOT.get(port));
    if (!Files.exists(rootPath)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(rootPath)) {
      walk.sorted(Comparator.reverseOrder()).forEach(this::deleteDirectoryStream);
    } catch (IOException e) {
      LOGGER.error("delete {} failure", rootPath);
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
    // └── f
    //     └── g
    //         └── 3.txt
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
  }

  private void createAndWriteFile(byte[] content, String first, String... more) {
    File file = new File(Paths.get(first, more).toString());
    try {
      if (file.exists()) {
        LOGGER.info("file {} has existed", file.getAbsolutePath());
        return;
      }
      if (!file.getParentFile().mkdirs()) {
        LOGGER.error("create directory {} failed", file.getParentFile().getAbsolutePath());
        return;
      }
      if (!file.exists() && !file.createNewFile()) {
        LOGGER.error("create file {} failed", file.getAbsolutePath());
        return;
      }
      try (FileOutputStream fos = new FileOutputStream(file)) {
        fos.write(content);
      }
    } catch (IOException e) {
      LOGGER.error("createAndWriteFile failed first {} more {}", first, more);
    }
  }
}
