package cn.edu.tsinghua.iginx.integration.expansion.filesystem;

import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.*;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger logger =
      LoggerFactory.getLogger(FileSystemHistoryDataGenerator.class);

  public static String root = "storage%d/";

  public FileSystemHistoryDataGenerator() {
    setDataTypeAndValuesForFileSystem();
  }

  @Override
  public void writeHistoryData(
      int port, List<String> pathList, List<DataType> dataTypeList, List<List<Object>> valuesList) {
    // 创建文件
    List<File> files = getFileList(pathList, String.format(root, port));
    // 将数据写入
    writeValuesToFile(valuesList, files);
  }

  @Override
  public void clearHistoryDataForGivenPort(int port) {
    String rootPath = String.format(root, port);
    try (Stream<Path> walk = Files.walk(Paths.get(rootPath))) {
      walk.sorted(Comparator.reverseOrder()).forEach(this::deleteDirectoryStream);
    } catch (IOException e) {
      logger.error("delete {} failure", rootPath);
    }
  }

  private void setDataTypeAndValuesForFileSystem() {
    Constant.oriDataTypeList = Arrays.asList(DataType.BINARY, DataType.BINARY);
    Constant.expDataTypeList = Arrays.asList(DataType.BINARY, DataType.BINARY);
    Constant.readOnlyDataTypeList = Arrays.asList(DataType.BINARY, DataType.BINARY);

    byte[] oriValue = generateRandomValue(1);
    byte[] expValue = generateRandomValue(2);
    byte[] readOnlyValue = generateRandomValue(3);
    oriValuesList =
        Arrays.asList(Collections.singletonList(oriValue), Collections.singletonList(oriValue));
    expValuesList =
        Arrays.asList(Collections.singletonList(expValue), Collections.singletonList(expValue));
    expValuesList1 = Collections.singletonList(Collections.singletonList(expValue));
    expValuesList2 = Collections.singletonList(Collections.singletonList(expValue));
    readOnlyValuesList =
        Arrays.asList(
            Collections.singletonList(readOnlyValue), Collections.singletonList(readOnlyValue));
  }

  private List<File> getFileList(List<String> pathList, String root) {
    List<File> res = new ArrayList<>();
    // 创建历史文件
    String separator = System.getProperty("file.separator");
    for (String path : pathList) {
      String realFilePath = root + path.replace(".", separator);
      File file = new File(realFilePath);
      res.add(file);
      Path filePath = Paths.get(file.getPath());
      try {
        if (!Files.exists(filePath)) {
          file.getParentFile().mkdirs();
          Files.createFile(filePath);
          logger.info("create file {} success", file.getAbsolutePath());
        }
      } catch (IOException e) {
        logger.error("create file {} failure", file.getAbsolutePath());
      }
    }
    return res;
  }

  private void writeValuesToFile(List<List<Object>> valuesList, List<File> files) {
    if (valuesList.size() != files.size()) {
      throw new IllegalArgumentException("Number of values lists and files don't match");
    }

    int numFiles = files.size();
    for (int i = 0; i < numFiles; i++) {
      List<Object> values = valuesList.get(i);
      File file = files.get(i);

      try (OutputStream out = Files.newOutputStream(file.toPath(), StandardOpenOption.APPEND)) {
        for (Object value : values) {
          if (value instanceof byte[]) {
            out.write((byte[]) value);
          }
        }
      } catch (IOException e) {
        logger.error("write file {} failure", file.getAbsolutePath());
      }
    }
  }

  private void deleteDirectoryStream(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      logger.error("delete {} failure", path);
    }
  }
}
