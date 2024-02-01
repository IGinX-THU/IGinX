package cn.edu.tsinghua.iginx.integration.expansion.parquet;

import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.*;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.parquet.manager.dummy.Storer;
import cn.edu.tsinghua.iginx.parquet.manager.dummy.Table;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetHistoryDataGenerator.class);

  private static final char IGINX_SEPARATOR = '.';

  public static final String IT_DATA_DIR = "IT_data";

  public static final String IT_DATA_FILENAME = "data.parquet";

  public ParquetHistoryDataGenerator() {}

  public void writeHistoryData(
      int port,
      String dir,
      String filename,
      List<String> pathList,
      List<DataType> dataTypeList,
      List<Long> keyList,
      List<List<Object>> valuesList) {
    if (!PARQUET_PARAMS.containsKey(port)) {
      LOGGER.error("writing to unknown port {}.", port);
      return;
    }

    Path dirPath = Paths.get("../" + dir);
    if (Files.notExists(dirPath)) {
      try {
        Files.createDirectories(dirPath);
      } catch (IOException e) {
        LOGGER.error("can't create data file path {}.", dir);
        return;
      }
    }

    assert pathList.size() == dataTypeList.size()
        : "pathList.size() = " + pathList.size() + ", dataTypeList.size() = " + dataTypeList.size();
    assert keyList.size() == valuesList.size()
        : "keyList.size() = " + keyList.size() + ", valuesList.size() = " + valuesList.size();

    Table table = new Table();
    for (int fieldIndex = 0; fieldIndex < pathList.size(); fieldIndex++) {
      String originalColumnName = pathList.get(fieldIndex);
      String columnName =
          originalColumnName.substring(originalColumnName.indexOf(IGINX_SEPARATOR) + 1);
      table.declareColumn(columnName, dataTypeList.get(fieldIndex));
    }

    for (int i = 0; i < valuesList.size(); i++) {
      long key = keyList.get(i);
      List<Object> rowData = valuesList.get(i);
      assert rowData.size() == pathList.size()
          : "rowData.size() = " + rowData.size() + ", pathList.size() = " + pathList.size();
      for (int fieldIndex = 0; fieldIndex < pathList.size(); fieldIndex++) {
        table.put(fieldIndex, key, rowData.get(fieldIndex));
      }
    }

    Path parquetPath = Paths.get("../" + dir, filename);
    try {
      new Storer(parquetPath).flush(table);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeHistoryData(
      int port,
      List<String> pathList,
      List<DataType> dataTypeList,
      List<Long> keyList,
      List<List<Object>> valuesList) {
    String dir = "test" + System.getProperty("file.separator") + PARQUET_PARAMS.get(port).get(0);
    String filename = PARQUET_PARAMS.get(port).get(1);
    writeHistoryData(port, dir, filename, pathList, dataTypeList, keyList, valuesList);
  }

  @Override
  public void writeHistoryData(
      int port, List<String> pathList, List<DataType> dataTypeList, List<List<Object>> valuesList) {
    List<Long> keyList =
        Stream.iterate(0L, n -> n + 1).limit(valuesList.size()).collect(Collectors.toList());
    writeHistoryData(port, pathList, dataTypeList, keyList, valuesList);
  }

  @Override
  public void clearHistoryDataForGivenPort(int port) {
    if (!PARQUET_PARAMS.containsKey(port)) {
      LOGGER.error("delete from unknown port {}.", port);
      return;
    }

    String dir = "test" + System.getProperty("file.separator") + PARQUET_PARAMS.get(port).get(0);
    String filename = PARQUET_PARAMS.get(port).get(1);
    Path parquetPath = Paths.get("../" + dir, filename);
    File file = new File(parquetPath.toString());

    if (file.exists() && file.isFile()) {
      file.delete();
    } else {
      LOGGER.warn("delete {}/{} error: does not exist or is not a file.", dir, filename);
    }

    // delete the normal IT data
    dir = IT_DATA_DIR + System.getProperty("file.separator");
    parquetPath = Paths.get("../" + dir);

    try {
      Files.walkFileTree(parquetPath, new DeleteFileVisitor());
    } catch (IOException e) {
      LOGGER.warn("delete {} error: {}", dir, e.toString());
    }
  }

  static class DeleteFileVisitor extends SimpleFileVisitor<Path> {
    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
      Files.delete(file);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
      Files.delete(dir);
      return FileVisitResult.CONTINUE;
    }
  }
}
