package cn.edu.tsinghua.iginx.integration.expansion.parquet;

import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.*;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.parquet.entity.Table;
import cn.edu.tsinghua.iginx.parquet.io.parquet.Storer;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger logger = LoggerFactory.getLogger(ParquetHistoryDataGenerator.class);

  private static final char IGINX_SEPARATOR = '.';

  public ParquetHistoryDataGenerator() {}

  @Override
  public void writeHistoryData(
      int port, List<String> pathList, List<DataType> dataTypeList, List<List<Object>> valuesList) {
    if (!PARQUET_PARAMS.containsKey(port)) {
      logger.error("writing to unknown port {}.", port);
      return;
    }

    String dir = "test" + System.getProperty("file.separator") + PARQUET_PARAMS.get(port).get(0);
    String filename = PARQUET_PARAMS.get(port).get(1);
    Path dirPath = Paths.get("../" + dir);
    if (Files.notExists(dirPath)) {
      try {
        Files.createDirectories(dirPath);
      } catch (IOException e) {
        logger.error("can't create data file path {}.", dir);
        return;
      }
    }

    assert pathList.size() == dataTypeList.size() && pathList.size() == valuesList.size();
    Table table = new Table();
    for (int fieldIndex = 0; fieldIndex < pathList.size(); fieldIndex++) {
      String originalColumnName = pathList.get(fieldIndex);
      String columnName =
          originalColumnName.substring(originalColumnName.indexOf(IGINX_SEPARATOR) + 1);
      table.declareColumn(columnName, dataTypeList.get(fieldIndex));
    }

    for (int key = 0; key < valuesList.size(); key++) {
      List<Object> rowData = valuesList.get(key);
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
  public void clearHistoryDataForGivenPort(int port) {
    if (!PARQUET_PARAMS.containsKey(port)) {
      logger.error("delete from unknown port {}.", port);
      return;
    }

    String dir = "test" + System.getProperty("file.separator") + PARQUET_PARAMS.get(port).get(0);
    String filename = PARQUET_PARAMS.get(port).get(1);
    Path parquetPath = Paths.get("../" + dir, filename);
    File file = new File(parquetPath.toString());

    if (file.exists() && file.isFile()) {
      file.delete();
    } else {
      logger.error("delete {}/{} error: does not exist or is not a file.", dir, filename);
    }
  }
}
