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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.integration.expansion.parquet;

import static cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT.DBCE_PARQUET_FS_TEST_DIR;
import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.*;

import cn.edu.tsinghua.iginx.format.parquet.ParquetWriter;
import cn.edu.tsinghua.iginx.format.parquet.example.ExampleParquetWriter;
import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shaded.iginx.org.apache.parquet.example.data.Group;
import shaded.iginx.org.apache.parquet.example.data.simple.SimpleGroupFactory;
import shaded.iginx.org.apache.parquet.io.api.Binary;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.PrimitiveType;
import shaded.iginx.org.apache.parquet.schema.Type;
import shaded.iginx.org.apache.parquet.schema.Types;

public class ParquetHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetHistoryDataGenerator.class);

  private static final char IGINX_SEPARATOR = '.';

  // 所有的IT测试产生的数据都会在IGinX/test/IT_DATA_DIR目录下
  public static final String IT_DATA_DIR = "IT_data";

  public static final String IT_DATA_FILENAME = "data%s.parquet";

  public ParquetHistoryDataGenerator() {}

  public void writeHistoryData(
      int port,
      String dir,
      String filename,
      List<String> pathList,
      List<DataType> dataTypeList,
      List<Long> keyList,
      List<List<Object>> valuesList) {
    LOGGER.info(
        "start writing data, dir:{}, filename:{}.", new File(dir).getAbsolutePath(), filename);

    if (!PARQUET_PARAMS.containsKey(port)) {
      LOGGER.error("writing to unknown port {}.", port);
      return;
    }

    Path dirPath = Paths.get("../" + dir);
    if (Files.notExists(dirPath)) {
      try {
        Files.createDirectories(dirPath);
      } catch (IOException e) {
        LOGGER.error("can't create data file path {}.", new File(dir).getAbsolutePath());
        return;
      }
    }

    assert pathList.size() == dataTypeList.size()
        : "pathList.size() = " + pathList.size() + ", dataTypeList.size() = " + dataTypeList.size();
    assert keyList.size() == valuesList.size()
        : "keyList.size() = " + keyList.size() + ", valuesList.size() = " + valuesList.size();

    List<String> columnNames = new ArrayList<>();
    for (String originalColumnName : pathList) {
      String columnName =
          originalColumnName.substring(originalColumnName.indexOf(IGINX_SEPARATOR) + 1);
      columnNames.add(columnName);
    }

    SortedMap<Long, List<Object>> sortedMap = new TreeMap<>();
    for (int i = 0; i < keyList.size(); i++) {
      sortedMap.put(keyList.get(i), valuesList.get(i));
    }
    List<Long> sortedKeyList = new ArrayList<>(sortedMap.keySet());
    List<List<Object>> sortedValuesList = new ArrayList<>(sortedMap.values());

    Path parquetPath = Paths.get("../" + dir, filename);
    try {
      flushRows(columnNames, dataTypeList, sortedKeyList, sortedValuesList, parquetPath);
    } catch (IOException e) {
      LOGGER.error("write data to {} error", new File(parquetPath.toString()).getAbsolutePath());
      throw new RuntimeException(e);
    }

    try {
      List<Path> paths = Files.list(Paths.get("../" + dir)).collect(Collectors.toList());
      LOGGER.info("directory contains {}", paths);
    } catch (IOException e) {
      LOGGER.error("write data to {} error", new File(parquetPath.toString()).getAbsolutePath());
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
    String dir =
        DBCE_PARQUET_FS_TEST_DIR
            + System.getProperty("file.separator")
            + PARQUET_PARAMS.get(port).get(0);
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

    String dir =
        DBCE_PARQUET_FS_TEST_DIR
            + System.getProperty("file.separator")
            + PARQUET_PARAMS.get(port).get(0);
    String filename = PARQUET_PARAMS.get(port).get(1);
    Path parquetPath = Paths.get("../" + dir, filename);
    File file = new File(parquetPath.toString());

    if (file.exists() && file.isFile()) {
      file.delete();
    } else {
      LOGGER.warn(
          "delete {}/{} error: does not exist or is not a file.", dir, file.getAbsoluteFile());
    }

    // delete the normal IT data
    dir = DBCE_PARQUET_FS_TEST_DIR + System.getProperty("file.separator") + IT_DATA_DIR;
    parquetPath = Paths.get("../" + dir);

    try {
      Files.walkFileTree(parquetPath, new DeleteFileVisitor());
    } catch (NoSuchFileException e) {
      LOGGER.warn(
          "no such file or directory: {}", new File(parquetPath.toString()).getAbsoluteFile());
    } catch (IOException e) {
      LOGGER.warn("delete {} error: ", new File(parquetPath.toString()).getAbsoluteFile(), e);
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

  public static final String KEY_FIELD_NAME = "*";

  private static void flushRows(
      List<String> pathList,
      List<DataType> dataTypeList,
      List<Long> keyList,
      List<List<Object>> valuesList,
      Path parquetPath)
      throws IOException {
    MessageType schema = getMessageType(pathList, dataTypeList);

    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    List<Group> groups = getGroups(pathList, dataTypeList, keyList, valuesList, f);

    ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(parquetPath, schema);
    try (ParquetWriter<Group> writer = builder.build()) {
      for (Group group : groups) {
        writer.write(group);
      }
    }
  }

  @Nonnull
  private static List<Group> getGroups(
      List<String> pathList,
      List<DataType> dataTypeList,
      List<Long> keyList,
      List<List<Object>> valuesList,
      SimpleGroupFactory f) {
    List<Group> groups = new ArrayList<>();
    for (int i = 0; i < valuesList.size(); i++) {
      long key = keyList.get(i);
      List<Object> rowData = valuesList.get(i);
      assert rowData.size() == pathList.size()
          : "rowData.size() = " + rowData.size() + ", pathList.size() = " + pathList.size();
      Group group = getGroup(pathList, dataTypeList, key, rowData, f);
      groups.add(group);
    }
    return groups;
  }

  @Nonnull
  private static Group getGroup(
      List<String> pathList,
      List<DataType> dataTypeList,
      long key,
      List<Object> rowData,
      SimpleGroupFactory f) {
    Group group = f.newGroup();
    group.add(KEY_FIELD_NAME, key);
    for (int fieldIndex = 0; fieldIndex < pathList.size(); fieldIndex++) {
      String columnName = pathList.get(fieldIndex);
      DataType dataType = dataTypeList.get(fieldIndex);
      Object value = rowData.get(fieldIndex);
      switch (dataType) {
        case BOOLEAN:
          group.add(columnName, (Boolean) value);
          break;
        case INTEGER:
          group.add(columnName, (Integer) value);
          break;
        case LONG:
          group.add(columnName, (Long) value);
          break;
        case FLOAT:
          group.add(columnName, (Float) value);
          break;
        case DOUBLE:
          group.add(columnName, (Double) value);
          break;
        case BINARY:
          group.add(columnName, Binary.fromConstantByteArray((byte[]) value));
          break;
        default:
          throw new RuntimeException("Unsupported data type: " + dataType);
      }
    }
    return group;
  }

  private static MessageType getMessageType(List<String> pathList, List<DataType> dataTypeList) {
    Types.MessageTypeBuilder messageTypeBuilder = Types.buildMessage();
    messageTypeBuilder.addField(
        new PrimitiveType(
            Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, KEY_FIELD_NAME));
    for (int i = 0; i < pathList.size(); i++) {
      String name = pathList.get(i);
      DataType type = dataTypeList.get(i);
      PrimitiveType.PrimitiveTypeName parquetTypeName;
      switch (type) {
        case BOOLEAN:
          parquetTypeName = PrimitiveType.PrimitiveTypeName.BOOLEAN;
          break;
        case INTEGER:
          parquetTypeName = PrimitiveType.PrimitiveTypeName.INT32;
          break;
        case LONG:
          parquetTypeName = PrimitiveType.PrimitiveTypeName.INT64;
          break;
        case FLOAT:
          parquetTypeName = PrimitiveType.PrimitiveTypeName.FLOAT;
          break;
        case DOUBLE:
          parquetTypeName = PrimitiveType.PrimitiveTypeName.DOUBLE;
          break;
        case BINARY:
          parquetTypeName = PrimitiveType.PrimitiveTypeName.BINARY;
          break;
        default:
          throw new RuntimeException("Unsupported data type: " + type);
      }
      PrimitiveType parquetType =
          new PrimitiveType(Type.Repetition.OPTIONAL, parquetTypeName, name);
      messageTypeBuilder.addField(parquetType);
    }
    return messageTypeBuilder.named("test");
  }
}
