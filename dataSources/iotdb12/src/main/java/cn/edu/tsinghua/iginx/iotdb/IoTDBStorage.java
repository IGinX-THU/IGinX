/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.iotdb;

import static cn.edu.tsinghua.iginx.iotdb.tools.DataTypeTransformer.toIoTDB;
import static cn.edu.tsinghua.iginx.thrift.DataType.BINARY;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream.EmptyRowStream;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.ClearEmptyRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.ColumnDataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.iotdb.query.entity.IoTDBQueryRowStream;
import cn.edu.tsinghua.iginx.iotdb.tools.DataViewWrapper;
import cn.edu.tsinghua.iginx.iotdb.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.iotdb.tools.TagKVUtils;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBStorage implements IStorage {

  private static final int BATCH_SIZE = 10000;

  private static final String USERNAME = "username";

  private static final String PASSWORD = "password";

  private static final String SESSION_POOL_SIZE = "sessionPoolSize";

  private static final String DEFAULT_USERNAME = "root";

  private static final String DEFAULT_PASSWORD = "root";

  private static final String DEFAULT_SESSION_POOL_SIZE = "100";

  private static final String PREFIX = "root.";

  private static final String QUERY_DATA = "SELECT %s FROM " + PREFIX + "%s";

  private static final String QUERY_HISTORY_DATA = "SELECT %s FROM root";

  private static final String QUERY_WHERE = " WHERE %s";

  private static final String DELETE_STORAGE_GROUP_CLAUSE = "DELETE STORAGE GROUP " + PREFIX + "%s";

  private static final String DELETE_TIMESERIES_CLAUSE = "DELETE TIMESERIES %s";

  private static final String SHOW_TIMESERIES = "SHOW TIMESERIES";

  private static final String DOES_NOT_EXISTED = "does not exist";

  private static final String HAS_NOT_EXECUTED_QUERY = "Has not executed query";

  private final SessionPool sessionPool;

  private final StorageEngineMeta meta;

  private static final Logger logger = LoggerFactory.getLogger(IoTDBStorage.class);

  public IoTDBStorage(StorageEngineMeta meta) throws StorageInitializationException {
    this.meta = meta;
    if (!meta.getStorageEngine().equals(StorageEngineType.iotdb12)) {
      throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
    }
    if (!testConnection()) {
      throw new StorageInitializationException("cannot connect to " + meta.toString());
    }
    sessionPool = createSessionPool();
  }

  private boolean testConnection() {
    Map<String, String> extraParams = meta.getExtraParams();
    String username = extraParams.getOrDefault(USERNAME, DEFAULT_USERNAME);
    String password = extraParams.getOrDefault(PASSWORD, DEFAULT_PASSWORD);

    Session session = new Session(meta.getIp(), meta.getPort(), username, password);
    try {
      session.open(false);
      session.close();
    } catch (IoTDBConnectionException e) {
      logger.error("test connection error: {}", e.getMessage());
      return false;
    }
    return true;
  }

  private SessionPool createSessionPool() {
    Map<String, String> extraParams = meta.getExtraParams();
    String username = extraParams.getOrDefault(USERNAME, DEFAULT_USERNAME);
    String password = extraParams.getOrDefault(PASSWORD, DEFAULT_PASSWORD);
    int sessionPoolSize =
        Integer.parseInt(extraParams.getOrDefault(SESSION_POOL_SIZE, DEFAULT_SESSION_POOL_SIZE));
    return new SessionPool(meta.getIp(), meta.getPort(), username, password, sessionPoolSize);
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String dataPrefix)
      throws PhysicalException {
    SessionDataSetWrapper dataSet;
    RowRecord record;

    // 获取序列范围
    List<String> paths = new ArrayList<>();
    ColumnsInterval columnsInterval;
    try {
      if (dataPrefix == null || dataPrefix.isEmpty()) {
        dataSet = sessionPool.executeQueryStatement(SHOW_TIMESERIES);
        while (dataSet.hasNext()) {
          record = dataSet.next();
          if (record == null || record.getFields().size() < 4) {
            continue;
          }
          String path = record.getFields().get(0).getStringValue();
          path = path.substring(5);
          path = TagKVUtils.splitFullName(path).k;
          paths.add(path);
        }
        dataSet.close();
        paths.sort(String::compareTo);
        if (paths.isEmpty()) {
          throw new PhysicalTaskExecuteFailureException("no data!");
        }
        columnsInterval =
            new ColumnsInterval(paths.get(0), StringUtils.nextString(paths.get(paths.size() - 1)));
      } else {
        columnsInterval = new ColumnsInterval(dataPrefix);
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new PhysicalTaskExecuteFailureException("get time series failure: ", e);
    }

    // 获取 key 范围
    long minTime = 0, maxTime = Long.MAX_VALUE;
    try {
      // 获取 key 的最小值
      if (dataPrefix == null || dataPrefix.isEmpty()) {
        dataSet = sessionPool.executeQueryStatement("select * from root");
      } else {
        dataSet = sessionPool.executeQueryStatement("select " + dataPrefix + " from root");
      }
      if (dataSet.hasNext()) {
        record = dataSet.next();
        minTime = record.getTimestamp();
      }
      dataSet.close();

      // 获取 key 的最大值
      if (dataPrefix == null || dataPrefix.isEmpty())
        dataSet = sessionPool.executeQueryStatement("select * from root order by time desc");
      else {
        dataSet =
            sessionPool.executeQueryStatement(
                "select " + dataPrefix + " from root order by time desc");
      }
      if (dataSet.hasNext()) {
        record = dataSet.next();
        maxTime = record.getTimestamp();
      }
      dataSet.close();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new PhysicalTaskExecuteFailureException("get time series failure: ", e);
    }
    KeyInterval keyInterval = new KeyInterval(minTime, maxTime + 1);

    return new Pair<>(columnsInterval, keyInterval);
  }

  @Override
  public void release() throws PhysicalException {
    sessionPool.close();
  }

  @Override
  public List<Column> getColumns() throws PhysicalException {
    List<Column> columns = new ArrayList<>();
    getColumns2StorageUnit(columns, null);
    return columns;
  }

  private void getColumns2StorageUnit(List<Column> columns, Map<String, String> columns2StorageUnit)
      throws PhysicalException {
    try {
      SessionDataSetWrapper dataSet = sessionPool.executeQueryStatement(SHOW_TIMESERIES);
      while (dataSet.hasNext()) {
        RowRecord record = dataSet.next();
        if (record == null || record.getFields().size() < 4) {
          continue;
        }
        String path = record.getFields().get(0).getStringValue();
        path = path.substring(5); // remove root.
        boolean isDummy = true;
        if (path.startsWith("unit")) {
          path = path.substring(path.indexOf('.') + 1);
          isDummy = false;
        }
        Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(path);
        String dataTypeName = record.getFields().get(3).getStringValue();

        String fragment = isDummy ? "" : record.getFields().get(2).getStringValue().substring(5);
        if (columns2StorageUnit != null) {
          columns2StorageUnit.put(pair.k, fragment);
        }

        switch (dataTypeName) {
          case "BOOLEAN":
            columns.add(new Column(pair.k, DataType.BOOLEAN, pair.v, isDummy));
            break;
          case "FLOAT":
            columns.add(new Column(pair.k, DataType.FLOAT, pair.v, isDummy));
            break;
          case "TEXT":
            columns.add(new Column(pair.k, DataType.BINARY, pair.v, isDummy));
            break;
          case "DOUBLE":
            columns.add(new Column(pair.k, DataType.DOUBLE, pair.v, isDummy));
            break;
          case "INT32":
            columns.add(new Column(pair.k, DataType.INTEGER, pair.v, isDummy));
            break;
          case "INT64":
            columns.add(new Column(pair.k, DataType.LONG, pair.v, isDummy));
            break;
        }
      }
      dataSet.close();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new PhysicalTaskExecuteFailureException("get time series failure: ", e);
    }
  }

  @Override
  public TaskExecuteResult executeProject(Project project, DataArea dataArea) {
    String storageUnit = dataArea.getStorageUnit();
    KeyInterval keyInterval = dataArea.getKeyInterval();
    Filter filter =
        new AndFilter(
            Arrays.asList(
                new KeyFilter(Op.GE, keyInterval.getStartKey()),
                new KeyFilter(Op.L, keyInterval.getEndKey())));
    return executeProjectWithFilter(project, filter, storageUnit);
  }

  @Override
  public boolean isSupportProjectWithSelect() {
    return true;
  }

  @Override
  public TaskExecuteResult executeProjectWithSelect(
      Project project, Select select, DataArea dataArea) {
    String storageUnit = dataArea.getStorageUnit();
    Filter filter = select.getFilter();
    return executeProjectWithFilter(project, filter, storageUnit);
  }

  private boolean isContainWildcard(List<String> paths) {
    for (String path : paths) {
      if (path.contains("*")) {
        return true;
      }
    }
    return false;
  }

  private TaskExecuteResult executeProjectWithFilter(
      Project project, Filter filter, String storageUnit) {
    try {
      StringBuilder builder = new StringBuilder();
      boolean containWildcard = isContainWildcard(project.getPatterns());
      List<String> paths = project.getPatterns();
      if (containWildcard) {
        paths = determinePathList(storageUnit, project.getPatterns());
      }
      for (String path : paths) {
        // TODO 暂时屏蔽含有\的pattern
        if (path.contains("\\")) {
          return new TaskExecuteResult(new EmptyRowStream());
        }
        builder.append(path);
        builder.append(',');
      }
      String statement =
          String.format(
              QUERY_DATA, builder.deleteCharAt(builder.length() - 1).toString(), storageUnit);

      String filterStr = getFilterString(filter, storageUnit);
      if (!filterStr.isEmpty()) {
        statement += String.format(QUERY_WHERE, filterStr);
      }

      logger.info("[Query] execute query: " + statement);
      RowStream rowStream =
          new ClearEmptyRowStreamWrapper(
              new IoTDBQueryRowStream(
                  sessionPool.executeQueryStatement(statement), true, project, filter));

      List<Row> newRows = new ArrayList<>();
      while (rowStream.hasNext()) {
        newRows.add(rowStream.next());
      }
      Table table = new Table(rowStream.getHeader(), newRows);
      return new TaskExecuteResult(table);
    } catch (IoTDBConnectionException | StatementExecutionException | PhysicalException e) {
      logger.error(e.getMessage());
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("execute project task in iotdb12 failure", e));
    }
  }

  @Override
  public TaskExecuteResult executeProjectDummy(Project project, DataArea dataArea) {
    KeyInterval keyInterval = dataArea.getKeyInterval();
    Filter filter =
        new AndFilter(
            Arrays.asList(
                new KeyFilter(Op.GE, keyInterval.getStartKey()),
                new KeyFilter(Op.L, keyInterval.getEndKey())));
    return executeProjectDummyWithFilter(project, filter);
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithSelect(
      Project project, Select select, DataArea dataArea) {
    return executeProjectDummyWithFilter(project, select.getFilter());
  }

  private TaskExecuteResult executeProjectDummyWithFilter(Project project, Filter filter) {
    try {
      StringBuilder builder = new StringBuilder();
      boolean containWildcard = isContainWildcard(project.getPatterns());
      List<String> paths = project.getPatterns();
      if (containWildcard) {
        paths = determinePathList(null, project.getPatterns());
      }
      for (String path : paths) {
        // TODO 暂时屏蔽含有\的pattern
        if (path.contains("\\")) {
          return new TaskExecuteResult(new EmptyRowStream());
        }
        if (path.startsWith("*") && path.indexOf("*.", 1) != 2) {
          path = "*." + path;
        }
        builder.append(path);
        builder.append(',');
      }
      String statement =
          String.format(QUERY_HISTORY_DATA, builder.deleteCharAt(builder.length() - 1).toString());

      String filterStr = getFilterString(filter, "");
      if (!filterStr.isEmpty()) {
        statement += String.format(QUERY_WHERE, filterStr);
      }

      logger.info("[Query] execute query: " + statement);
      RowStream rowStream =
          new ClearEmptyRowStreamWrapper(
              new IoTDBQueryRowStream(
                  sessionPool.executeQueryStatement(statement), false, project, filter));
      return new TaskExecuteResult(rowStream);
    } catch (IoTDBConnectionException | StatementExecutionException | PhysicalException e) {
      logger.error(e.getMessage());
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("execute project task in iotdb12 failure", e));
    }
  }

  @Override
  public TaskExecuteResult executeInsert(Insert insert, DataArea dataArea) {
    DataView dataView = insert.getData();
    String storageUnit = dataArea.getStorageUnit();
    Exception e = null;
    switch (dataView.getRawDataType()) {
      case Row:
        e = insertRowRecords((RowDataView) dataView, storageUnit);
        break;
      case Column:
        e = insertColumnRecords((ColumnDataView) dataView, storageUnit);
        break;
      case NonAlignedRow:
        e = insertNonAlignedRowRecords((RowDataView) dataView, storageUnit);
        break;
      case NonAlignedColumn:
        e = insertNonAlignedColumnRecords((ColumnDataView) dataView, storageUnit);
        break;
    }
    if (e != null) {
      return new TaskExecuteResult(
          null, new PhysicalException("execute insert task in iotdb12 failure", e));
    }
    return new TaskExecuteResult(null, null);
  }

  private Exception insertRowRecords(RowDataView dataView, String storageUnit) {
    DataViewWrapper data = new DataViewWrapper(dataView);
    Map<String, Tablet> tablets = new HashMap<>();
    Map<String, List<MeasurementSchema>> schemasMap = new HashMap<>();
    Map<String, List<Integer>> deviceIdToPathIndexes = new HashMap<>();
    int batchSize = Math.min(data.getTimeSize(), BATCH_SIZE);

    // 创建 tablets
    for (int i = 0; i < data.getPathNum(); i++) {
      String path = data.getPath(i);
      String deviceId = PREFIX + storageUnit + "." + path.substring(0, path.lastIndexOf('.'));
      String measurement = path.substring(path.lastIndexOf('.') + 1);
      List<MeasurementSchema> schemaList;
      List<Integer> pathIndexes;
      if (schemasMap.containsKey(deviceId)) {
        schemaList = schemasMap.get(deviceId);
        pathIndexes = deviceIdToPathIndexes.get(deviceId);
      } else {
        schemaList = new ArrayList<>();
        pathIndexes = new ArrayList<>();
      }
      schemaList.add(new MeasurementSchema(measurement, toIoTDB(data.getDataType(i))));
      schemasMap.put(deviceId, schemaList);
      pathIndexes.add(i);
      deviceIdToPathIndexes.put(deviceId, pathIndexes);
    }

    for (Map.Entry<String, List<MeasurementSchema>> entry : schemasMap.entrySet()) {
      tablets.put(entry.getKey(), new Tablet(entry.getKey(), entry.getValue(), batchSize));
    }

    int cnt = 0;
    do {
      int size = Math.min(data.getTimeSize() - cnt, batchSize);
      // 对于每个时间戳，需要记录每个 deviceId 对应的 tablet 的 row 的变化
      Map<String, Integer> deviceIdToRow = new HashMap<>();

      // 插入 timestamps 和 values
      for (int i = cnt; i < cnt + size; i++) {
        int index = 0;
        deviceIdToRow.clear();
        BitmapView bitmapView = data.getBitmapView(i);
        for (int j = 0; j < data.getPathNum(); j++) {
          if (bitmapView.get(j)) {
            String path = data.getPath(j);
            String deviceId = PREFIX + storageUnit + "." + path.substring(0, path.lastIndexOf('.'));
            String measurement = path.substring(path.lastIndexOf('.') + 1);
            Tablet tablet = tablets.get(deviceId);
            if (!deviceIdToRow.containsKey(deviceId)) {
              int row = tablet.rowSize++;
              tablet.addTimestamp(row, data.getTimestamp(i));
              deviceIdToRow.put(deviceId, row);
            }
            if (data.getDataType(j) == BINARY) {
              tablet.addValue(
                  measurement,
                  deviceIdToRow.get(deviceId),
                  new Binary((byte[]) data.getValue(i, index)));
            } else {
              tablet.addValue(measurement, deviceIdToRow.get(deviceId), data.getValue(i, index));
            }
            index++;
          }
        }
      }

      try {
        sessionPool.insertTablets(tablets);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        logger.error(e.getMessage());
        return e;
      }

      for (Tablet tablet : tablets.values()) {
        tablet.reset();
      }
      cnt += size;
    } while (cnt < data.getTimeSize());

    return null;
  }

  private Exception insertNonAlignedRowRecords(RowDataView dataView, String storageUnit) {
    DataViewWrapper data = new DataViewWrapper(dataView);
    Map<Integer, Map<String, Tablet>> tabletsMap = new HashMap<>();
    Map<Integer, Integer> pathIndexToTabletIndex = new HashMap<>();
    Map<String, Integer> deviceIdToCnt = new HashMap<>();
    int batchSize = Math.min(data.getTimeSize(), BATCH_SIZE);

    // 创建 tablets
    for (int i = 0; i < data.getPathNum(); i++) {
      String path = data.getPath(i);
      String deviceId = PREFIX + storageUnit + "." + path.substring(0, path.lastIndexOf('.'));
      String measurement = path.substring(path.lastIndexOf('.') + 1);
      int measurementNum;
      Map<String, Tablet> tablets;

      measurementNum = deviceIdToCnt.computeIfAbsent(deviceId, x -> -1);
      deviceIdToCnt.put(deviceId, measurementNum + 1);
      pathIndexToTabletIndex.put(i, measurementNum + 1);
      tablets = tabletsMap.computeIfAbsent(measurementNum + 1, x -> new HashMap<>());
      tablets.put(
          deviceId,
          new Tablet(
              deviceId,
              Collections.singletonList(
                  new MeasurementSchema(measurement, toIoTDB(data.getDataType(i)))),
              batchSize));
      tabletsMap.put(measurementNum + 1, tablets);
    }

    int cnt = 0;
    do {
      int size = Math.min(data.getTimeSize() - cnt, batchSize);
      boolean[] needToInsert = new boolean[tabletsMap.size()];
      Arrays.fill(needToInsert, false);

      // 插入 timestamps 和 values
      for (int i = cnt; i < cnt + size; i++) {
        int index = 0;
        BitmapView bitmapView = data.getBitmapView(i);
        for (int j = 0; j < data.getPathNum(); j++) {
          if (bitmapView.get(j)) {
            String path = data.getPath(j);
            String deviceId = PREFIX + storageUnit + "." + path.substring(0, path.lastIndexOf('.'));
            String measurement = path.substring(path.lastIndexOf('.') + 1);
            Tablet tablet = tabletsMap.get(pathIndexToTabletIndex.get(j)).get(deviceId);
            int row = tablet.rowSize++;
            tablet.addTimestamp(row, data.getTimestamp(i));
            if (data.getDataType(j) == BINARY) {
              tablet.addValue(measurement, row, new Binary((byte[]) data.getValue(i, index)));
            } else {
              tablet.addValue(measurement, row, data.getValue(i, index));
            }
            needToInsert[pathIndexToTabletIndex.get(j)] = true;
            index++;
          }
        }
      }

      // 插入 tablets
      try {
        for (int i = 0; i < needToInsert.length; i++) {
          if (needToInsert[i]) {
            sessionPool.insertTablets(tabletsMap.get(i));
          }
        }
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        logger.error(e.getMessage());
        return e;
      }

      // 重置 tablets
      for (int i = 0; i < needToInsert.length; i++) {
        if (needToInsert[i]) {
          for (Tablet tablet : tabletsMap.get(i).values()) {
            tablet.reset();
          }
          needToInsert[i] = false;
        }
      }
      cnt += size;
    } while (cnt < data.getTimeSize());

    return null;
  }

  private Exception insertColumnRecords(ColumnDataView dataView, String storageUnit) {
    DataViewWrapper data = new DataViewWrapper(dataView);
    Map<String, Tablet> tablets = new HashMap<>();
    Map<String, List<MeasurementSchema>> schemasMap = new HashMap<>();
    Map<String, List<Integer>> deviceIdToPathIndexes = new HashMap<>();
    int batchSize = Math.min(data.getTimeSize(), BATCH_SIZE);

    // 创建 tablets
    for (int i = 0; i < data.getPathNum(); i++) {
      String path = data.getPath(i);
      String deviceId = PREFIX + storageUnit + "." + path.substring(0, path.lastIndexOf('.'));
      String measurement = path.substring(path.lastIndexOf('.') + 1);
      List<MeasurementSchema> schemaList;
      List<Integer> pathIndexes;
      if (schemasMap.containsKey(deviceId)) {
        schemaList = schemasMap.get(deviceId);
        pathIndexes = deviceIdToPathIndexes.get(deviceId);
      } else {
        schemaList = new ArrayList<>();
        pathIndexes = new ArrayList<>();
      }
      schemaList.add(new MeasurementSchema(measurement, toIoTDB(data.getDataType(i))));
      schemasMap.put(deviceId, schemaList);
      pathIndexes.add(i);
      deviceIdToPathIndexes.put(deviceId, pathIndexes);
    }

    for (Map.Entry<String, List<MeasurementSchema>> entry : schemasMap.entrySet()) {
      tablets.put(entry.getKey(), new Tablet(entry.getKey(), entry.getValue(), batchSize));
    }

    int cnt = 0;
    int[] indexes = new int[data.getPathNum()];
    do {
      int size = Math.min(data.getTimeSize() - cnt, batchSize);

      // 插入 timestamps 和 values
      for (Map.Entry<String, List<Integer>> entry : deviceIdToPathIndexes.entrySet()) {
        String deviceId = entry.getKey();
        Tablet tablet = tablets.get(deviceId);
        for (int i = cnt; i < cnt + size; i++) {
          BitmapView bitmapView = data.getBitmapView(entry.getValue().get(0));
          if (bitmapView.get(i)) {
            int row = tablet.rowSize++;
            tablet.addTimestamp(row, data.getTimestamp(i));
            for (Integer j : entry.getValue()) {
              String path = data.getPath(j);
              String measurement = path.substring(path.lastIndexOf('.') + 1);
              if (data.getDataType(j) == BINARY) {
                tablet.addValue(
                    measurement, row, new Binary((byte[]) data.getValue(j, indexes[j])));
              } else {
                tablet.addValue(measurement, row, data.getValue(j, indexes[j]));
              }
              indexes[j]++;
            }
          }
        }
      }

      try {
        sessionPool.insertTablets(tablets);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        logger.error(e.getMessage());
        return e;
      }

      for (Tablet tablet : tablets.values()) {
        tablet.reset();
      }
      cnt += size;
    } while (cnt < data.getTimeSize());

    return null;
  }

  private Exception insertNonAlignedColumnRecords(ColumnDataView dataView, String storageUnit) {
    DataViewWrapper data = new DataViewWrapper(dataView);
    Map<Integer, Map<String, Tablet>> tabletsMap = new HashMap<>();
    Map<Integer, List<Integer>> tabletIndexToPathIndexes = new HashMap<>();
    Map<String, Integer> deviceIdToCnt = new HashMap<>();
    int batchSize = Math.min(data.getTimeSize(), BATCH_SIZE);

    // 创建 tablets
    for (int i = 0; i < data.getPathNum(); i++) {
      String path = data.getPath(i);
      String deviceId = PREFIX + storageUnit + "." + path.substring(0, path.lastIndexOf('.'));
      String measurement = path.substring(path.lastIndexOf('.') + 1);
      int measurementNum;
      List<Integer> pathIndexes;
      Map<String, Tablet> tablets;

      measurementNum = deviceIdToCnt.computeIfAbsent(deviceId, x -> -1);
      deviceIdToCnt.put(deviceId, measurementNum + 1);
      pathIndexes =
          tabletIndexToPathIndexes.computeIfAbsent(measurementNum + 1, x -> new ArrayList<>());
      pathIndexes.add(i);
      tabletIndexToPathIndexes.put(measurementNum + 1, pathIndexes);
      tablets = tabletsMap.computeIfAbsent(measurementNum + 1, x -> new HashMap<>());
      tablets.put(
          deviceId,
          new Tablet(
              deviceId,
              Collections.singletonList(
                  new MeasurementSchema(measurement, toIoTDB(data.getDataType(i)))),
              batchSize));
      tabletsMap.put(measurementNum + 1, tablets);
    }

    for (Map.Entry<Integer, List<Integer>> entry : tabletIndexToPathIndexes.entrySet()) {
      int cnt = 0;
      int[] indexesOfBitmap = new int[entry.getValue().size()];
      do {
        int size = Math.min(data.getTimeSize() - cnt, batchSize);

        // 插入 timestamps 和 values
        for (int i = 0; i < entry.getValue().size(); i++) {
          int index = entry.getValue().get(i);
          String path = data.getPath(index);
          String deviceId = PREFIX + storageUnit + "." + path.substring(0, path.lastIndexOf('.'));
          String measurement = path.substring(path.lastIndexOf('.') + 1);
          Tablet tablet = tabletsMap.get(entry.getKey()).get(deviceId);
          BitmapView bitmapView = data.getBitmapView(index);
          for (int j = cnt; j < cnt + size; j++) {
            if (bitmapView.get(j)) {
              int row = tablet.rowSize++;
              tablet.addTimestamp(row, data.getTimestamp(j));
              if (data.getDataType(index) == BINARY) {
                tablet.addValue(
                    measurement,
                    row,
                    new Binary((byte[]) data.getValue(index, indexesOfBitmap[i])));
              } else {
                tablet.addValue(measurement, row, data.getValue(index, indexesOfBitmap[i]));
              }
              indexesOfBitmap[i]++;
            }
          }
        }

        try {
          sessionPool.insertTablets(tabletsMap.get(entry.getKey()));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          logger.error(e.getMessage());
          return e;
        }

        for (Tablet tablet : tabletsMap.get(entry.getKey()).values()) {
          tablet.reset();
        }
        cnt += size;
      } while (cnt < data.getTimeSize());
    }
    return null;
  }

  @Override
  public TaskExecuteResult executeDelete(Delete delete, DataArea dataArea) {
    String storageUnit = dataArea.getStorageUnit();
    if (delete.getKeyRanges() == null || delete.getKeyRanges().size() == 0) { // 没有传任何 time range
      List<String> paths = delete.getPatterns();
      if (paths.size() == 1 && paths.get(0).equals("*") && delete.getTagFilter() == null) {
        try {
          sessionPool.executeNonQueryStatement(
              String.format(DELETE_STORAGE_GROUP_CLAUSE, storageUnit));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          logger.warn("encounter error when clear data: " + e.getMessage());
          if (!e.getMessage().contains(DOES_NOT_EXISTED)) {
            return new TaskExecuteResult(
                new PhysicalTaskExecuteFailureException(
                    "execute clear data in iotdb12 failure", e));
          }
        }
      } else {
        List<String> deletedPaths;
        try {
          deletedPaths = determineDeletePathList(storageUnit, delete);
        } catch (PhysicalException e) {
          logger.warn("encounter error when delete path: " + e.getMessage());
          return new TaskExecuteResult(
              new PhysicalTaskExecuteFailureException(
                  "execute delete path task in iotdb11 failure", e));
        }
        for (String path : deletedPaths) {
          try {
            sessionPool.executeNonQueryStatement(String.format(DELETE_TIMESERIES_CLAUSE, path));
          } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.warn("encounter error when delete path: " + e.getMessage());
            if (!e.getMessage().contains(DOES_NOT_EXISTED)) {
              return new TaskExecuteResult(
                  new PhysicalTaskExecuteFailureException(
                      "execute delete path task in iotdb12 failure", e));
            }
          }
        }
      }
    } else {
      try {
        List<String> paths = determineDeletePathList(storageUnit, delete);
        if (paths.size() != 0) {
          for (KeyRange keyRange : delete.getKeyRanges()) {
            sessionPool.deleteData(paths, keyRange.getActualBeginKey(), keyRange.getActualEndKey());
          }
        }
      } catch (IoTDBConnectionException | StatementExecutionException | PhysicalException e) {
        logger.warn("encounter error when delete data: " + e.getMessage());
        if (!e.getMessage().contains(DOES_NOT_EXISTED)) {
          return new TaskExecuteResult(
              new PhysicalTaskExecuteFailureException(
                  "execute delete data task in iotdb12 failure", e));
        }
      }
    }
    return new TaskExecuteResult(null, null);
  }

  private List<String> determineDeletePathList(String storageUnit, Delete delete)
      throws PhysicalException {
    if (delete.getTagFilter() == null) {
      return delete.getPatterns().stream()
          .map(x -> PREFIX + storageUnit + "." + x)
          .collect(Collectors.toList());
    } else {
      List<String> patterns = delete.getPatterns();
      TagFilter tagFilter = delete.getTagFilter();
      List<Column> timeSeries = getColumns();

      List<String> pathList = new ArrayList<>();
      for (Column ts : timeSeries) {
        for (String pattern : patterns) {
          if (Pattern.matches(StringUtils.reformatPath(pattern), ts.getPath())
              && TagKVUtils.match(ts.getTags(), tagFilter)) {
            pathList.add(PREFIX + storageUnit + "." + ts.getPhysicalPath());
            break;
          }
        }
      }
      return pathList;
    }
  }

  private List<String> determinePathList(String storageUnit, List<String> patterns)
      throws IoTDBConnectionException, StatementExecutionException {
    Set<String> pathSet = new HashSet<>();
    String showColumns = SHOW_TIMESERIES;
    showColumns = storageUnit == null ? showColumns : showColumns + " " + PREFIX + storageUnit;
    SessionDataSetWrapper dataSet = sessionPool.executeQueryStatement(showColumns);
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      if (record == null || record.getFields().size() < 4) {
        continue;
      }
      String path = record.getFields().get(0).getStringValue();
      path = path.substring(5); // remove root.
      boolean isDummy = true;
      if (path.startsWith("unit")) {
        path = path.substring(path.indexOf('.') + 1);
        isDummy = false;
      }
      if (!isDummy && storageUnit == null) {
        continue;
      }
      Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(path);
      if (patterns == null || patterns.isEmpty()) {
        pathSet.add(pair.k);
      } else {
        for (String pattern : patterns) {
          if (match(pair.k, pattern)) {
            pathSet.add(pair.k);
            break;
          }
        }
      }
    }
    dataSet.close();
    return pathSet.isEmpty() ? patterns : new ArrayList<>(pathSet);
  }

  private boolean match(String s, String p) {
    // 将输入的字符串p转换为正则表达式
    String regex = p.replace(".", "\\.").replace("*", ".*");

    // 使用正则表达式匹配字符串s
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(s);

    return matcher.matches();
  }

  private String getFilterString(Filter filter, String storageUnit) throws PhysicalException {
    String filterStr = FilterTransformer.toString(filter);
    if (filterStr.contains("*")) {
      List<Column> columns = new ArrayList<>();
      Map<String, String> columns2Fragment = new HashMap<>();
      getColumns2StorageUnit(columns, columns2Fragment);
      filterStr =
          FilterTransformer.toString(
              expandFilterWildcard(filter.copy(), columns, columns2Fragment, storageUnit));
    }

    return filterStr;
  }

  private Filter expandFilterWildcard(
      Filter filter,
      List<Column> columns,
      Map<String, String> columns2Fragment,
      String storageUnit) {
    switch (filter.getType()) {
      case And:
        AndFilter andFilter = (AndFilter) filter;
        List<Filter> children = andFilter.getChildren();
        List<Filter> newAndFilters = new ArrayList<>();
        for (Filter f : children) {
          Filter newFilter = expandFilterWildcard(f, columns, columns2Fragment, storageUnit);
          if (newFilter != null) {
            newAndFilters.add(expandFilterWildcard(f, columns, columns2Fragment, storageUnit));
          }
        }
        return new AndFilter(newAndFilters);
      case Or:
        OrFilter orFilter = (OrFilter) filter;
        List<Filter> orChildren = orFilter.getChildren();
        List<Filter> newOrFilters = new ArrayList<>();
        for (Filter f : orChildren) {
          Filter newFilter = expandFilterWildcard(f, columns, columns2Fragment, storageUnit);
          if (newFilter != null) {
            newOrFilters.add(expandFilterWildcard(f, columns, columns2Fragment, storageUnit));
          }
        }
        return new OrFilter(newOrFilters);
      case Not:
        NotFilter notFilter = (NotFilter) filter;
        Filter notChild = notFilter.getChild();
        Filter newNotFilter =
            expandFilterWildcard(notChild, columns, columns2Fragment, storageUnit);
        if (newNotFilter != null) return new NotFilter(newNotFilter);
        else return null;
      case Key:
        return filter;
      case Value:
        ValueFilter valueFilter = (ValueFilter) filter;
        DataType valueType = valueFilter.getValue().getDataType();
        String path = valueFilter.getPath();

        if (path.contains("*")) {
          List<String> matchedPath =
              getMatchPath(path, valueType, columns, columns2Fragment, storageUnit);
          if (matchedPath.size() == 0) {
            return null;
          }

          List<Filter> newFilters = new ArrayList<>();
          for (String p : matchedPath) {
            newFilters.add(new ValueFilter(p, valueFilter.getOp(), valueFilter.getValue()));
          }
          if (Op.isOrOp(valueFilter.getOp())) {
            return new OrFilter(newFilters);
          } else {
            return new AndFilter(newFilters);
          }
        } else {
          return filter;
        }

      default:
        return null;
    }
  }

  private List<String> getMatchPath(
      String path,
      DataType dataType,
      List<Column> columns,
      Map<String, String> columns2Fragment,
      String storageUnit) {
    List<String> matchedPath = new ArrayList<>();
    path = StringUtils.reformatPath(path);
    Pattern pattern = Pattern.compile("^" + path + "$");

    for (Column col : columns) {
      String columnName = col.getPath();
      DataType columnType = col.getDataType();

      List<DataType> numberType =
          Arrays.asList(DataType.DOUBLE, DataType.FLOAT, DataType.LONG, DataType.INTEGER);
      boolean canCompare =
          (numberType.contains(columnType) && numberType.contains(dataType))
              || columnType == dataType;

      if (!canCompare
          || columns2Fragment.get(columnName) == null
          || !columns2Fragment.get(columnName).equals(storageUnit)) {
        continue;
      }

      Matcher matcher = pattern.matcher(columnName);
      if (matcher.find()) {
        matchedPath.add(columnName);
      }
    }

    return matchedPath;
  }
}
