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
package cn.edu.tsinghua.iginx.session_v2.internal;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getByteArrayFromLongArray;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session_v2.WriteClient;
import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import cn.edu.tsinghua.iginx.session_v2.write.Point;
import cn.edu.tsinghua.iginx.session_v2.write.Record;
import cn.edu.tsinghua.iginx.session_v2.write.Table;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteClientImpl extends AbstractFunctionClient implements WriteClient {
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(WriteClientImpl.class);

  private final MeasurementMapper measurementMapper;

  public WriteClientImpl(IginXClientImpl iginXClient, MeasurementMapper measurementMapper) {
    super(iginXClient);
    this.measurementMapper = measurementMapper;
  }

  @Override
  public void writePoint(Point point) {
    writePoints(Collections.singletonList(point), null);
  }

  @Override
  public void writePoint(Point point, TimePrecision timePrecision) {
    writePoints(Collections.singletonList(point), timePrecision);
  }

  @Override
  public void writePoints(List<Point> points) {
    writePoints(points, null);
  }

  @Override
  public void writePoints(List<Point> points, TimePrecision timePrecision) {
    SortedMap<String, DataType> measurementMap = new TreeMap<>();
    Set<Long> timestampSet = new HashSet<>();
    for (Point point : points) {
      String measurement = point.getFullName();
      DataType dataType = point.getDataType();
      if (measurementMap.getOrDefault(measurement, dataType) != dataType) {
        throw new IllegalArgumentException(
            "measurement " + measurement + " has multi data type, which is invalid.");
      }
      measurementMap.putIfAbsent(measurement, dataType);
      timestampSet.add(point.getKey());
    }
    List<String> measurements = new ArrayList<>();
    List<DataType> dataTypeList = new ArrayList<>();
    Map<String, Integer> measurementIndexMap = new HashMap<>();
    int index = 0;
    for (String measurement : measurementMap.keySet()) {
      measurementIndexMap.put(measurement, index);
      index++;
      measurements.add(measurement);
      dataTypeList.add(measurementMap.get(measurement));
    }

    long[] timestamps = timestampSet.stream().sorted().mapToLong(e -> e).toArray();
    Map<Long, Integer> timestampIndexMap = new HashMap<>();
    for (int i = 0; i < timestamps.length; i++) {
      timestampIndexMap.put(timestamps[i], i);
    }

    Object[][] valuesList = new Object[measurements.size()][];
    for (int i = 0; i < valuesList.length; i++) {
      valuesList[i] = new Object[timestamps.length];
    }
    for (Point point : points) {
      String measurement = point.getFullName();
      long timestamp = point.getKey();
      int measurementIndex = measurementIndexMap.get(measurement);
      int timestampIndex = timestampIndexMap.get(timestamp);
      valuesList[measurementIndex][timestampIndex] = point.getValue();
    }
    List<Map<String, String>> tagsList = new ArrayList<>();
    for (int i = 0; i < measurements.size(); i++) {
      String measurement = measurements.get(i);
      Pair<String, Map<String, String>> pair = TagKVUtils.fromFullName(measurement);
      measurements.set(i, pair.k);
      tagsList.add(pair.v);
    }
    writeColumnData(measurements, timestamps, valuesList, dataTypeList, tagsList, timePrecision);
  }

  @Override
  public void writeRecord(Record record) {
    writeRecords(Collections.singletonList(record), null);
  }

  @Override
  public void writeRecord(Record record, TimePrecision timePrecision) {
    writeRecords(Collections.singletonList(record), timePrecision);
  }

  @Override
  public void writeRecords(List<Record> records) {
    writeRecords(records, null);
  }

  @Override
  public void writeRecords(List<Record> records, TimePrecision timePrecision) {
    SortedMap<String, DataType> measurementMap = new TreeMap<>();
    for (Record record : records) {
      for (int index = 0; index < record.getLength(); index++) {
        String measurement = record.getFullName(index);
        DataType dataType = record.getDataType(index);
        if (measurementMap.getOrDefault(measurement, dataType) != dataType) {
          throw new IllegalArgumentException(
              "measurement " + measurement + " has multi data type, which is invalid.");
        }
        measurementMap.putIfAbsent(measurement, dataType);
      }
    }

    List<String> measurements = new ArrayList<>();
    List<DataType> dataTypeList = new ArrayList<>();
    Map<String, Integer> measurementIndexMap = new HashMap<>(); // measurement 对应的 index
    int index = 0;
    for (String measurement : measurementMap.keySet()) {
      measurementIndexMap.put(measurement, index);
      index++;
      measurements.add(measurement);
      dataTypeList.add(measurementMap.get(measurement));
    }

    SortedMap<Long, Object[]> valuesMap = new TreeMap<>();
    for (Record record : records) {
      long timestamp = record.getKey();
      Object[] values = valuesMap.getOrDefault(timestamp, new Object[measurements.size()]);
      for (int i = 0; i < record.getValues().size(); i++) {
        String measurement = record.getFullName(i);
        int measurementIndex = measurementIndexMap.get(measurement);
        values[measurementIndex] = record.getValue(i);
      }
      valuesMap.put(timestamp, values);
    }

    long[] timestamps = new long[valuesMap.size()];
    Object[][] valuesList = new Object[valuesMap.size()][];
    index = 0;
    for (Map.Entry<Long, Object[]> entry : valuesMap.entrySet()) {
      timestamps[index] = entry.getKey();
      valuesList[index] = entry.getValue();
      index++;
    }
    List<Map<String, String>> tagsList = new ArrayList<>();
    for (int i = 0; i < measurements.size(); i++) {
      String measurement = measurements.get(i);
      Pair<String, Map<String, String>> pair = TagKVUtils.fromFullName(measurement);
      measurements.set(i, pair.k);
      tagsList.add(pair.v);
    }
    writeRowData(measurements, timestamps, valuesList, dataTypeList, tagsList, timePrecision);
  }

  @Override
  public <M> void writeMeasurement(M measurement) {
    writeMeasurements(Collections.singletonList(measurement), null);
  }

  @Override
  public <M> void writeMeasurement(M measurement, TimePrecision timePrecision) {
    writeMeasurements(Collections.singletonList(measurement), timePrecision);
  }

  @Override
  public <M> void writeMeasurements(List<M> measurements) {
    writeRecords(
        measurements.stream().map(measurementMapper::toRecord).collect(Collectors.toList()), null);
  }

  @Override
  public <M> void writeMeasurements(List<M> measurements, TimePrecision timePrecision) {
    writeRecords(
        measurements.stream().map(measurementMapper::toRecord).collect(Collectors.toList()),
        timePrecision);
  }

  @Override
  public void writeTable(Table table) {
    writeTable(table, null);
  }

  @Override
  public void writeTable(Table table, TimePrecision timePrecision) {
    long[] timestamps = new long[table.getLength()];
    Object[][] valuesList = new Object[table.getLength()][];
    for (int i = 0; i < table.getLength(); i++) {
      timestamps[i] = table.getKey(i);
      valuesList[i] = table.getValues(i);
    }
    List<String> measurements = table.getMeasurements();
    List<Map<String, String>> tagsList = table.getTagsList();
    writeRowData(
        measurements, timestamps, valuesList, table.getDataTypes(), tagsList, timePrecision);
  }

  private void writeColumnData(
      List<String> paths,
      long[] timestamps,
      Object[][] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList,
      TimePrecision timePrecision) {
    List<ByteBuffer> valueBufferList = new ArrayList<>();
    List<ByteBuffer> bitmapBufferList = new ArrayList<>();
    for (int i = 0; i < valuesList.length; i++) {
      Object[] values = valuesList[i];
      valueBufferList.add(ByteUtils.getColumnByteBuffer(values, dataTypeList.get(i)));
      Bitmap bitmap = new Bitmap(values.length);
      for (int j = 0; j < values.length; j++) {
        if (values[j] != null) {
          bitmap.mark(j);
        }
      }
      bitmapBufferList.add(ByteBuffer.wrap(bitmap.getBytes()));
    }

    InsertNonAlignedColumnRecordsReq req = new InsertNonAlignedColumnRecordsReq();
    req.setSessionId(sessionId);
    req.setPaths(paths);
    req.setKeys(getByteArrayFromLongArray(timestamps));
    req.setValuesList(valueBufferList);
    req.setBitmapList(bitmapBufferList);
    req.setDataTypeList(dataTypeList);
    req.setTagsList(tagsList);
    req.setTimePrecision(timePrecision);

    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        Status status = client.insertNonAlignedColumnRecords(req);
        StatusUtils.verifySuccess(status);
      } catch (TException | SessionException e) {
        throw new IginXException("insert data failure: ", e);
      }
    }
  }

  private void writeRowData(
      List<String> paths,
      long[] timestamps,
      Object[][] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList,
      TimePrecision timePrecision) {
    List<ByteBuffer> valueBufferList = new ArrayList<>();
    List<ByteBuffer> bitmapBufferList = new ArrayList<>();
    for (Object[] values : valuesList) {
      valueBufferList.add(ByteUtils.getRowByteBuffer(values, dataTypeList));
      Bitmap bitmap = new Bitmap(values.length);
      for (int j = 0; j < values.length; j++) {
        if (values[j] != null) {
          bitmap.mark(j);
        }
      }
      bitmapBufferList.add(ByteBuffer.wrap(bitmap.getBytes()));
    }

    InsertNonAlignedRowRecordsReq req = new InsertNonAlignedRowRecordsReq();
    req.setSessionId(sessionId);
    req.setPaths(paths);
    req.setKeys(getByteArrayFromLongArray(timestamps));
    req.setValuesList(valueBufferList);
    req.setBitmapList(bitmapBufferList);
    req.setDataTypeList(dataTypeList);
    req.setTagsList(tagsList);
    req.setTimePrecision(timePrecision);

    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        Status status = client.insertNonAlignedRowRecords(req);
        StatusUtils.verifySuccess(status);
      } catch (TException | SessionException e) {
        throw new IginXException("insert data failure: ", e);
      }
    }
  }
}
