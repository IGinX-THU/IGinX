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

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getLongArrayFromByteBuffer;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getValueFromByteBufferByDataType;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session_v2.QueryClient;
import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import cn.edu.tsinghua.iginx.session_v2.query.AggregateQuery;
import cn.edu.tsinghua.iginx.session_v2.query.DownsampleQuery;
import cn.edu.tsinghua.iginx.session_v2.query.IginXColumn;
import cn.edu.tsinghua.iginx.session_v2.query.IginXHeader;
import cn.edu.tsinghua.iginx.session_v2.query.IginXRecord;
import cn.edu.tsinghua.iginx.session_v2.query.IginXTable;
import cn.edu.tsinghua.iginx.session_v2.query.LastQuery;
import cn.edu.tsinghua.iginx.session_v2.query.Query;
import cn.edu.tsinghua.iginx.session_v2.query.SimpleQuery;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryReq;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryReq;
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryResp;
import cn.edu.tsinghua.iginx.thrift.LastQueryReq;
import cn.edu.tsinghua.iginx.thrift.LastQueryResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataReq;
import cn.edu.tsinghua.iginx.thrift.QueryDataResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataSet;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.StatusUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryClientImpl extends AbstractFunctionClient implements QueryClient {
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryClientImpl.class);

  private final ResultMapper resultMapper;

  public QueryClientImpl(IginXClientImpl iginXClient, ResultMapper resultMapper) {
    super(iginXClient);
    this.resultMapper = resultMapper;
  }

  private IginXTable simpleQuery(SimpleQuery query) {
    List<String> measurements = new ArrayList<>(query.getMeasurements());
    QueryDataReq req =
        new QueryDataReq(
            sessionId,
            MeasurementUtils.mergeAndSortMeasurements(measurements),
            query.getStartKey(),
            query.getEndKey());

    if (query.getTagsList() != null) {
      req.setTagsList(query.getTagsList());
    }
    req.setTimePrecision(req.timePrecision);

    QueryDataResp resp;

    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        resp = client.queryData(req);
        StatusUtils.verifySuccess(resp.status);
      } catch (TException | SessionException e) {
        throw new IginXException("simple query failure: ", e);
      }
    }
    return buildIginXTable(
        resp.getQueryDataSet(), resp.getPaths(), resp.getTagsList(), resp.getDataTypeList());
  }

  private IginXTable aggregateQuery(AggregateQuery query) throws IginXException {
    List<String> measurements = new ArrayList<>(query.getMeasurements());
    AggregateQueryReq req =
        new AggregateQueryReq(
            sessionId,
            MeasurementUtils.mergeAndSortMeasurements(measurements),
            query.getStartKey(),
            query.getEndKey(),
            query.getAggregateType());

    if (query.getTagsList() != null) {
      req.setTagsList(query.getTagsList());
    }
    req.setTimePrecision(req.timePrecision);

    AggregateQueryResp resp;

    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        resp = client.aggregateQuery(req);
        StatusUtils.verifySuccess(resp.status);
      } catch (TException | SessionException e) {
        throw new IginXException("aggregate query failure: ", e);
      }
    }

    // 构造结果集
    measurements = resp.getPaths();
    List<Map<String, String>> tagsList = resp.getTagsList();
    List<DataType> dataTypes = resp.getDataTypeList();
    Object[] values = ByteUtils.getValuesByDataType(resp.valuesList, resp.dataTypeList);
    List<IginXColumn> columns = new ArrayList<>();
    Map<String, Object> recordValues = new HashMap<>();
    for (int i = 0; i < measurements.size(); i++) {
      IginXColumn column = new IginXColumn(measurements.get(i), tagsList.get(i), dataTypes.get(i));
      columns.add(column);
      recordValues.put(measurements.get(i), values[i]);
    }
    IginXHeader header = new IginXHeader(columns);
    IginXRecord record = new IginXRecord(header, recordValues);
    return new IginXTable(header, Collections.singletonList(record));
  }

  private IginXTable downsampleQuery(DownsampleQuery query) throws IginXException {
    List<String> measurements = new ArrayList<>(query.getMeasurements());
    DownsampleQueryReq req =
        new DownsampleQueryReq(
            sessionId,
            MeasurementUtils.mergeAndSortMeasurements(measurements),
            query.getStartKey(),
            query.getEndKey(),
            query.getAggregateType(),
            query.getPrecision());

    if (query.getTagsList() != null) {
      req.setTagsList(query.getTagsList());
    }
    req.setTimePrecision(req.timePrecision);

    DownsampleQueryResp resp;
    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        resp = client.downsampleQuery(req);
        StatusUtils.verifySuccess(resp.status);
      } catch (TException | SessionException e) {
        throw new IginXException("downsample query failure: ", e);
      }
    }
    return buildIginXTable(
        resp.getQueryDataSet(), resp.getPaths(), resp.getTagsList(), resp.getDataTypeList());
  }

  private IginXTable lastQuery(LastQuery query) throws IginXException {
    List<String> measurements = new ArrayList<>(query.getMeasurements());
    LastQueryReq req =
        new LastQueryReq(
            sessionId,
            MeasurementUtils.mergeAndSortMeasurements(measurements),
            query.getStartKey());

    if (query.getTagsList() != null) {
      req.setTagsList(query.getTagsList());
    }
    req.setTimePrecision(req.timePrecision);

    LastQueryResp resp;
    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        resp = client.lastQuery(req);
        StatusUtils.verifySuccess(resp.status);
      } catch (TException | SessionException e) {
        throw new IginXException("last query failure: ", e);
      }
    }

    return buildIginXTable(
        resp.getQueryDataSet(), resp.getPaths(), resp.getTagsList(), resp.getDataTypeList());
  }

  @Override
  public <M> List<M> query(Query query, Class<M> measurementType) throws IginXException {
    IginXTable table = query(query);
    return table.getRecords().stream()
        .map(e -> resultMapper.toPOJO(e, measurementType))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  @Override
  public IginXTable query(Query query) throws IginXException {
    IginXTable table;
    if (query instanceof SimpleQuery) {
      table = simpleQuery((SimpleQuery) query);
    } else if (query instanceof LastQuery) {
      table = lastQuery((LastQuery) query);
    } else if (query instanceof DownsampleQuery) {
      table = downsampleQuery((DownsampleQuery) query);
    } else if (query instanceof AggregateQuery) {
      table = aggregateQuery((AggregateQuery) query);
    } else {
      throw new IllegalArgumentException("unknown query type: " + query.getClass());
    }
    return table;
  }

  @Override
  public void query(Query query, Consumer<IginXRecord> onNext) throws IginXException {
    IginXTable table = query(query);
    new ConsumerControlThread(table, onNext).start();
  }

  @Override
  public <M> void query(Query query, Class<M> measurementType, Consumer<M> onNext)
      throws IginXException {
    IginXTable table = query(query);
    new ConsumerMapperControlThread<M>(table, onNext, measurementType, resultMapper).start();
  }

  @Override
  public IginXTable query(String query) throws IginXException {
    // TODO: 执行查询 SQL
    return null;
  }

  @Override
  public <M> void query(String query, Class<M> measurementType, Consumer<M> onNext)
      throws IginXException {
    IginXTable table = query(query);
    new ConsumerMapperControlThread<M>(table, onNext, measurementType, resultMapper).start();
  }

  @Override
  public void query(String query, Consumer<IginXRecord> onNext) throws IginXException {
    IginXTable table = query(query);
    new ConsumerControlThread(table, onNext).start();
  }

  @Override
  public <M> List<M> query(String query, Class<M> measurementType) throws IginXException {
    IginXTable table = query(query);
    return table.getRecords().stream()
        .map(e -> resultMapper.toPOJO(e, measurementType))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private IginXTable buildIginXTable(
      QueryDataSet dataSet,
      List<String> measurements,
      List<Map<String, String>> tagsList,
      List<DataType> dataTypes) {
    boolean hasTimestamp = dataSet.getKeys() != null;
    long[] timestamps = new long[0];
    if (hasTimestamp) {
      timestamps = getLongArrayFromByteBuffer(dataSet.keys);
    }

    List<IginXColumn> columns = new ArrayList<>();
    Map<Integer, String> columnIndexMap = new HashMap<>();
    for (int i = 0; i < measurements.size(); i++) {
      String measurement = measurements.get(i);
      DataType dataType = dataTypes.get(i);
      Map<String, String> tags = tagsList.get(i);
      columns.add(new IginXColumn(measurement, tags, dataType));
      columnIndexMap.put(i, measurement);
    }
    IginXHeader header = new IginXHeader(hasTimestamp ? IginXColumn.TIME : null, columns);
    List<IginXRecord> records = new ArrayList<>();
    for (int i = 0; i < dataSet.valuesList.size(); i++) {
      ByteBuffer valuesBuffer = dataSet.valuesList.get(i);
      ByteBuffer bitmapBuffer = dataSet.bitmapList.get(i);
      Bitmap bitmap = new Bitmap(dataTypes.size(), bitmapBuffer.array());
      Map<String, Object> values = new HashMap<>();
      for (int j = 0; j < dataTypes.size(); j++) {
        if (bitmap.get(j)) {
          values.put(
              columnIndexMap.get(j),
              getValueFromByteBufferByDataType(valuesBuffer, dataTypes.get(j)));
        }
      }
      records.add(new IginXRecord(hasTimestamp ? timestamps[i] : 0L, header, values));
    }
    return new IginXTable(header, records);
  }

  private static class ConsumerControlThread extends Thread {

    private final IginXTable table;

    private final Consumer<IginXRecord> onNext;

    public ConsumerControlThread(IginXTable table, Consumer<IginXRecord> onNext) {
      this.table = table;
      this.onNext = onNext;
    }

    @Override
    public void run() {
      List<IginXRecord> records = table.getRecords();
      for (IginXRecord record : records) {
        onNext.accept(record);
      }
    }
  }

  private static class ConsumerMapperControlThread<M> extends Thread {

    private final IginXTable table;

    private final Consumer<M> onNext;

    private final Class<M> measurementType;

    private final ResultMapper resultMapper;

    public ConsumerMapperControlThread(
        IginXTable table, Consumer<M> onNext, Class<M> measurementType, ResultMapper resultMapper) {
      this.table = table;
      this.onNext = onNext;
      this.measurementType = measurementType;
      this.resultMapper = resultMapper;
    }

    @Override
    public void run() {
      List<IginXRecord> records = table.getRecords();
      for (IginXRecord record : records) {
        onNext.accept(resultMapper.toPOJO(record, measurementType));
      }
    }
  }
}
