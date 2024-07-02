package cn.edu.tsinghua.iginx.parquet.exec;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.AndTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.BasePreciseTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.BaseTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.OrTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.PreciseTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.parquet.server.FilterTransformer;
import cn.edu.tsinghua.iginx.parquet.thrift.*;
import cn.edu.tsinghua.iginx.parquet.thrift.ParquetService.Client;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.ThriftConnPool;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteExecutor implements Executor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteExecutor.class);

  private static final int SUCCESS_CODE = 200;

  private final ThriftConnPool thriftConnPool;

  public RemoteExecutor(String ip, int port, Map<String, String> extraParams)
      throws TTransportException {
    this.thriftConnPool = new ThriftConnPool(ip, port, extraParams);
  }

  @Override
  public TaskExecuteResult executeProjectTask(
      List<String> paths,
      TagFilter tagFilter,
      Filter filter,
      String storageUnit,
      boolean isDummyStorageUnit) {
    ProjectReq req = new ProjectReq(storageUnit, isDummyStorageUnit, paths);
    if (tagFilter != null) {
      req.setTagFilter(constructRawTagFilter(tagFilter));
    }
    if (filter != null) {
      req.setFilter(FilterTransformer.toRawFilter(filter));
    }

    try {
      TTransport transport = thriftConnPool.borrowTransport();
      Client client = new Client(new TBinaryProtocol(transport));
      ProjectResp resp = client.executeProject(req);
      thriftConnPool.returnTransport(transport);
      if (resp.getStatus().code == SUCCESS_CODE) {
        ParquetHeader parquetHeader = resp.getHeader();
        List<DataType> dataTypes = new ArrayList<>();
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < parquetHeader.getNamesSize(); i++) {
          DataType dataType = DataTypeUtils.getDataTypeFromString(parquetHeader.getTypes().get(i));
          dataTypes.add(dataType);
          Map<String, String> tags = parquetHeader.getTagsList().get(i);
          fields.add(new Field(parquetHeader.getNames().get(i), dataType, tags));
        }
        Header header = parquetHeader.hasKey ? new Header(Field.KEY, fields) : new Header(fields);

        List<Row> rowList = new ArrayList<>();
        resp.getRows()
            .forEach(
                parquetRow -> {
                  Object[] values = new Object[dataTypes.size()];
                  Bitmap bitmap = new Bitmap(dataTypes.size(), parquetRow.getBitmap());
                  ByteBuffer valuesBuffer = ByteBuffer.wrap(parquetRow.getRowValues());
                  for (int i = 0; i < dataTypes.size(); i++) {
                    if (bitmap.get(i)) {
                      values[i] =
                          ByteUtils.getValueFromByteBufferByDataType(
                              valuesBuffer, dataTypes.get(i));
                    } else {
                      values[i] = null;
                    }
                  }

                  if (parquetRow.isSetKey()) {
                    rowList.add(new Row(header, parquetRow.getKey(), values));
                  } else {
                    rowList.add(new Row(header, values));
                  }
                });
        RowStream rowStream = new Table(header, rowList);
        return new TaskExecuteResult(rowStream, null);
      } else {
        return new TaskExecuteResult(
            null, new PhysicalException("execute remote project task error"));
      }
    } catch (TException e) {
      return new TaskExecuteResult(null, new PhysicalException(e));
    }
  }

  @Override
  public TaskExecuteResult executeInsertTask(DataView dataView, String storageUnit) {
    List<String> paths = new ArrayList<>();
    List<String> types = new ArrayList<>();
    List<Map<String, String>> tagsList = new ArrayList<>();
    for (int i = 0; i < dataView.getPathNum(); i++) {
      paths.add(dataView.getPath(i));
      types.add(dataView.getDataType(i).toString());
      tagsList.add(dataView.getTags(i) == null ? new HashMap<>() : dataView.getTags(i));
    }

    long[] times = new long[dataView.getKeySize()];
    for (int i = 0; i < dataView.getKeySize(); i++) {
      times[i] = dataView.getKey(i);
    }

    Pair<List<ByteBuffer>, List<ByteBuffer>> pair;
    if (dataView.getRawDataType() == RawDataType.Row
        || dataView.getRawDataType() == RawDataType.NonAlignedRow) {
      pair = compressRowData(dataView);
    } else {
      pair = compressColData(dataView);
    }

    ParquetRawData parquetRawData =
        new ParquetRawData(
            paths,
            tagsList,
            ByteBuffer.wrap(ByteUtils.getByteArrayFromLongArray(times)),
            pair.getK(),
            pair.getV(),
            types,
            dataView.getRawDataType().toString());

    InsertReq req = new InsertReq(storageUnit, parquetRawData);
    try {
      TTransport transport = thriftConnPool.borrowTransport();
      Client client = new Client(new TBinaryProtocol(transport));
      Status status = client.executeInsert(req);
      thriftConnPool.returnTransport(transport);
      if (status.code == SUCCESS_CODE) {
        return new TaskExecuteResult(null, null);
      } else {
        return new TaskExecuteResult(
            null, new PhysicalException("execute remote insert task error"));
      }
    } catch (TException e) {
      return new TaskExecuteResult(null, new PhysicalException(e));
    }
  }

  private Pair<List<ByteBuffer>, List<ByteBuffer>> compressRowData(DataView dataView) {
    List<ByteBuffer> valueBufferList = new ArrayList<>();
    List<ByteBuffer> bitmapBufferList = new ArrayList<>();

    List<DataType> dataTypeList = new ArrayList<>();
    for (int i = 0; i < dataView.getPathNum(); i++) {
      dataTypeList.add(dataView.getDataType(i));
    }

    for (int i = 0; i < dataView.getKeySize(); i++) {
      BitmapView bitmapView = dataView.getBitmapView(i);
      Object[] values = new Object[dataView.getPathNum()];

      int index = 0;
      for (int j = 0; j < dataView.getPathNum(); j++) {
        if (bitmapView.get(j)) {
          values[j] = dataView.getValue(i, index);
          index++;
        } else {
          values[j] = null;
        }
      }
      valueBufferList.add(ByteUtils.getRowByteBuffer(values, dataTypeList));
      bitmapBufferList.add(ByteBuffer.wrap(bitmapView.getBitmap().getBytes()));
    }
    return new Pair<>(valueBufferList, bitmapBufferList);
  }

  private Pair<List<ByteBuffer>, List<ByteBuffer>> compressColData(DataView dataView) {
    List<ByteBuffer> valueBufferList = new ArrayList<>();
    List<ByteBuffer> bitmapBufferList = new ArrayList<>();

    for (int i = 0; i < dataView.getPathNum(); i++) {
      DataType dataType = dataView.getDataType(i);
      BitmapView bitmapView = dataView.getBitmapView(i);
      Object[] values = new Object[dataView.getKeySize()];

      int index = 0;
      for (int j = 0; j < dataView.getKeySize(); j++) {
        if (bitmapView.get(j)) {
          values[j] = dataView.getValue(i, index);
          index++;
        } else {
          values[j] = null;
        }
      }
      valueBufferList.add(ByteUtils.getColumnByteBuffer(values, dataType));
      bitmapBufferList.add(ByteBuffer.wrap(bitmapView.getBitmap().getBytes()));
    }
    return new Pair<>(valueBufferList, bitmapBufferList);
  }

  @Override
  public TaskExecuteResult executeDeleteTask(
      List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter, String storageUnit) {
    DeleteReq req = new DeleteReq(storageUnit, paths);
    if (tagFilter != null) {
      req.setTagFilter(constructRawTagFilter(tagFilter));
    }
    if (keyRanges != null) {
      List<ParquetKeyRange> parquetKeyRanges = new ArrayList<>();
      keyRanges.forEach(
          timeRange ->
              parquetKeyRanges.add(
                  new ParquetKeyRange(
                      timeRange.getBeginKey(),
                      timeRange.isIncludeBeginKey(),
                      timeRange.getEndKey(),
                      timeRange.isIncludeEndKey())));
      req.setKeyRanges(parquetKeyRanges);
    }

    try {
      TTransport transport = thriftConnPool.borrowTransport();
      Client client = new Client(new TBinaryProtocol(transport));
      Status status = client.executeDelete(req);
      thriftConnPool.returnTransport(transport);
      if (status.code == SUCCESS_CODE) {
        return new TaskExecuteResult(null, null);
      } else {
        return new TaskExecuteResult(
            null, new PhysicalException("execute remote delete task error"));
      }
    } catch (TException e) {
      return new TaskExecuteResult(null, new PhysicalException(e));
    }
  }

  private RawTagFilter constructRawTagFilter(TagFilter tagFilter) {
    switch (tagFilter.getType()) {
      case Base:
        {
          BaseTagFilter baseTagFilter = (BaseTagFilter) tagFilter;
          RawTagFilter filter = new RawTagFilter(TagFilterType.Base);
          filter.setKey(baseTagFilter.getTagKey());
          filter.setValue(baseTagFilter.getTagValue());
          return filter;
        }
      case WithoutTag:
        {
          return new RawTagFilter(TagFilterType.WithoutTag);
        }
      case BasePrecise:
        {
          BasePreciseTagFilter basePreciseTagFilter = (BasePreciseTagFilter) tagFilter;
          RawTagFilter filter = new RawTagFilter(TagFilterType.BasePrecise);
          filter.setTags(basePreciseTagFilter.getTags());
          return filter;
        }
      case Precise:
        {
          PreciseTagFilter preciseTagFilter = (PreciseTagFilter) tagFilter;
          RawTagFilter filter = new RawTagFilter(TagFilterType.Precise);
          List<RawTagFilter> children = new ArrayList<>();
          preciseTagFilter
              .getChildren()
              .forEach(child -> children.add(constructRawTagFilter(child)));
          filter.setChildren(children);
          return filter;
        }
      case And:
        {
          AndTagFilter andTagFilter = (AndTagFilter) tagFilter;
          RawTagFilter filter = new RawTagFilter(TagFilterType.And);
          List<RawTagFilter> children = new ArrayList<>();
          andTagFilter.getChildren().forEach(child -> children.add(constructRawTagFilter(child)));
          filter.setChildren(children);
          return filter;
        }
      case Or:
        {
          OrTagFilter orTagFilter = (OrTagFilter) tagFilter;
          RawTagFilter filter = new RawTagFilter(TagFilterType.Or);
          List<RawTagFilter> children = new ArrayList<>();
          orTagFilter.getChildren().forEach(child -> children.add(constructRawTagFilter(child)));
          filter.setChildren(children);
          return filter;
        }
      default:
        {
          LOGGER.error("unknown tag filter type: {}", tagFilter.getType());
          return null;
        }
    }
  }

  @Override
  public List<Column> getColumnsOfStorageUnit(String storageUnit) throws PhysicalException {
    try {
      TTransport transport = thriftConnPool.borrowTransport();
      Client client = new Client(new TBinaryProtocol(transport));
      GetColumnsOfStorageUnitResp resp = client.getColumnsOfStorageUnit(storageUnit);
      thriftConnPool.returnTransport(transport);
      List<Column> columnList = new ArrayList<>();
      resp.getTsList()
          .forEach(
              ts ->
                  columnList.add(
                      new Column(
                          ts.getPath(),
                          DataTypeUtils.getDataTypeFromString(ts.getDataType()),
                          ts.getTags())));
      return columnList;
    } catch (TException e) {
      throw new PhysicalException("encounter error when getColumnsOfStorageUnit ", e);
    }
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String dataPrefix)
      throws PhysicalException {
    try {
      TTransport transport = thriftConnPool.borrowTransport();
      Client client = new Client(new TBinaryProtocol(transport));
      GetStorageBoundaryResp resp = client.getBoundaryOfStorage(dataPrefix);
      thriftConnPool.returnTransport(transport);
      return new Pair<>(
          new ColumnsInterval(resp.getStartColumn(), resp.getEndColumn()),
          new KeyInterval(resp.getStartKey(), resp.getEndKey()));
    } catch (TException e) {
      throw new PhysicalException("encounter error when getBoundaryOfStorage ", e);
    }
  }

  @Override
  public void close() throws PhysicalException {
    thriftConnPool.close();
  }
}
