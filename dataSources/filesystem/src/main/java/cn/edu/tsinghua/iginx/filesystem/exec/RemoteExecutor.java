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
package cn.edu.tsinghua.iginx.filesystem.exec;

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
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.*;
import cn.edu.tsinghua.iginx.filesystem.exception.FilesystemException;
import cn.edu.tsinghua.iginx.filesystem.thrift.*;
import cn.edu.tsinghua.iginx.filesystem.thrift.FileSystemService.Client;
import cn.edu.tsinghua.iginx.filesystem.thrift.TagFilterType;
import cn.edu.tsinghua.iginx.filesystem.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
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
import java.util.stream.Collectors;
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
    if (filter != null && !filter.toString().isEmpty()) {
      req.setFilter(FilterTransformer.toFSFilter(filter));
    }
    try {
      TTransport transport = thriftConnPool.borrowTransport();
      Client client = new Client(new TBinaryProtocol(transport));
      ProjectResp resp = client.executeProject(req);
      thriftConnPool.returnTransport(transport);
      if (resp.getStatus().code == SUCCESS_CODE) {
        FSHeader fileDataHeader = resp.getHeader();
        List<DataType> dataTypes = new ArrayList<>();
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < fileDataHeader.getNamesSize(); i++) {
          DataType dataType = DataTypeUtils.getDataTypeFromString(fileDataHeader.getTypes().get(i));
          dataTypes.add(dataType);
          fields.add(
              new Field(
                  fileDataHeader.getNames().get(i), dataType, fileDataHeader.getTagsList().get(i)));
        }
        Header header = fileDataHeader.hasKey ? new Header(Field.KEY, fields) : new Header(fields);

        List<Row> rowList = new ArrayList<>();
        resp.getRows()
            .forEach(
                fileDataRow -> {
                  Object[] values = new Object[dataTypes.size()];
                  Bitmap bitmap = new Bitmap(dataTypes.size(), fileDataRow.getBitmap());
                  ByteBuffer valuesBuffer = ByteBuffer.wrap(fileDataRow.getRowValues());
                  for (int i = 0; i < dataTypes.size(); i++) {
                    if (bitmap.get(i)) {
                      values[i] =
                          ByteUtils.getValueFromByteBufferByDataType(
                              valuesBuffer, dataTypes.get(i));
                    } else {
                      values[i] = null;
                    }
                  }

                  if (fileDataRow.isSetKey()) {
                    rowList.add(new Row(header, fileDataRow.getKey(), values));
                  } else {
                    rowList.add(new Row(header, values));
                  }
                });
        RowStream rowStream = new Table(header, rowList);
        return new TaskExecuteResult(rowStream, null);
      } else {
        return new TaskExecuteResult(
            null, new FilesystemException("execute remote project task error"));
      }
    } catch (TException e) {
      return new TaskExecuteResult(null, new FilesystemException(e));
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

    long[] keys = new long[dataView.getKeySize()];
    for (int i = 0; i < dataView.getKeySize(); i++) {
      keys[i] = dataView.getKey(i);
    }

    Pair<List<ByteBuffer>, List<ByteBuffer>> pair;
    if (dataView.getRawDataType() == RawDataType.Row
        || dataView.getRawDataType() == RawDataType.NonAlignedRow) {
      pair = compressRowData(dataView);
    } else {
      pair = compressColData(dataView);
    }

    FSRawData fileDataRawData =
        new FSRawData(
            paths,
            tagsList,
            ByteBuffer.wrap(ByteUtils.getByteArrayFromLongArray(keys)),
            pair.getK(),
            pair.getV(),
            types,
            dataView.getRawDataType().toString());

    InsertReq req = new InsertReq(storageUnit, fileDataRawData);
    try {
      TTransport transport = thriftConnPool.borrowTransport();
      Client client = new Client(new TBinaryProtocol(transport));
      Status status = client.executeInsert(req);
      thriftConnPool.returnTransport(transport);
      if (status.code == SUCCESS_CODE) {
        return new TaskExecuteResult(null, null);
      } else {
        return new TaskExecuteResult(
            null, new FilesystemException("execute remote insert task error"));
      }
    } catch (TException e) {
      return new TaskExecuteResult(null, new FilesystemException(e));
    }
  }

  @Override
  public TaskExecuteResult executeDeleteTask(
      List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter, String storageUnit) {
    DeleteReq req = new DeleteReq(storageUnit, paths);
    if (tagFilter != null) {
      req.setTagFilter(constructRawTagFilter(tagFilter));
    }
    if (keyRanges != null) {
      List<FSKeyRange> fsKeyRanges = new ArrayList<>();
      keyRanges.forEach(
          keyRange ->
              fsKeyRanges.add(
                  new FSKeyRange(
                      keyRange.getBeginKey(),
                      keyRange.isIncludeBeginKey(),
                      keyRange.getEndKey(),
                      keyRange.isIncludeEndKey())));
      req.setKeyRanges(fsKeyRanges);
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
            null, new FilesystemException("execute remote delete task error"));
      }
    } catch (TException e) {
      return new TaskExecuteResult(null, new FilesystemException(e));
    }
  }

  @Override
  public List<Column> getColumnsOfStorageUnit(String storageUnit) throws PhysicalException {
    try {
      TTransport transport = thriftConnPool.borrowTransport();
      Client client = new Client(new TBinaryProtocol(transport));
      GetColumnsOfStorageUnitResp resp = client.getColumnsOfStorageUnit(storageUnit);
      thriftConnPool.returnTransport(transport);
      List<Column> columns = new ArrayList<>();
      resp.getPathList()
          .forEach(
              column ->
                  columns.add(
                      new Column(
                          column.getPath(),
                          DataTypeUtils.getDataTypeFromString(column.getDataType()),
                          column.getTags(),
                          column.isDummy())));
      return columns;
    } catch (TException e) {
      throw new FilesystemException(
          "encounter error when executing remote getColumnsOfStorageUnit task", e);
    }
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String dataPrefix)
      throws PhysicalException {
    try {
      TTransport transport = thriftConnPool.borrowTransport();
      Client client = new Client(new TBinaryProtocol(transport));
      GetBoundaryOfStorageResp resp = client.getBoundaryOfStorage(dataPrefix);
      thriftConnPool.returnTransport(transport);
      return new Pair<>(
          new ColumnsInterval(resp.getStartColumn(), resp.getEndColumn()),
          new KeyInterval(resp.getStartKey(), resp.getEndKey()));
    } catch (TException e) {
      throw new FilesystemException(
          "encounter error when executing remote getBoundaryOfStorage task", e);
    }
  }

  @Override
  public void close() {
    thriftConnPool.close();
  }

  private RawTagFilter constructRawTagFilter(TagFilter tagFilter) {
    RawTagFilter filter = null;
    switch (tagFilter.getType()) {
      case Base:
        {
          BaseTagFilter baseTagFilter = (BaseTagFilter) tagFilter;
          filter = new RawTagFilter(TagFilterType.Base);
          filter.setKey(baseTagFilter.getTagKey());
          filter.setValue(baseTagFilter.getTagValue());
          break;
        }
      case WithoutTag:
        {
          filter = new RawTagFilter(TagFilterType.WithoutTag);
          break;
        }
      case BasePrecise:
        {
          BasePreciseTagFilter basePreciseTagFilter = (BasePreciseTagFilter) tagFilter;
          filter = new RawTagFilter(TagFilterType.BasePrecise);
          filter.setTags(basePreciseTagFilter.getTags());
          break;
        }
      case Precise:
        {
          PreciseTagFilter preciseTagFilter = (PreciseTagFilter) tagFilter;
          filter = new RawTagFilter(TagFilterType.Precise);
          filter.setChildren(
              preciseTagFilter.getChildren().stream()
                  .map(this::constructRawTagFilter)
                  .collect(Collectors.toList()));
          break;
        }
      case And:
        {
          AndTagFilter andTagFilter = (AndTagFilter) tagFilter;
          filter = new RawTagFilter(TagFilterType.And);
          filter.setChildren(
              andTagFilter.getChildren().stream()
                  .map(this::constructRawTagFilter)
                  .collect(Collectors.toList()));
          break;
        }
      case Or:
        {
          OrTagFilter orTagFilter = (OrTagFilter) tagFilter;
          filter = new RawTagFilter(TagFilterType.Or);
          filter.setChildren(
              orTagFilter.getChildren().stream()
                  .map(this::constructRawTagFilter)
                  .collect(Collectors.toList()));
          break;
        }
      default:
        {
          LOGGER.error("unknown tag filter type: {}", tagFilter.getType());
        }
    }
    return filter;
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
}
