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
package cn.edu.tsinghua.iginx.filestore.server;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.*;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filestore.executor.Executor;
import cn.edu.tsinghua.iginx.filestore.executor.FileStoreException;
import cn.edu.tsinghua.iginx.filestore.rpc.*;
import cn.edu.tsinghua.iginx.filestore.rpc.Status;
import cn.edu.tsinghua.iginx.filestore.rpc.StorageUnit;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.thrift.InsertData;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStoreServerWorker implements FileStoreService.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileStoreServerWorker.class);

  private static final Status SUCCESS = new Status(200, "success");

  private static final Status EXEC_PROJECT_FAIL = new Status(401, "execute project fail");

  private static final Status EXEC_INSERT_FAIL = new Status(402, "execute insert fail");

  private static final Status EXEC_DELETE_FAIL = new Status(403, "execute delete fail");

  private static final Status GET_TS_FAIL = new Status(404, "get time series fail");

  private static final Status GET_BOUNDARY_FAIL = new Status(405, "get boundary of storage fail");

  private final Executor executor;

  public FileStoreServerWorker(Executor executor) {
    this.executor = executor;
  }

  @Override
  public ProjectResp executeProject(ProjectReq req) throws TException {
    String storageUnit = req.getStorageUnit();
    List<String> patterns = req.getPatterns();

    Filter filter = ServerObjectMappingUtils.toFilter(req.getFilter());
    TagFilter tagFilter = ServerObjectMappingUtils.resolveRawTagFilter(req.getTagFilter());

    List<FunctionCall> calls;
    if (req.getAggregations() != null) {
      calls =
          req.getAggregations().stream()
              .map(ServerObjectMappingUtils::resolveRawFunctionCall)
              .collect(Collectors.toList());
    } else {
      calls = null;
    }

    try {
      RowStream rowStream = executor.query(storageUnit, filter, patterns, tagFilter, calls);

      List<String> names = new ArrayList<>();
      List<DataType> types = new ArrayList<>();
      List<DataType> dataTypes = new ArrayList<>();
      List<Map<String, String>> tagsList = new ArrayList<>();
      boolean hasTime = rowStream.getHeader().hasKey();
      rowStream
          .getHeader()
          .getFields()
          .forEach(
              field -> {
                names.add(field.getName());
                types.add(field.getType());
                dataTypes.add(field.getType());
                Map<String, String> tags =
                    field.getTags() == null ? new HashMap<>() : field.getTags();
                tagsList.add(tags);
              });

      RawHeader rawHeader = new RawHeader(names, types, tagsList, hasTime);

      List<RawRow> rawRows = new ArrayList<>();
      while (rowStream.hasNext()) {
        Row row = rowStream.next();
        Object[] rowValues = row.getValues();
        Bitmap bitmap = new Bitmap(rowValues.length);
        for (int j = 0; j < rowValues.length; j++) {
          if (rowValues[j] != null) {
            bitmap.mark(j);
          }
        }
        RawRow rawRow =
            new RawRow(
                ByteUtils.getRowByteBuffer(rowValues, dataTypes),
                ByteBuffer.wrap(bitmap.getBytes()));
        if (hasTime) {
          rawRow.setKey(row.getKey());
        }
        rawRows.add(rawRow);
      }
      ProjectResp resp = new ProjectResp(SUCCESS);
      resp.setHeader(rawHeader);
      resp.setRows(rawRows);
      return resp;
    } catch (PhysicalException e) {
      LOGGER.error("encounter error when query({})", req, e);
      return new ProjectResp(EXEC_PROJECT_FAIL);
    }
  }

  @Override
  public Status executeInsert(InsertReq req) throws TException {
    InsertData insertData = req.getRawData();
    RawDataType rawDataType =
        ServerObjectMappingUtils.strToRawDataType(insertData.getRawDataType());
    if (rawDataType == null) {
      return EXEC_INSERT_FAIL;
    }

    List<String> paths = insertData.getPatterns();
    List<Map<String, String>> tags = insertData.getTagsList();
    long[] timeArray = ByteUtils.getLongArrayFromByteArray(insertData.getKeys());
    List<Long> times = new ArrayList<>();
    Arrays.stream(timeArray).forEach(times::add);

    List<ByteBuffer> valueList = insertData.getValuesList();
    List<ByteBuffer> bitmapList = insertData.getBitmapList();
    List<DataType> types = new ArrayList<>();
    for (String dataType : insertData.getDataTypeList()) {
      types.add(DataTypeUtils.getDataTypeFromString(dataType));
    }

    List<Bitmap> bitmaps;
    Object[] values;
    if (rawDataType == RawDataType.Row || rawDataType == RawDataType.NonAlignedRow) {
      bitmaps =
          insertData.getBitmapList().stream()
              .map(x -> new Bitmap(paths.size(), x.array()))
              .collect(Collectors.toList());
      values = ByteUtils.getRowValuesByDataType(valueList, types, bitmapList);
    } else {
      bitmaps =
          bitmapList.stream()
              .map(x -> new Bitmap(times.size(), x.array()))
              .collect(Collectors.toList());
      values = ByteUtils.getColumnValuesByDataType(valueList, types, bitmapList, times.size());
    }

    RawData rawData = new RawData(paths, tags, times, values, types, bitmaps, rawDataType);

    DataView dataView;
    if (rawDataType == RawDataType.Row || rawDataType == RawDataType.NonAlignedRow) {
      dataView =
          new RowDataView(rawData, 0, rawData.getPaths().size(), 0, rawData.getKeys().size());
    } else {
      dataView =
          new ColumnDataView(rawData, 0, rawData.getPaths().size(), 0, rawData.getKeys().size());
    }

    try {
      executor.insert(req.getStorageUnit(), dataView);
      return SUCCESS;
    } catch (FileStoreException e) {
      LOGGER.error("encounter error when insert", e);
      return EXEC_INSERT_FAIL;
    }
  }

  @Override
  public Status executeDelete(DeleteReq req) throws TException {
    String storageUnit = req.getStorageUnit();
    TagFilter tagFilter = ServerObjectMappingUtils.resolveRawTagFilter(req.getTagFilter());
    Filter filter = ServerObjectMappingUtils.toFilter(req.getFilter());
    List<String> patterns = req.getPatterns();

    try {
      executor.delete(storageUnit, filter, patterns, tagFilter);
      return SUCCESS;
    } catch (FileStoreException e) {
      LOGGER.error("encounter error when delete({})", req, e);
      return EXEC_DELETE_FAIL;
    }
  }

  @Override
  public GetColumnsOfStorageUnitResp getColumnsOfStorageUnit() throws TException {

    try {

      Map<String, Set<Field>> schema = executor.getSchema(null, null);
      Map<StorageUnit, Set<RawField>> rawSchema = new HashMap<>();
      for (Map.Entry<String, Set<Field>> entry : schema.entrySet()) {
        Set<RawField> rawFields = new HashSet<>();
        for (Field field : entry.getValue()) {
          rawFields.add(new RawField(field.getName(), field.getType(), field.getTags()));
        }
        StorageUnit storageUnit = new StorageUnit();
        storageUnit.setName(entry.getKey());
        rawSchema.put(storageUnit, rawFields);
      }
      GetColumnsOfStorageUnitResp resp = new GetColumnsOfStorageUnitResp(SUCCESS);
      resp.setSchemas(rawSchema);
      return resp;
    } catch (PhysicalException e) {
      LOGGER.error("encounter error when getColumnsOfStorageUnit ", e);
      return new GetColumnsOfStorageUnitResp(GET_TS_FAIL);
    }
  }

  @Override
  public GetStorageBoundaryResp getBoundaryOfStorage(String prefix) throws TException {
    try {
      ColumnsInterval columnsInterval = executor.getDummyBoundary(prefix);
      GetStorageBoundaryResp resp = new GetStorageBoundaryResp(SUCCESS);
      resp.setStartColumn(columnsInterval.getStartColumn());
      resp.setEndColumn(columnsInterval.getEndColumn());
      return resp;
    } catch (PhysicalException e) {
      LOGGER.error("encounter error when getBoundaryOfStorage ", e);
      return new GetStorageBoundaryResp(GET_BOUNDARY_FAIL);
    }
  }
}
