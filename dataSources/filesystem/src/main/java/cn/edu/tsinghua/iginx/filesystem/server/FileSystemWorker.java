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
package cn.edu.tsinghua.iginx.filesystem.server;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.*;
import cn.edu.tsinghua.iginx.filesystem.exec.Executor;
import cn.edu.tsinghua.iginx.filesystem.thrift.*;
import cn.edu.tsinghua.iginx.filesystem.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemWorker implements FileSystemService.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemWorker.class);

  private static final Status SUCCESS = new Status(200, "success");

  private static final Status EXEC_PROJECT_FAIL = new Status(401, "execute project fail");

  private static final Status EXEC_INSERT_FAIL = new Status(402, "execute insert fail");

  private static final Status EXEC_DELETE_FAIL = new Status(403, "execute delete fail");

  private static final Status GET_COLUMNS_FAIL = new Status(404, "get columns fail");

  private static final Status GET_BOUNDARY_FAIL = new Status(405, "get boundary of storage fail");

  private final Executor executor;

  public FileSystemWorker(Executor executor) {
    this.executor = executor;
  }

  @Override
  public ProjectResp executeProject(ProjectReq req) throws TException {
    TagFilter tagFilter = resolveRawTagFilter(req.getTagFilter());
    TaskExecuteResult result =
        executor.executeProjectTask(
            req.getPaths(),
            tagFilter,
            FilterTransformer.toFilter(req.getFilter()),
            req.getStorageUnit(),
            req.isDummyStorageUnit);

    RowStream rowStream = result.getRowStream();

    if (result.getException() != null || rowStream == null) {
      return new ProjectResp(EXEC_PROJECT_FAIL);
    }

    List<String> names = new ArrayList<>();
    List<String> types = new ArrayList<>();
    List<DataType> dataTypes = new ArrayList<>();
    List<Map<String, String>> tagsList = new ArrayList<>();
    boolean hasKey;
    try {
      hasKey = rowStream.getHeader().hasKey();
      rowStream
          .getHeader()
          .getFields()
          .forEach(
              field -> {
                names.add(field.getName());
                types.add(field.getType().toString());
                dataTypes.add(field.getType());
                Map<String, String> tags =
                    field.getTags() == null ? new HashMap<>() : field.getTags();
                tagsList.add(tags);
              });
    } catch (PhysicalException e) {
      LOGGER.error("encounter error when getting header from RowStream ", e);
      return new ProjectResp(EXEC_PROJECT_FAIL);
    }
    FSHeader fsHeader = new FSHeader(names, types, tagsList, hasKey);

    List<FSRow> fsRows = new ArrayList<>();
    try {
      while (rowStream.hasNext()) {
        Row row = rowStream.next();
        Object[] rowValues = row.getValues();
        Bitmap bitmap = new Bitmap(rowValues.length);
        for (int j = 0; j < rowValues.length; j++) {
          if (rowValues[j] != null) {
            bitmap.mark(j);
          }
        }
        FSRow fsRow =
            new FSRow(
                ByteUtils.getRowByteBuffer(rowValues, dataTypes),
                ByteBuffer.wrap(bitmap.getBytes()));
        if (hasKey) {
          fsRow.setKey(row.getKey());
        }
        fsRows.add(fsRow);
      }
    } catch (PhysicalException e) {
      LOGGER.error("encounter error when getting result from RowStream ", e);
      return new ProjectResp(EXEC_PROJECT_FAIL);
    }

    ProjectResp resp = new ProjectResp(SUCCESS);
    resp.setHeader(fsHeader);
    resp.setRows(fsRows);
    return resp;
  }

  @Override
  public Status executeInsert(InsertReq req) throws TException {
    FSRawData fsRawData = req.getRawData();
    RawDataType rawDataType = strToRawDataType(fsRawData.getRawDataType());
    if (rawDataType == null) {
      return EXEC_INSERT_FAIL;
    }

    List<String> paths = fsRawData.getPaths();
    long[] keyArray = ByteUtils.getLongArrayFromByteArray(fsRawData.getKeys());
    List<Long> keys = new ArrayList<>();
    Arrays.stream(keyArray).forEach(keys::add);
    List<ByteBuffer> valueList = fsRawData.getValuesList();
    List<ByteBuffer> bitmapList = fsRawData.getBitmapList();
    List<DataType> types = new ArrayList<>();
    for (String dataType : fsRawData.getDataTypeList()) {
      types.add(DataTypeUtils.getDataTypeFromString(dataType));
    }

    List<Bitmap> bitmaps;
    Object[] values;
    if (rawDataType == RawDataType.Row || rawDataType == RawDataType.NonAlignedRow) {
      bitmaps =
          fsRawData.getBitmapList().stream()
              .map(x -> new Bitmap(paths.size(), x.array()))
              .collect(Collectors.toList());
      values = ByteUtils.getRowValuesByDataType(valueList, types, bitmapList);
    } else {
      bitmaps =
          bitmapList.stream()
              .map(x -> new Bitmap(keys.size(), x.array()))
              .collect(Collectors.toList());
      values = ByteUtils.getColumnValuesByDataType(valueList, types, bitmapList, keys.size());
    }

    RawData rawData =
        new RawData(paths, fsRawData.getTagsList(), keys, values, types, bitmaps, rawDataType);

    DataView dataView;
    if (rawDataType == RawDataType.Row || rawDataType == RawDataType.NonAlignedRow) {
      dataView =
          new RowDataView(rawData, 0, rawData.getPaths().size(), 0, rawData.getKeys().size());
    } else {
      dataView =
          new ColumnDataView(rawData, 0, rawData.getPaths().size(), 0, rawData.getKeys().size());
    }

    TaskExecuteResult result = executor.executeInsertTask(dataView, req.getStorageUnit());
    if (result.getException() == null) {
      return SUCCESS;
    } else {
      return EXEC_INSERT_FAIL;
    }
  }

  @Override
  public Status executeDelete(DeleteReq req) throws TException {
    TagFilter tagFilter = resolveRawTagFilter(req.getTagFilter());

    // null keyRanges means delete key
    List<KeyRange> keyRanges = null;
    if (req.isSetKeyRanges()) {
      keyRanges = new ArrayList<>();
      for (FSKeyRange range : req.getKeyRanges()) {
        keyRanges.add(new KeyRange(range.getBeginKey(), range.getEndKey()));
      }
    }

    TaskExecuteResult result =
        executor.executeDeleteTask(req.getPaths(), keyRanges, tagFilter, req.getStorageUnit());
    if (result.getException() == null) {
      return SUCCESS;
    } else {
      return EXEC_DELETE_FAIL;
    }
  }

  @Override
  public GetColumnsOfStorageUnitResp getColumnsOfStorageUnit(String storageUnit) throws TException {
    List<FSColumn> ret = new ArrayList<>();
    try {
      List<Column> columns = executor.getColumnsOfStorageUnit(storageUnit);
      columns.forEach(
          column -> {
            FSColumn fsColumn =
                new FSColumn(column.getPath(), column.getDataType().toString(), column.isDummy());
            if (column.getTags() != null) {
              fsColumn.setTags(column.getTags());
            }
            ret.add(fsColumn);
          });
      GetColumnsOfStorageUnitResp resp = new GetColumnsOfStorageUnitResp(SUCCESS);
      resp.setPathList(ret);
      return resp;
    } catch (PhysicalException e) {
      LOGGER.error("encounter error when geColumnsOfStorageUnit ", e);
      return new GetColumnsOfStorageUnitResp(GET_COLUMNS_FAIL);
    }
  }

  @Override
  public GetBoundaryOfStorageResp getBoundaryOfStorage(String prefix) throws TException {
    try {
      Pair<ColumnsInterval, KeyInterval> pair = executor.getBoundaryOfStorage(prefix);
      GetBoundaryOfStorageResp resp = new GetBoundaryOfStorageResp(SUCCESS);
      resp.setStartKey(pair.getV().getStartKey());
      resp.setEndKey(pair.getV().getEndKey());
      resp.setStartColumn(pair.getK().getStartColumn());
      resp.setEndColumn(pair.getK().getEndColumn());
      return resp;
    } catch (PhysicalException e) {
      LOGGER.error("encounter error when getBoundaryOfStorage ", e);
      return new GetBoundaryOfStorageResp(GET_BOUNDARY_FAIL);
    }
  }

  private TagFilter resolveRawTagFilter(RawTagFilter rawTagFilter) {
    if (rawTagFilter == null) {
      return null;
    }
    switch (rawTagFilter.getType()) {
      case Base:
        return new BaseTagFilter(rawTagFilter.getKey(), rawTagFilter.getValue());
      case WithoutTag:
        return new WithoutTagFilter();
      case BasePrecise:
        return new BasePreciseTagFilter(rawTagFilter.getTags());
      case Precise:
        {
          List<BasePreciseTagFilter> children = new ArrayList<>();
          rawTagFilter
              .getChildren()
              .forEach(child -> children.add((BasePreciseTagFilter) resolveRawTagFilter(child)));
          return new PreciseTagFilter(children);
        }
      case And:
        {
          List<TagFilter> children = new ArrayList<>();
          rawTagFilter.getChildren().forEach(child -> children.add(resolveRawTagFilter(child)));
          return new AndTagFilter(children);
        }
      case Or:
        {
          List<TagFilter> children = new ArrayList<>();
          rawTagFilter.getChildren().forEach(child -> children.add(resolveRawTagFilter(child)));
          return new OrTagFilter(children);
        }
      default:
        {
          LOGGER.error("unknown tag filter type: {}", rawTagFilter.getType());
          return null;
        }
    }
  }

  private RawDataType strToRawDataType(String type) {
    switch (type.toLowerCase()) {
      case "row":
        return RawDataType.Row;
      case "nonalignedrow":
        return RawDataType.NonAlignedRow;
      case "column":
        return RawDataType.Column;
      case "nonalignedcolumn":
        return RawDataType.NonAlignedColumn;
      default:
        return null;
    }
  }
}
