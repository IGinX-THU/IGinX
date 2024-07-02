package cn.edu.tsinghua.iginx.parquet.server;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.ColumnDataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawData;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.AndTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.BasePreciseTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.BaseTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.OrTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.PreciseTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.WithoutTagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.parquet.exec.Executor;
import cn.edu.tsinghua.iginx.parquet.thrift.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetWorker implements ParquetService.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetWorker.class);

  private static final Status SUCCESS = new Status(200, "success");

  private static final Status EXEC_PROJECT_FAIL = new Status(401, "execute project fail");

  private static final Status EXEC_INSERT_FAIL = new Status(402, "execute insert fail");

  private static final Status EXEC_DELETE_FAIL = new Status(403, "execute delete fail");

  private static final Status GET_TS_FAIL = new Status(404, "get time series fail");

  private static final Status GET_BOUNDARY_FAIL = new Status(405, "get boundary of storage fail");

  private final Executor executor;

  public ParquetWorker(Executor executor) {
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
    boolean hasTime;
    try {
      hasTime = rowStream.getHeader().hasKey();
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
      LOGGER.error("encounter error when get header from RowStream ", e);
      return new ProjectResp(EXEC_PROJECT_FAIL);
    }
    ParquetHeader parquetHeader = new ParquetHeader(names, types, tagsList, hasTime);

    List<ParquetRow> parquetRows = new ArrayList<>();
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
        ParquetRow parquetRow =
            new ParquetRow(
                ByteUtils.getRowByteBuffer(rowValues, dataTypes),
                ByteBuffer.wrap(bitmap.getBytes()));
        if (hasTime) {
          parquetRow.setKey(row.getKey());
        }
        parquetRows.add(parquetRow);
      }
    } catch (PhysicalException e) {
      LOGGER.error("encounter error when get result from RowStream ", e);
      return new ProjectResp(EXEC_PROJECT_FAIL);
    }

    ProjectResp resp = new ProjectResp(SUCCESS);
    resp.setHeader(parquetHeader);
    resp.setRows(parquetRows);
    return resp;
  }

  @Override
  public Status executeInsert(InsertReq req) throws TException {
    ParquetRawData parquetRawData = req.getRawData();
    RawDataType rawDataType = strToRawDataType(parquetRawData.getRawDataType());
    if (rawDataType == null) {
      return EXEC_INSERT_FAIL;
    }

    List<String> paths = parquetRawData.getPaths();
    long[] timeArray = ByteUtils.getLongArrayFromByteArray(parquetRawData.getKeys());
    List<Long> times = new ArrayList<>();
    Arrays.stream(timeArray).forEach(times::add);
    List<ByteBuffer> valueList = parquetRawData.getValuesList();
    List<ByteBuffer> bitmapList = parquetRawData.getBitmapList();
    List<DataType> types = new ArrayList<>();
    for (String dataType : parquetRawData.getDataTypeList()) {
      types.add(DataTypeUtils.getDataTypeFromString(dataType));
    }

    List<Bitmap> bitmaps;
    Object[] values;
    if (rawDataType == RawDataType.Row || rawDataType == RawDataType.NonAlignedRow) {
      bitmaps =
          parquetRawData.getBitmapList().stream()
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

    RawData rawData =
        new RawData(
            paths, parquetRawData.getTagsList(), times, values, types, bitmaps, rawDataType);

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

  @Override
  public Status executeDelete(DeleteReq req) throws TException {
    TagFilter tagFilter = resolveRawTagFilter(req.getTagFilter());

    // null timeRanges means delete columns
    List<KeyRange> keyRanges = null;
    if (req.isSetKeyRanges()) {
      keyRanges = new ArrayList<>();
      for (ParquetKeyRange range : req.getKeyRanges()) {
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

  @Override
  public GetColumnsOfStorageUnitResp getColumnsOfStorageUnit(String storageUnit) throws TException {
    List<TS> ret = new ArrayList<>();
    try {
      List<Column> tsList = executor.getColumnsOfStorageUnit(storageUnit);
      tsList.forEach(
          timeseries -> {
            TS ts = new TS(timeseries.getPath(), timeseries.getDataType().toString());
            if (timeseries.getTags() != null) {
              ts.setTags(timeseries.getTags());
            }
            ret.add(ts);
          });
      GetColumnsOfStorageUnitResp resp = new GetColumnsOfStorageUnitResp(SUCCESS);
      resp.setTsList(ret);
      return resp;
    } catch (PhysicalException e) {
      LOGGER.error("encounter error when getColumnsOfStorageUnit ", e);
      return new GetColumnsOfStorageUnitResp(GET_TS_FAIL);
    }
  }

  @Override
  public GetStorageBoundaryResp getBoundaryOfStorage(String dataPrefix) throws TException {
    try {
      Pair<ColumnsInterval, KeyInterval> pair = executor.getBoundaryOfStorage(dataPrefix);
      GetStorageBoundaryResp resp = new GetStorageBoundaryResp(SUCCESS);
      resp.setStartKey(pair.getV().getStartKey());
      resp.setEndKey(pair.getV().getEndKey());
      resp.setStartColumn(pair.getK().getStartColumn());
      resp.setEndColumn(pair.getK().getEndColumn());
      return resp;
    } catch (PhysicalException e) {
      LOGGER.error("encounter error when getBoundaryOfStorage ", e);
      return new GetStorageBoundaryResp(GET_BOUNDARY_FAIL);
    }
  }
}
