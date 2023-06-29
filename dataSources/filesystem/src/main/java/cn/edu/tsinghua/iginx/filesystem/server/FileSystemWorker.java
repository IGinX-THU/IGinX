package cn.edu.tsinghua.iginx.filesystem.server;

import cn.edu.tsinghua.iginx.common.thrift.GetStorageBoundaryResp;
import cn.edu.tsinghua.iginx.common.thrift.ProjectReq;
import cn.edu.tsinghua.iginx.common.thrift.RawTagFilter;
import cn.edu.tsinghua.iginx.common.thrift.Status;
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
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsRange;
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

  private static final Logger logger = LoggerFactory.getLogger(FileSystemWorker.class);

  private static final Status SUCCESS = new Status(200, "success");

  private static final Status EXEC_PROJECT_FAIL = new Status(401, "execute project fail");

  private static final Status EXEC_INSERT_FAIL = new Status(402, "execute insert fail");

  private static final Status EXEC_DELETE_FAIL = new Status(403, "execute delete fail");

  private static final Status GET_TS_FAIL = new Status(404, "get time series fail");

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
            req.getFilter(),
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
      logger.error("encounter error when get header from RowStream ", e);
      return new ProjectResp(EXEC_PROJECT_FAIL);
    }
    FileDataHeader fileDataHeader = new FileDataHeader(names, types, tagsList, hasTime);

    List<FileDataRow> fileDataRows = new ArrayList<>();
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
        FileDataRow fileDataRow =
            new FileDataRow(
                ByteUtils.getRowByteBuffer(rowValues, dataTypes),
                ByteBuffer.wrap(bitmap.getBytes()));
        if (hasTime) {
          fileDataRow.setTimestamp(row.getKey());
        }
        fileDataRows.add(fileDataRow);
      }
    } catch (PhysicalException e) {
      logger.error("encounter error when get result from RowStream ", e);
      return new ProjectResp(EXEC_PROJECT_FAIL);
    }

    ProjectResp resp = new ProjectResp(SUCCESS);
    resp.setHeader(fileDataHeader);
    resp.setRows(fileDataRows);
    return resp;
  }

  @Override
  public Status executeInsert(InsertReq req) throws TException {
    FileDataRawData fileDataRawData = req.getRawData();
    RawDataType rawDataType = strToRawDataType(fileDataRawData.getRawDataType());
    if (rawDataType == null) {
      return EXEC_INSERT_FAIL;
    }

    List<String> paths = fileDataRawData.getPaths();
    long[] timeArray = ByteUtils.getLongArrayFromByteArray(fileDataRawData.getTimestamps());
    List<Long> times = new ArrayList<>();
    Arrays.stream(timeArray).forEach(times::add);
    List<ByteBuffer> valueList = fileDataRawData.getValuesList();
    List<ByteBuffer> bitmapList = fileDataRawData.getBitmapList();
    List<DataType> types = new ArrayList<>();
    for (String dataType : fileDataRawData.getDataTypeList()) {
      types.add(DataTypeUtils.strToDataType(dataType));
    }

    List<Bitmap> bitmaps;
    Object[] values;
    if (rawDataType == RawDataType.Row || rawDataType == RawDataType.NonAlignedRow) {
      bitmaps =
          fileDataRawData.getBitmapList().stream()
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
            paths, fileDataRawData.getTagsList(), times, values, types, bitmaps, rawDataType);

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

    // null timeRanges means delete time series
    List<KeyRange> keyRanges = null;
    if (req.isSetTimeRanges()) {
      keyRanges = new ArrayList<>();
      for (FileSystemTimeRange range : req.getTimeRanges()) {
        keyRanges.add(new KeyRange(range.getBeginTime(), range.getEndTime()));
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
  public GetTimeSeriesOfStorageUnitResp getTimeSeriesOfStorageUnit(String storageUnit)
      throws TException {
    List<PathSet> ret = new ArrayList<>();
    try {
      List<Column> tsList = executor.getColumnOfStorageUnit(storageUnit);
      tsList.forEach(
          timeseries -> {
            PathSet pathSet =
                new PathSet(timeseries.getPath(), timeseries.getDataType().toString());
            if (timeseries.getTags() != null) {
              pathSet.setTags(timeseries.getTags());
            }
            ret.add(pathSet);
          });
      GetTimeSeriesOfStorageUnitResp resp = new GetTimeSeriesOfStorageUnitResp(SUCCESS);
      resp.setPathList(ret);
      return resp;
    } catch (PhysicalException e) {
      logger.error("encounter error when getTimeSeriesOfStorageUnit ", e);
      return new GetTimeSeriesOfStorageUnitResp(GET_TS_FAIL);
    }
  }

  @Override
  public GetStorageBoundaryResp getBoundaryOfStorage(String prefix) throws TException {
    try {
      Pair<ColumnsRange, KeyInterval> pair = executor.getBoundaryOfStorage(prefix);
      GetStorageBoundaryResp resp = new GetStorageBoundaryResp(SUCCESS);
      resp.setStartKey(pair.getV().getStartKey());
      resp.setEndKey(pair.getV().getEndKey());
      resp.setStartColumn(pair.getK().getStartColumn());
      resp.setEndColumn(pair.getK().getEndColumn());
      return resp;
    } catch (PhysicalException e) {
      logger.error("encounter error when getBoundaryOfStorage ", e);
      return new GetStorageBoundaryResp(GET_BOUNDARY_FAIL);
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
          logger.error("unknown tag filter type: {}", rawTagFilter.getType());
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
