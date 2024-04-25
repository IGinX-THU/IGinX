package cn.edu.tsinghua.iginx.engine;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.*;
import cn.edu.tsinghua.iginx.sql.statement.*;
import cn.edu.tsinghua.iginx.sql.statement.selectstatement.UnarySelectStatement;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.thrift.TagFilterType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class ContextBuilder {

  private static ContextBuilder instance;

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private ContextBuilder() {}

  public static ContextBuilder getInstance() {
    if (instance == null) {
      synchronized (ContextBuilder.class) {
        if (instance == null) {
          instance = new ContextBuilder();
        }
      }
    }
    return instance;
  }

  public long getTimeWithPrecision(long time, TimePrecision timePrecision) {
    if (timePrecision == null) timePrecision = config.getTimePrecision();
    return TimeUtils.getTimeInNs(time, timePrecision);
  }

  public RequestContext build(DeleteColumnsReq req) {
    DeleteColumnsStatement statement = new DeleteColumnsStatement(req.getPaths());
    if (req.isSetTagsList()) {
      statement.setTagFilter(constructTagFilterFromTagList(req.getTagsList(), req.getFilterType()));
    }
    return new RequestContext(req.getSessionId(), statement);
  }

  public RequestContext build(InsertColumnRecordsReq req) {
    return buildFromInsertReq(
        req.getSessionId(),
        RawDataType.Column,
        req.getPaths(),
        req.getDataTypeList(),
        req.getKeys(),
        req.getValuesList(),
        req.getBitmapList(),
        req.getTagsList(),
        req.getTimePrecision());
  }

  public RequestContext build(InsertNonAlignedColumnRecordsReq req) {
    return buildFromInsertReq(
        req.getSessionId(),
        RawDataType.NonAlignedColumn,
        req.getPaths(),
        req.getDataTypeList(),
        req.getKeys(),
        req.getValuesList(),
        req.getBitmapList(),
        req.getTagsList(),
        req.getTimePrecision());
  }

  public RequestContext build(InsertRowRecordsReq req) {
    return buildFromInsertReq(
        req.getSessionId(),
        RawDataType.Row,
        req.getPaths(),
        req.getDataTypeList(),
        req.getKeys(),
        req.getValuesList(),
        req.getBitmapList(),
        req.getTagsList(),
        req.getTimePrecision());
  }

  public RequestContext build(InsertNonAlignedRowRecordsReq req) {
    return buildFromInsertReq(
        req.getSessionId(),
        RawDataType.NonAlignedRow,
        req.getPaths(),
        req.getDataTypeList(),
        req.getKeys(),
        req.getValuesList(),
        req.getBitmapList(),
        req.getTagsList(),
        req.getTimePrecision());
  }

  private RequestContext buildFromInsertReq(
      long sessionId,
      RawDataType rawDataType,
      List<String> paths,
      List<DataType> types,
      byte[] timestamps,
      List<ByteBuffer> valueList,
      List<ByteBuffer> bitmapList,
      List<Map<String, String>> tagsList,
      TimePrecision timePrecision) {
    long[] timeArray = ByteUtils.getLongArrayFromByteArray(timestamps);
    List<Long> times = new ArrayList<>();
    if (timePrecision == null) timePrecision = config.getTimePrecision();
    for (long time : timeArray) {
      times.add(TimeUtils.getTimeInNs(time, timePrecision));
    }

    List<Bitmap> bitmaps;
    Object[] values;
    if (rawDataType == RawDataType.Row || rawDataType == RawDataType.NonAlignedRow) {
      bitmaps =
          bitmapList.stream()
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

    InsertStatement statement =
        new InsertStatement(rawDataType, paths, times, values, types, bitmaps, tagsList);
    return new RequestContext(sessionId, statement);
  }

  public RequestContext build(DeleteDataInColumnsReq req) {
    DeleteStatement statement =
        new DeleteStatement(
            req.getPaths(),
            getTimeWithPrecision(req.getStartKey(), req.getTimePrecision()),
            getTimeWithPrecision(req.getEndKey(), req.getTimePrecision()));

    if (req.isSetTagsList()) {
      statement.setTagFilter(constructTagFilterFromTagList(req.getTagsList(), req.getFilterType()));
    }
    return new RequestContext(req.getSessionId(), statement);
  }

  public RequestContext build(QueryDataReq req) {
    UnarySelectStatement statement =
        new UnarySelectStatement(
            req.getPaths(),
            getTimeWithPrecision(req.getStartKey(), req.getTimePrecision()),
            getTimeWithPrecision(req.getEndKey(), req.getTimePrecision()));

    if (req.isSetTagsList()) {
      statement.setTagFilter(constructTagFilterFromTagList(req.getTagsList(), req.getFilterType()));
    }
    return new RequestContext(req.getSessionId(), statement);
  }

  public RequestContext build(AggregateQueryReq req) {
    UnarySelectStatement statement =
        new UnarySelectStatement(
            req.getPaths(),
            getTimeWithPrecision(req.getStartKey(), req.getTimePrecision()),
            getTimeWithPrecision(req.getEndKey(), req.getTimePrecision()),
            req.getAggregateType());

    if (req.isSetTagsList()) {
      statement.setTagFilter(constructTagFilterFromTagList(req.getTagsList(), req.getFilterType()));
    }
    return new RequestContext(req.getSessionId(), statement);
  }

  public RequestContext build(DownsampleQueryReq req) {
    UnarySelectStatement statement =
        new UnarySelectStatement(
            req.getPaths(),
            getTimeWithPrecision(req.getStartKey(), req.getTimePrecision()),
            getTimeWithPrecision(req.getEndKey(), req.getTimePrecision()),
            req.getAggregateType(),
            getTimeWithPrecision(req.getPrecision(), req.getTimePrecision()));

    if (req.isSetTagsList()) {
      statement.setTagFilter(constructTagFilterFromTagList(req.getTagsList(), req.getFilterType()));
    }
    return new RequestContext(req.getSessionId(), statement);
  }

  public RequestContext build(ShowColumnsReq req) {
    ShowColumnsStatement statement = new ShowColumnsStatement();
    return new RequestContext(req.getSessionId(), statement);
  }

  public RequestContext build(ExecuteSqlReq req) {
    return new RequestContext(req.getSessionId(), req.getStatement());
  }

  public RequestContext build(ExecuteStatementReq req) {
    return new RequestContext(req.getSessionId(), req.getStatement(), true);
  }

  public RequestContext build(ExecuteSubPlanReq req) {
    RequestContext context = new RequestContext(req.getSessionId());
    context.setSubPlanMsg(req.getSubPlan());
    return context;
  }

  public RequestContext build(LoadCSVReq req) {
    return new RequestContext(req.getSessionId(), req.getStatement());
  }

  public RequestContext build(LastQueryReq req) {
    UnarySelectStatement statement =
        new UnarySelectStatement(
            req.getPaths(),
            getTimeWithPrecision(req.getStartKey(), req.getTimePrecision()),
            Long.MAX_VALUE,
            AggregateType.LAST);

    if (req.isSetTagsList()) {
      statement.setTagFilter(constructTagFilterFromTagList(req.getTagsList(), req.getFilterType()));
    }
    return new RequestContext(req.getSessionId(), statement);
  }

  private TagFilter constructTagFilterFromTagList(
      List<Map<String, List<String>>> tagList, TagFilterType type) {
    if (type == null) {
      type = TagFilterType.Or;
    }
    switch (type) {
      case Base:
      case And: // 合取范式
        List<TagFilter> andTagFilterList = new ArrayList<>();
        tagList.forEach(
            map -> {
              List<TagFilter> orTagFilterList = new ArrayList<>();
              map.forEach(
                  (key, value) -> {
                    for (String val : value) {
                      orTagFilterList.add(new BaseTagFilter(key, val));
                    }
                  });
              andTagFilterList.add(new AndTagFilter(orTagFilterList));
            });
        return andTagFilterList.isEmpty() ? null : new AndTagFilter(andTagFilterList);
      case Or: // 析取范式
        List<TagFilter> orTagFilterList = new ArrayList<>();
        tagList.forEach(
            map -> {
              List<TagFilter> andTagList = new ArrayList<>();
              map.forEach((key, value) -> andTagList.add(new BaseTagFilter(key, value.get(0))));
              orTagFilterList.add(new OrTagFilter(andTagList));
            });
        return orTagFilterList.isEmpty() ? null : new OrTagFilter(orTagFilterList);
      case BasePrecise:
      case Precise: // 转换为析取范式
        List<BasePreciseTagFilter> baseTagFilterList = new ArrayList<>();
        List<Map<String, String>> rawTags = convertToDNF(tagList);
        rawTags.forEach(map -> baseTagFilterList.add(new BasePreciseTagFilter(map)));
        return baseTagFilterList.isEmpty() ? null : new PreciseTagFilter(baseTagFilterList);
      case WithoutTag:
        return new WithoutTagFilter();
      default:
        throw new IllegalArgumentException("tagkv type not right!");
    }
  }

  private List<Map<String, String>> convertToDNF(List<Map<String, List<String>>> tagList) {
    List<Map<String, String>> dnfList = new ArrayList<>();
    List<Map.Entry<String, List<String>>> valList = new ArrayList<>();
    tagList.forEach(
        map -> {
          valList.addAll(map.entrySet());
        });
    tagList.forEach(map -> generateDNF(valList, 0, new HashMap<>(), dnfList));
    return dnfList;
  }

  private void generateDNF(
      List<Map.Entry<String, List<String>>> entry,
      int index,
      Map<String, String> currentMap,
      List<Map<String, String>> dnfList) {
    if (index == entry.size()) {
      dnfList.add(currentMap);
      return;
    }

    String key = entry.get(index).getKey();
    List<String> valList = entry.get(index).getValue();
    for (String val : valList) {
      currentMap.put(key, val);
      generateDNF(entry, index + 1, new HashMap<>(currentMap), dnfList);
    }
  }
}
