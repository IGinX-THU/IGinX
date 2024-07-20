package cn.edu.tsinghua.iginx.filestore.service.rpc.client;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.*;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.thrift.*;
import cn.edu.tsinghua.iginx.thrift.TagFilterType;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClientObjectMappingUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClientObjectMappingUtils.class);

  private ClientObjectMappingUtils() {
  }

  public static RawPrefix constructRawPrefix(@Nullable String prefix) {
    RawPrefix rawPrefix = new RawPrefix();
    if (prefix != null) {
      rawPrefix.setPrefix(prefix);
    }
    return rawPrefix;
  }

  public static RawDataTarget constructRawDataTarget(DataTarget dataTarget) {
    RawDataTarget rawDataTarget = new RawDataTarget(dataTarget.getPatterns());
    if (dataTarget.getFilter() != null) {
      rawDataTarget.setFilter(toRawFilter(dataTarget.getFilter()));
    }
    if (dataTarget.getTagFilter() != null) {
      rawDataTarget.setTagFilter(constructRawTagFilter(dataTarget.getTagFilter()));
    }
    return rawDataTarget;
  }

  public static RawFilter toRawFilter(Filter filter) {
    switch (filter.getType()) {
      case And:
        return toRawFilter((AndFilter) filter);
      case Or:
        return toRawFilter((OrFilter) filter);
      case Not:
        return toRawFilter((NotFilter) filter);
      case Value:
        return toRawFilter((ValueFilter) filter);
      case Key:
        return toRawFilter((KeyFilter) filter);
      case Bool:
        return toRawFilter((BoolFilter) filter);
      case Path:
        return toRawFilter((PathFilter) filter);
      default:
        return null;
    }
  }

  private static RawFilter toRawFilter(AndFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.And);
    for (Filter f : filter.getChildren()) {
      RawFilter.addToChildren(toRawFilter(f));
    }
    return RawFilter;
  }

  private static RawFilter toRawFilter(PathFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.Path);
    RawFilter.setPathA(filter.getPathA());
    RawFilter.setPathB(filter.getPathB());
    RawFilter.setOp(toRawFilterOp(filter.getOp()));
    return RawFilter;
  }

  private static RawFilter toRawFilter(OrFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.Or);
    for (Filter f : filter.getChildren()) {
      RawFilter.addToChildren(toRawFilter(f));
    }
    return RawFilter;
  }

  private static RawFilter toRawFilter(NotFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.Not);
    RawFilter.addToChildren(toRawFilter(filter.getChild()));
    return RawFilter;
  }

  private static RawFilter toRawFilter(KeyFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.Key);
    RawFilter.setOp(toRawFilterOp(filter.getOp()));
    RawFilter.setKeyValue(filter.getValue());
    return RawFilter;
  }

  private static RawFilter toRawFilter(ValueFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.Value);
    RawFilter.setValue(toRawValue(filter.getValue()));
    RawFilter.setPath(filter.getPath());
    RawFilter.setOp(toRawFilterOp(filter.getOp()));
    return RawFilter;
  }

  private static RawFilter toRawFilter(BoolFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.Bool);
    RawFilter.setIsTrue(filter.isTrue());
    return RawFilter;
  }

  private static RawFilterOp toRawFilterOp(Op op) {
    switch (op) {
      case L:
        return RawFilterOp.L;
      case LE:
        return RawFilterOp.LE;
      case LIKE:
        return RawFilterOp.LIKE;
      case NE:
        return RawFilterOp.NE;
      case E:
        return RawFilterOp.E;
      case GE:
        return RawFilterOp.GE;
      case G:
        return RawFilterOp.G;
      case L_AND:
        return RawFilterOp.L_AND;
      case LE_AND:
        return RawFilterOp.LE_AND;
      case LIKE_AND:
        return RawFilterOp.LIKE_AND;
      case NE_AND:
        return RawFilterOp.NE_AND;
      case E_AND:
        return RawFilterOp.E_AND;
      case GE_AND:
        return RawFilterOp.GE_AND;
      case G_AND:
        return RawFilterOp.G_AND;
      default:
        return RawFilterOp.UNKNOWN;
    }
  }

  private static RawValue toRawValue(cn.edu.tsinghua.iginx.engine.shared.data.Value value) {
    RawValue RawValue = new RawValue();
    RawValue.setDataType(value.getDataType());
    switch (value.getDataType()) {
      case FLOAT:
        RawValue.setFloatV(value.getFloatV());
        break;
      case INTEGER:
        RawValue.setIntV(value.getIntV());
        break;
      case BINARY:
        RawValue.setBinaryV(value.getBinaryV());
        break;
      case BOOLEAN:
        RawValue.setBoolV(value.getBoolV());
        break;
      case DOUBLE:
        RawValue.setDoubleV(value.getDoubleV());
        break;
      case LONG:
        RawValue.setLongV(value.getLongV());
        break;
    }
    return RawValue;
  }

  public static RawTagFilter constructRawTagFilter(TagFilter tagFilter) {
    switch (tagFilter.getType()) {
      case Base: {
        BaseTagFilter baseTagFilter = (BaseTagFilter) tagFilter;
        RawTagFilter filter = new RawTagFilter(TagFilterType.Base);
        filter.setKey(baseTagFilter.getTagKey());
        filter.setValue(baseTagFilter.getTagValue());
        return filter;
      }
      case WithoutTag: {
        return new RawTagFilter(TagFilterType.WithoutTag);
      }
      case BasePrecise: {
        BasePreciseTagFilter basePreciseTagFilter = (BasePreciseTagFilter) tagFilter;
        RawTagFilter filter = new RawTagFilter(TagFilterType.BasePrecise);
        filter.setTags(basePreciseTagFilter.getTags());
        return filter;
      }
      case Precise: {
        PreciseTagFilter preciseTagFilter = (PreciseTagFilter) tagFilter;
        RawTagFilter filter = new RawTagFilter(TagFilterType.Precise);
        List<RawTagFilter> children = new ArrayList<>();
        preciseTagFilter
            .getChildren()
            .forEach(child -> children.add(constructRawTagFilter(child)));
        filter.setChildren(children);
        return filter;
      }
      case And: {
        AndTagFilter andTagFilter = (AndTagFilter) tagFilter;
        RawTagFilter filter = new RawTagFilter(TagFilterType.And);
        List<RawTagFilter> children = new ArrayList<>();
        andTagFilter.getChildren().forEach(child -> children.add(constructRawTagFilter(child)));
        filter.setChildren(children);
        return filter;
      }
      case Or: {
        OrTagFilter orTagFilter = (OrTagFilter) tagFilter;
        RawTagFilter filter = new RawTagFilter(TagFilterType.Or);
        List<RawTagFilter> children = new ArrayList<>();
        orTagFilter.getChildren().forEach(child -> children.add(constructRawTagFilter(child)));
        filter.setChildren(children);
        return filter;
      }
      default: {
        LOGGER.error("unknown tag filter type: {}", tagFilter.getType());
        return null;
      }
    }
  }

  public static RawAggregate constructRawAggregate(@Nullable AggregateType aggregate) {
    RawAggregate rawAggregate = new RawAggregate();
    if (aggregate != null) {
      rawAggregate.setType(aggregate);
    }
    return rawAggregate;
  }

  public static RowStream constructRowStream(RawDataSet dataSet) {
    Header header = constructHeader(dataSet.getHeader());
    List<Row> rowList = constructRows(dataSet.getRows(), header);
    return new Table(header, rowList);
  }

  private static List<Row> constructRows(List<RawRow> rows, Header header) {
    List<Row> rowList = new ArrayList<>();
    for (RawRow rawRow : rows) {
      Object[] values = new Object[header.getFields().size()];
      Bitmap bitmap = new Bitmap(header.getFields().size(), rawRow.getBitmap());
      ByteBuffer valuesBuffer = ByteBuffer.wrap(rawRow.getRowValues());
      for (int i = 0; i < header.getFields().size(); i++) {
        if (bitmap.get(i)) {
          values[i] =
              ByteUtils.getValueFromByteBufferByDataType(
                  valuesBuffer, header.getFields().get(i).getType());
        } else {
          values[i] = null;
        }
      }

      if (rawRow.isSetKey()) {
        rowList.add(new Row(header, rawRow.getKey(), values));
      } else {
        rowList.add(new Row(header, values));
      }
    }
    return rowList;
  }

  public static Header constructHeader(RawHeader rawHeader) {
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < rawHeader.getNamesSize(); i++) {
      String path = rawHeader.getNames().get(i);
      DataType dataType = rawHeader.getTypes().get(i);
      Map<String, String> tags = rawHeader.getTagsList().get(i);
      fields.add(new Field(path, dataType, tags));
    }
    return rawHeader.isHasKey() ? new Header(Field.KEY, fields) : new Header(fields);
  }

  public static RawInserted constructRawInserted(DataView dataView) {

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

    return new RawInserted(
            paths,
            tagsList,
            ByteBuffer.wrap(ByteUtils.getByteArrayFromLongArray(times)),
            pair.getK(),
            pair.getV(),
            types,
            dataView.getRawDataType().toString());
  }

  private static Pair<List<ByteBuffer>, List<ByteBuffer>> compressRowData(DataView dataView) {
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

  private static Pair<List<ByteBuffer>, List<ByteBuffer>> compressColData(DataView dataView) {
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


}
