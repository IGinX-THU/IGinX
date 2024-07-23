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

package cn.edu.tsinghua.iginx.filestore.service.rpc.server;

import static cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op.*;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.*;
import cn.edu.tsinghua.iginx.filestore.common.FileStoreException;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.thrift.*;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerObjectMappingUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerObjectMappingUtils.class);

  private ServerObjectMappingUtils() {}

  public static RawFilter constructRawFilter(Filter filter) {
    if (filter == null) {
      return null;
    }
    switch (filter.getType()) {
      case And:
        return constructRawFilter((AndFilter) filter);
      case Or:
        return constructRawFilter((OrFilter) filter);
      case Not:
        return constructRawFilter((NotFilter) filter);
      case Value:
        return constructRawFilter((ValueFilter) filter);
      case Key:
        return constructRawFilter((KeyFilter) filter);
      case Bool:
        return constructRawFilter((BoolFilter) filter);
      case Path:
        return constructRawFilter((PathFilter) filter);
      default:
        return null;
    }
  }

  private static RawFilter constructRawFilter(AndFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.And);
    for (Filter f : filter.getChildren()) {
      RawFilter.addToChildren(constructRawFilter(f));
    }
    return RawFilter;
  }

  private static RawFilter constructRawFilter(PathFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.Path);
    RawFilter.setPathA(filter.getPathA());
    RawFilter.setPathB(filter.getPathB());
    RawFilter.setOp(constructRawFilterOp(filter.getOp()));
    return RawFilter;
  }

  private static RawFilter constructRawFilter(OrFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.Or);
    for (Filter f : filter.getChildren()) {
      RawFilter.addToChildren(constructRawFilter(f));
    }
    return RawFilter;
  }

  private static RawFilter constructRawFilter(NotFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.Not);
    RawFilter.addToChildren(constructRawFilter(filter.getChild()));
    return RawFilter;
  }

  private static RawFilter constructRawFilter(KeyFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.Key);
    RawFilter.setOp(constructRawFilterOp(filter.getOp()));
    RawFilter.setKeyValue(filter.getValue());
    return RawFilter;
  }

  private static RawFilter constructRawFilter(ValueFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.Value);
    RawFilter.setValue(constructRawValue(filter.getValue()));
    RawFilter.setPath(filter.getPath());
    RawFilter.setOp(constructRawFilterOp(filter.getOp()));
    return RawFilter;
  }

  private static RawFilter constructRawFilter(BoolFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.Bool);
    RawFilter.setIsTrue(filter.isTrue());
    return RawFilter;
  }

  private static RawFilterOp constructRawFilterOp(Op op) {
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

  private static RawValue constructRawValue(Value value) {
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

  public static Filter resolveRawFilter(RawFilter filter) {
    if (filter == null) {
      return null;
    }
    switch (filter.getType()) {
      case And:
        return resolveRawAndFilter(filter);
      case Or:
        return resolveRawOrFilter(filter);
      case Not:
        return resolveRawNotFilter(filter);
      case Value:
        return resolveRawValueFilter(filter);
      case Key:
        return resolveRawKeyFilter(filter);
      case Bool:
        return resolveRawBoolFilter(filter);
      case Path:
        return resolveRawPathFilter(filter);
      default:
        return null;
    }
  }

  private static Filter resolveRawAndFilter(RawFilter andFilter) {
    List<Filter> filters = new ArrayList<>();
    for (RawFilter f : andFilter.getChildren()) {
      filters.add(resolveRawFilter(f));
    }
    return new AndFilter(filters);
  }

  private static Filter resolveRawPathFilter(RawFilter filter) {
    return new PathFilter(filter.getPathA(), resolveRawFilterOp(filter.getOp()), filter.getPathB());
  }

  private static Filter resolveRawOrFilter(RawFilter filter) {
    List<Filter> filters = new ArrayList<>();
    for (RawFilter f : filter.getChildren()) {
      filters.add(resolveRawFilter(f));
    }
    return new OrFilter(filters);
  }

  private static Filter resolveRawNotFilter(RawFilter filter) {
    return new NotFilter(resolveRawFilter(filter.getChildren().get(0)));
  }

  private static Filter resolveRawKeyFilter(RawFilter filter) {
    return new KeyFilter(resolveRawFilterOp(filter.getOp()), filter.getKeyValue());
  }

  private static Filter resolveRawValueFilter(RawFilter filter) {
    return new ValueFilter(
        filter.getPath(), resolveRawFilterOp(filter.getOp()), resolveRawValue(filter.getValue()));
  }

  private static Filter resolveRawBoolFilter(RawFilter filter) {
    return new BoolFilter(filter.isIsTrue());
  }

  private static Op resolveRawFilterOp(RawFilterOp op) {
    switch (op) {
      case L:
        return L;
      case LE:
        return LE;
      case LIKE:
        return LIKE;
      case NE:
        return NE;
      case E:
        return E;
      case GE:
        return GE;
      case G:
        return G;
      case L_AND:
        return L_AND;
      case LE_AND:
        return LE_AND;
      case LIKE_AND:
        return LIKE_AND;
      case NE_AND:
        return NE_AND;
      case E_AND:
        return E_AND;
      case GE_AND:
        return GE_AND;
      case G_AND:
        return G_AND;
      default:
        return null;
    }
  }

  private static Value resolveRawValue(RawValue RawValue) {
    Value value = null;
    switch (RawValue.getDataType()) {
      case FLOAT:
        value = new Value(RawValue.getFloatV());
        break;
      case INTEGER:
        value = new Value(RawValue.getIntV());
        break;
      case BINARY:
        value = new Value(RawValue.getBinaryV());
        break;
      case BOOLEAN:
        value = new Value(RawValue.isBoolV());
        break;
      case DOUBLE:
        value = new Value(RawValue.getDoubleV());
        break;
      case LONG:
        value = new Value(RawValue.getLongV());
        break;
    }
    return value;
  }

  public static TagFilter resolveRawTagFilter(RawTagFilter rawTagFilter) {
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

  public static RawDataType resolveRawDataType(String type) {
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

  public static String resolveRawPrefix(RawPrefix prefix) {
    return prefix.getPrefix();
  }

  public static DataTarget resolveRawDataTarget(RawDataTarget target) {
    Filter filter = resolveRawFilter(target.getFilter());
    TagFilter tagFilter = resolveRawTagFilter(target.getTagFilter());
    return new DataTarget(filter, target.getPatterns(), tagFilter);
  }

  public static AggregateType resolveRawAggregate(RawAggregate aggregate) {
    return aggregate.getType();
  }

  public static RawDataSet constructRawDataSet(RowStream rowStream) throws FileStoreException {

    try {
      RawHeader rawHeader = constructRawHeader(rowStream.getHeader());
      List<RawRow> rawRows = constructRawRows(rowStream, rawHeader);
      return new RawDataSet(rawHeader, rawRows);
    } catch (PhysicalException e) {
      throw new FileStoreException(e);
    }
  }

  public static RawHeader constructRawHeader(Header header) {
    List<String> names = new ArrayList<>();
    List<DataType> types = new ArrayList<>();
    List<DataType> dataTypes = new ArrayList<>();
    List<Map<String, String>> tagsList = new ArrayList<>();
    boolean hasKey = header.hasKey();
    header
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

    return new RawHeader(names, types, tagsList, hasKey);
  }

  public static List<RawRow> constructRawRows(RowStream rowStream, RawHeader rawHeader)
      throws PhysicalException {
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
              ByteUtils.getRowByteBuffer(rowValues, rawHeader.getTypes()),
              ByteBuffer.wrap(bitmap.getBytes()));
      if (rawHeader.isHasKey()) {
        rawRow.setKey(row.getKey());
      }
      rawRows.add(rawRow);
    }
    return rawRows;
  }

  public static DataView resolveRawInserted(RawInserted rawInserted) {
    RawDataType rawDataType =
        ServerObjectMappingUtils.resolveRawDataType(rawInserted.getRawDataType());

    List<String> paths = rawInserted.getPatterns();
    List<Map<String, String>> tags = rawInserted.getTagsList();
    long[] timeArray = ByteUtils.getLongArrayFromByteArray(rawInserted.getKeys());
    List<Long> times = new ArrayList<>();
    Arrays.stream(timeArray).forEach(times::add);

    List<ByteBuffer> valueList = rawInserted.getValuesList();
    List<ByteBuffer> bitmapList = rawInserted.getBitmapList();
    List<DataType> types = new ArrayList<>();
    for (String dataType : rawInserted.getDataTypeList()) {
      types.add(DataTypeUtils.getDataTypeFromString(dataType));
    }

    List<Bitmap> bitmaps;
    Object[] values;
    if (rawDataType == RawDataType.Row || rawDataType == RawDataType.NonAlignedRow) {
      bitmaps =
          rawInserted.getBitmapList().stream()
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

    return dataView;
  }
}
