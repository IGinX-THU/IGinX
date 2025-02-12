/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.filesystem.service.rpc.server;

import static cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType.In;
import static cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op.*;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.*;
import cn.edu.tsinghua.iginx.filesystem.common.FileSystemException;
import cn.edu.tsinghua.iginx.filesystem.struct.DataTarget;
import cn.edu.tsinghua.iginx.filesystem.thrift.*;
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
      case In:
        return resolveRawInFilter(filter);
      default:
        throw new UnsupportedOperationException("unsupported filter type: " + filter.getType());
    }
  }

  private static Filter resolveRawInFilter(RawFilter filter) {
    return new InFilter(
        filter.getPath(),
        resolveRawFilterInOp(filter.inOp),
        filter.getArray().stream()
            .map(ServerObjectMappingUtils::resolveRawValue)
            .collect(Collectors.toList()));
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
      case NOT_LIKE:
        return NOT_LIKE;
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
      case NOT_LIKE_AND:
        return NOT_LIKE_AND;
      case NE_AND:
        return NE_AND;
      case E_AND:
        return E_AND;
      case GE_AND:
        return GE_AND;
      case G_AND:
        return G_AND;
      default:
        throw new UnsupportedOperationException("unsupported filter op: " + op);
    }
  }

  private static InFilter.InOp resolveRawFilterInOp(RawFilterInOp rawFilterInOp) {
    switch (rawFilterInOp) {
      case IN_AND:
        return InFilter.InOp.IN_AND;
      case IN:
        return InFilter.InOp.IN_OR;
      case NOT_IN_AND:
        return InFilter.InOp.NOT_IN_AND;
      case NOT_IN:
        return InFilter.InOp.NOT_IN_OR;
      default:
        throw new UnsupportedOperationException("unsupported filter in op: " + rawFilterInOp);
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
        throw new UnsupportedOperationException(
            "unsupported tag filter type: " + rawTagFilter.getType());
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
        throw new UnsupportedOperationException("unsupported data type: " + type);
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

  public static RawDataSet constructRawDataSet(RowStream rowStream) throws FileSystemException {

    try {
      RawHeader rawHeader = constructRawHeader(rowStream.getHeader());
      List<RawRow> rawRows = constructRawRows(rowStream, rawHeader);
      return new RawDataSet(rawHeader, rawRows);
    } catch (PhysicalException e) {
      throw new FileSystemException(e);
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
