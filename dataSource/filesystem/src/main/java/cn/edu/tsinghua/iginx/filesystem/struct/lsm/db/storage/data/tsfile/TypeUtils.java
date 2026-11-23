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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.tsfile;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.TagKVUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.List;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.*;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

public class TypeUtils {

  private TypeUtils() {}

  public static IMeasurementSchema toTsfileField(Field field, TSFileConfig config) {
    TSDataType dataType = toTsfileType(field.getType());
    return new MeasurementSchema(
        TagKVUtils.toFullName(field.getName(), field.getTags()),
        dataType,
        TSEncoding.valueOf(config.getValueEncoder(dataType)),
        config.getCompressor(dataType));
  }

  public static TSDataType toTsfileType(DataType type) {
    switch (type) {
      case BOOLEAN:
        return TSDataType.BOOLEAN;
      case INTEGER:
        return TSDataType.INT32;
      case LONG:
        return TSDataType.INT64;
      case FLOAT:
        return TSDataType.FLOAT;
      case DOUBLE:
        return TSDataType.DOUBLE;
      case BINARY:
        return TSDataType.BLOB;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + type);
    }
  }

  public static Path toTsfilePath(String subTable, Field field) {
    return new Path(subTable, TagKVUtils.toFullName(field.getName(), field.getTags()), false);
  }

  public static Field toIginxField(String measurementId, TSDataType type) {
    ColumnKey columnKey = TagKVUtils.splitFullName(measurementId);
    return new Field(columnKey.getPath(), toIginxType(type), columnKey.getTags());
  }

  private static DataType toIginxType(TSDataType type) {
    switch (type) {
      case BOOLEAN:
        return DataType.BOOLEAN;
      case INT32:
        return DataType.INTEGER;
      case INT64:
        return DataType.LONG;
      case FLOAT:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case BLOB:
        return DataType.BINARY;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + type);
    }
  }

  public static TSRecord toTsRecord(
      String deviceId, List<IMeasurementSchema> schema, long key, Object[] values) {
    TSRecord record = new TSRecord(deviceId, key);
    for (int i = 0; i < schema.size(); i++) {
      Object value = values[i];
      if (value != null) {
        record.addTuple(toDataPoint(schema.get(i), value));
      }
    }
    return record;
  }

  private static DataPoint toDataPoint(IMeasurementSchema measurementSchema, Object value) {
    String name = measurementSchema.getMeasurementName();
    switch (measurementSchema.getType()) {
      case BOOLEAN:
        return new BooleanDataPoint(name, (Boolean) value);
      case INT32:
        return new IntDataPoint(name, (Integer) value);
      case INT64:
        return new LongDataPoint(name, (Long) value);
      case FLOAT:
        return new FloatDataPoint(name, (Float) value);
      case DOUBLE:
        return new DoubleDataPoint(name, (Double) value);
      case BLOB:
        return new StringDataPoint(name, new Binary((byte[]) value));
      default:
        throw new IllegalArgumentException("Unsupported data type: " + measurementSchema.getType());
    }
  }

  public static Object toIginxValue(org.apache.tsfile.read.common.Field field) {
    TSDataType type = field.getDataType();
    if (type == null) {
      return null;
    }
    switch (field.getDataType()) {
      case BOOLEAN:
        return field.getBoolV();
      case INT32:
        return field.getIntV();
      case INT64:
        return field.getLongV();
      case FLOAT:
        return field.getFloatV();
      case DOUBLE:
        return field.getDoubleV();
      case BLOB:
      case TEXT:
        return field.getBinaryV().getValues();
      default:
        throw new IllegalArgumentException("Unsupported data type: " + type);
    }
  }
}
