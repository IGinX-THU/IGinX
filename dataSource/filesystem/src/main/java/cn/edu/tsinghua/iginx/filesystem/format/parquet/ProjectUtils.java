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
package cn.edu.tsinghua.iginx.filesystem.format.parquet;

import static cn.edu.tsinghua.iginx.filesystem.format.parquet.IRecordDematerializer.OBJECT_MODEL_NAME_VALUE;
import static cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.manager.dummy.Storer.getParquetType;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.Constants;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.*;
import javax.annotation.Nullable;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.Type;
import shaded.iginx.org.apache.parquet.schema.Types;

public class ProjectUtils {
  private ProjectUtils() {}

  static MessageType projectMessageType(
      MessageType schema, @Nullable Set<String> fields, boolean hasKey) {
    Set<String> schemaFields = new HashSet<>(Objects.requireNonNull(fields));
    if (hasKey) {
      schemaFields.add(Constants.KEY_FIELD_NAME);
    }

    Types.MessageTypeBuilder builder = Types.buildMessage();
    for (String field : schemaFields) {
      if (schema.containsField(field)) {
        Type type = schema.getType(field);
        if (!type.isPrimitive()) {
          throw new IllegalArgumentException("not primitive type is not supported: " + field);
        }
        builder.addField(schema.getType(field));
      }
    }

    return builder.named(schema.getName());
  }

  public static MessageType toMessageType(Header header) {
    List<Type> parquetFields = new ArrayList<>();
    if (header.hasKey()) {
      parquetFields.add(
          getParquetType(Constants.KEY_FIELD_NAME, DataType.LONG, Type.Repetition.REQUIRED));
    }
    for (Field field : header.getFields()) {
      parquetFields.add(getParquetType(field.getName(), field.getType(), Type.Repetition.OPTIONAL));
    }
    return new MessageType(OBJECT_MODEL_NAME_VALUE, parquetFields);
  }

  public static IRecord toRecord(Row row) {
    IRecord record = new IRecord();
    int offset = 0;
    if (row.getHeader().hasKey()) {
      record.add(0, row.getKey());
      offset++;
    }
    for (int i = 0; i < row.getValues().length; i++) {
      if (row.getValues()[i] != null) {
        record.add(i + offset, row.getValues()[i]);
      }
    }
    return record;
  }

  public static Row toRow(Header header, long key, IRecord record) {
    if (!header.hasKey()) {
      throw new IllegalArgumentException("header does not have key field");
    }
    Object[] values = new Object[header.getFields().size()];
    for (Map.Entry<Integer, Object> entry : record) {
      values[entry.getKey()] = entry.getValue();
    }
    return new Row(header, key, values);
  }
}
