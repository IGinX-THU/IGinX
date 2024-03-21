/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.manager.dummy;

import cn.edu.tsinghua.iginx.parquet.io.parquet.IParquetWriter;
import cn.edu.tsinghua.iginx.parquet.io.parquet.IRecord;
import cn.edu.tsinghua.iginx.parquet.util.Constants;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.PrimitiveType;
import shaded.iginx.org.apache.parquet.schema.Type;

@Deprecated
public class Storer {

  protected final Path path;

  public Storer(Path path) {
    this.path = path;
  }

  public static final String KEY_FIELD_NAME = Constants.KEY_FIELD_NAME;

  public static PrimitiveType getParquetType(
      String name, DataType type, Type.Repetition repetition) {
    switch (type) {
      case BOOLEAN:
        return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.BOOLEAN, name);
      case INTEGER:
        return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.INT32, name);
      case LONG:
        return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.INT64, name);
      case FLOAT:
        return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.FLOAT, name);
      case DOUBLE:
        return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.DOUBLE, name);
      case BINARY:
        return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.BINARY, name);
      default:
        throw new RuntimeException("Unsupported data type: " + type);
    }
  }

  public void flush(Table memTable) throws IOException {
    MessageType schema = getMessageTypeStartWithKey("test", memTable.getHeader());
    IParquetWriter.Builder writerBuilder = IParquetWriter.builder(path, schema);

    try (IParquetWriter writer = writerBuilder.build()) {
      long lastKey = Long.MIN_VALUE;
      IRecord record = null;
      for (Table.Point point : memTable.scanRows()) {
        if (record != null && point.key != lastKey) {
          writer.write(record);
          record = null;
        }
        if (record == null) {
          record = new IRecord();
          record.add(0, point.key);
          lastKey = point.key;
        }
        record.add(point.field + 1, point.value);
      }
      if (record != null) {
        writer.write(record);
      }
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("unexpected writer error!", e);
    }
  }

  public static MessageType getMessageTypeStartWithKey(String name, List<Field> header) {
    List<Type> fields = new ArrayList<>();
    fields.add(getParquetType(KEY_FIELD_NAME, DataType.LONG, Type.Repetition.REQUIRED));
    for (Field field : header) {
      fields.add(getParquetType(field.getName(), field.getType(), Type.Repetition.OPTIONAL));
    }
    return new MessageType(name, fields);
  }
}
