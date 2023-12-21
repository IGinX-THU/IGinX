package cn.edu.tsinghua.iginx.parquet.io.parquet;

import cn.edu.tsinghua.iginx.parquet.entity.Field;
import cn.edu.tsinghua.iginx.parquet.entity.Table;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

@Deprecated
public class Storer {

  protected final Path path;

  public Storer(Path path) {
    this.path = path;
  }

  public static final String KEY_FIELD_NAME = "key";

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
