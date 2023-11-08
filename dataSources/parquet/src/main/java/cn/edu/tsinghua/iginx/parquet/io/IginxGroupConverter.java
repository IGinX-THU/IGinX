package cn.edu.tsinghua.iginx.parquet.io;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

public class IginxGroupConverter extends GroupConverter {

  private final Converter[] converters;
  private IginxRecord currentRecord = null;

  public IginxGroupConverter(MessageType schema) {
    this.converters = new Converter[schema.getFieldCount()];
    for (int i = 0; i < schema.getFieldCount(); i++) {
      if (!schema.getType(i).isPrimitive()) {
        throw new IllegalArgumentException("Unsupported type: " + schema.getType(i));
      }
      PrimitiveType fieldType = schema.getType(i).asPrimitiveType();
      converters[i] = createConverter(fieldType.getPrimitiveTypeName(), i);
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
    currentRecord = new IginxRecord();
  }

  @Override
  public void end() {}

  public IginxRecord getCurrentRecord() {
    return currentRecord;
  }

  private Converter createConverter(PrimitiveType.PrimitiveTypeName primitiveTypeName, int index) {
    switch (primitiveTypeName) {
      case INT32:
        return new PrimitiveConverter() {
          @Override
          public void addInt(int value) {
            currentRecord.add(index, value);
          }
        };
      case INT64:
        return new PrimitiveConverter() {
          @Override
          public void addLong(long value) {
            currentRecord.add(index, value);
          }
        };
      case FLOAT:
        return new PrimitiveConverter() {
          @Override
          public void addFloat(float value) {
            currentRecord.add(index, value);
          }
        };
      case DOUBLE:
        return new PrimitiveConverter() {
          @Override
          public void addDouble(double value) {
            currentRecord.add(index, value);
          }
        };
      case BOOLEAN:
        return new PrimitiveConverter() {
          @Override
          public void addBoolean(boolean value) {
            currentRecord.add(index, value);
          }
        };
      case BINARY:
        return new PrimitiveConverter() {
          @Override
          public void addBinary(Binary value) {
            currentRecord.add(index, value.getBytes());
          }
        };
      default:
        throw new IllegalArgumentException("Unsupported type: " + primitiveTypeName);
    }
  }
}
