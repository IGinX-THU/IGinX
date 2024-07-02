package cn.edu.tsinghua.iginx.parquet.io.parquet;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import shaded.iginx.org.apache.parquet.io.api.Binary;
import shaded.iginx.org.apache.parquet.io.api.Converter;
import shaded.iginx.org.apache.parquet.io.api.GroupConverter;
import shaded.iginx.org.apache.parquet.io.api.PrimitiveConverter;
import shaded.iginx.org.apache.parquet.schema.GroupType;
import shaded.iginx.org.apache.parquet.schema.PrimitiveType;
import shaded.iginx.org.apache.parquet.schema.Type;

class IGroupConverter extends GroupConverter {
  private final Converter[] converters;

  private final Consumer<IRecord> setter;

  private final List<IginxRepeatedPrimitiveConverter> repeatedConverters = new ArrayList<>();

  public IGroupConverter(GroupType groupType, Consumer<IRecord> setter) {
    this.setter = setter;
    this.converters = new Converter[groupType.getFieldCount()];
    for (int fieldIndex = 0; fieldIndex < groupType.getFieldCount(); fieldIndex++) {
      Type fieldType = groupType.getType(fieldIndex);
      if (fieldType.isPrimitive()) {
        PrimitiveType primitiveType = fieldType.asPrimitiveType();
        if (primitiveType.isRepetition(PrimitiveType.Repetition.REPEATED)) {
          IginxRepeatedPrimitiveConverter converter =
              createRepeatedPrimitiveConverter(primitiveType.getPrimitiveTypeName(), fieldIndex);
          repeatedConverters.add(converter);
          converters[fieldIndex] = converter;
        } else {
          converters[fieldIndex] =
              createOptionalPrimitiveConverter(primitiveType.getPrimitiveTypeName(), fieldIndex);
        }
      } else {
        converters[fieldIndex] = createGroupConverter(fieldType.asGroupType(), fieldIndex);
      }
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  private IRecord currentRecord = null;

  @Override
  public void start() {
    currentRecord = new IRecord();
  }

  @Override
  public void end() {
    for (IginxRepeatedPrimitiveConverter converter : repeatedConverters) {
      currentRecord.add(converter.getIndex(), converter.build());
    }
    setter.accept(currentRecord);
  }

  private Converter createGroupConverter(GroupType fieldType, int index) {
    return new IGroupConverter(
        fieldType,
        record -> {
          currentRecord.add(index, record);
        });
  }

  private Converter createOptionalPrimitiveConverter(
      PrimitiveType.PrimitiveTypeName fieldTypeName, int index) {
    switch (fieldTypeName) {
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
            currentRecord.add(index, value);
          }
        };
      default:
        throw new IllegalArgumentException("Unsupported type: " + fieldTypeName);
    }
  }

  private IginxRepeatedPrimitiveConverter createRepeatedPrimitiveConverter(
      PrimitiveType.PrimitiveTypeName fieldTypeName, int index) {
    switch (fieldTypeName) {
      case INT32:
        return new IginxRepeatedPrimitiveConverter(index) {
          @Override
          public void addInt(int value) {
            buffer.add(Ints.toByteArray(value));
          }
        };
      case INT64:
        return new IginxRepeatedPrimitiveConverter(index) {
          @Override
          public void addLong(long value) {
            buffer.add(Longs.toByteArray(value));
          }
        };
      case FLOAT:
        return new IginxRepeatedPrimitiveConverter(index) {
          @Override
          public void addFloat(float value) {
            buffer.add(Ints.toByteArray(Float.floatToIntBits(value)));
          }
        };
      case DOUBLE:
        return new IginxRepeatedPrimitiveConverter(index) {
          @Override
          public void addDouble(double value) {
            buffer.add(Longs.toByteArray(Double.doubleToLongBits(value)));
          }
        };
      case BOOLEAN:
        return new IginxRepeatedPrimitiveConverter(index) {
          @Override
          public void addBoolean(boolean value) {
            buffer.add(new byte[] {(byte) (value ? 0xFF : 0)});
          }
        };
      case BINARY:
        return new IginxRepeatedPrimitiveConverter(index) {
          @Override
          public void addBinary(Binary value) {
            buffer.add(value.getBytes());
          }
        };
      default:
        throw new IllegalArgumentException("Unsupported type: " + fieldTypeName);
    }
  }

  static class IginxRepeatedPrimitiveConverter extends PrimitiveConverter {
    protected final List<byte[]> buffer = new ArrayList<>();
    private final int index;

    IginxRepeatedPrimitiveConverter(int index) {
      this.index = index;
    }

    int getIndex() {
      return index;
    }

    byte[] build() {
      if (buffer.isEmpty()) {
        return null;
      }
      int length = buffer.stream().mapToInt(bytes -> bytes.length).sum();
      byte[] result = new byte[length];
      int offset = 0;
      for (byte[] bytes : buffer) {
        System.arraycopy(bytes, 0, result, offset, bytes.length);
        offset += bytes.length;
      }
      buffer.clear();
      return result;
    }
  }
}
