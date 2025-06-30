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
package cn.edu.tsinghua.iginx.utils;

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.exception.IginxRuntimeException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

public class ByteUtils {

  public static byte booleanToByte(boolean x) {
    if (x) {
      return 1;
    } else {
      return 0;
    }
  }

  public static ByteBuffer getBytesFromVectorOfIginx(VectorSchemaRoot batch) throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(batch, null, out)) {
      writer.start();
      writer.writeBatch();
      writer.end();
      return ByteBuffer.wrap(out.toByteArray());
    }
  }

  public static Object[] getColumnValuesByDataType(
      List<ByteBuffer> valuesList,
      List<DataType> dataTypeList,
      List<ByteBuffer> bitmapList,
      int timestampsSize) {
    Object[] tempValues = new Object[valuesList.size()];
    for (int i = 0; i < valuesList.size(); i++) {
      Bitmap bitmap = new Bitmap(timestampsSize, bitmapList.get(i).array());
      int cnt = 0;
      for (int j = 0; j < timestampsSize; j++) {
        if (bitmap.get(j)) {
          cnt++;
        }
      }
      ByteBuffer buffer = valuesList.get(i);
      Object[] tempColumnValues = new Object[cnt];
      switch (dataTypeList.get(i)) {
        case BOOLEAN:
          for (int j = 0; j < cnt; j++) {
            tempColumnValues[j] = buffer.get() == 1;
          }
          break;
        case INTEGER:
          for (int j = 0; j < cnt; j++) {
            tempColumnValues[j] = buffer.getInt();
          }
          break;
        case LONG:
          for (int j = 0; j < cnt; j++) {
            tempColumnValues[j] = buffer.getLong();
          }
          break;
        case FLOAT:
          for (int j = 0; j < cnt; j++) {
            tempColumnValues[j] = buffer.getFloat();
          }
          break;
        case DOUBLE:
          for (int j = 0; j < cnt; j++) {
            tempColumnValues[j] = buffer.getDouble();
          }
          break;
        case BINARY:
          for (int j = 0; j < cnt; j++) {
            int length = buffer.getInt();
            byte[] bytes = new byte[length];
            buffer.get(bytes, 0, length);
            tempColumnValues[j] = bytes;
          }
          break;
        default:
          throw new UnsupportedOperationException(dataTypeList.get(i).toString());
      }
      tempValues[i] = tempColumnValues;
    }
    return tempValues;
  }

  public static Object[] getRowValuesByDataType(
      List<ByteBuffer> valuesList, List<DataType> dataTypeList, List<ByteBuffer> bitmapList) {
    Object[] tempValues = new Object[valuesList.size()];
    for (int i = 0; i < valuesList.size(); i++) {
      Bitmap bitmap = new Bitmap(dataTypeList.size(), bitmapList.get(i).array());
      List<Integer> indexes = new ArrayList<>();
      for (int j = 0; j < dataTypeList.size(); j++) {
        if (bitmap.get(j)) {
          indexes.add(j);
        }
      }
      Object[] tempRowValues = new Object[indexes.size()];
      for (int j = 0; j < indexes.size(); j++) {
        switch (dataTypeList.get(indexes.get(j))) {
          case BOOLEAN:
            tempRowValues[j] = valuesList.get(i).get() == 1;
            break;
          case INTEGER:
            tempRowValues[j] = valuesList.get(i).getInt();
            break;
          case LONG:
            tempRowValues[j] = valuesList.get(i).getLong();
            break;
          case FLOAT:
            tempRowValues[j] = valuesList.get(i).getFloat();
            break;
          case DOUBLE:
            tempRowValues[j] = valuesList.get(i).getDouble();
            break;
          case BINARY:
            int length = valuesList.get(i).getInt();
            byte[] bytes = new byte[length];
            valuesList.get(i).get(bytes, 0, length);
            tempRowValues[j] = bytes;
            break;
          default:
            throw new UnsupportedOperationException(dataTypeList.get(i).toString());
        }
      }
      tempValues[i] = tempRowValues;
    }
    return tempValues;
  }

  public static byte[] getByteArrayFromLongArray(long[] array) {
    ByteBuffer buffer = ByteBuffer.allocate(array.length * 8);
    buffer.asLongBuffer().put(array);
    return buffer.array();
  }

  public static long[] getLongArrayFromByteBuffer(ByteBuffer buffer) {
    long[] array = new long[buffer.array().length / 8];
    for (int i = 0; i < array.length; i++) {
      array[i] = buffer.getLong();
    }
    return array;
  }

  public static long[] getLongArrayFromByteArray(byte[] array) {
    return getLongArrayFromByteBuffer(ByteBuffer.wrap(array));
  }

  public static ByteBuffer getRowByteBuffer(Object[] values, List<DataType> dataTypes) {
    ByteBuffer buffer = ByteBuffer.allocate(getRowByteBufferSize(values, dataTypes));
    for (int i = 0; i < dataTypes.size(); i++) {
      DataType dataType = dataTypes.get(i);
      Object value = values[i];
      if (value == null) {
        continue;
      }
      switch (dataType) {
        case BOOLEAN:
          buffer.put(booleanToByte((boolean) value));
          break;
        case INTEGER:
          buffer.putInt((int) value);
          break;
        case LONG:
          buffer.putLong((long) value);
          break;
        case FLOAT:
          buffer.putFloat((float) value);
          break;
        case DOUBLE:
          buffer.putDouble((double) value);
          break;
        case BINARY:
          buffer.putInt(((byte[]) value).length);
          buffer.put((byte[]) value);
          break;
        default:
          throw new UnsupportedOperationException(dataType.toString());
      }
    }
    buffer.flip();
    return buffer;
  }

  public static ByteBuffer getColumnByteBuffer(Object[] values, DataType dataType) {
    ByteBuffer buffer = ByteBuffer.allocate(getColumnByteBufferSize(values, dataType));
    switch (dataType) {
      case BOOLEAN:
        for (Object value : values) {
          if (value == null) {
            continue;
          }
          buffer.put(booleanToByte((boolean) value));
        }
        break;
      case INTEGER:
        for (Object value : values) {
          if (value == null) {
            continue;
          }
          buffer.putInt((int) value);
        }
        break;
      case LONG:
        for (Object value : values) {
          if (value == null) {
            continue;
          }
          buffer.putLong((long) value);
        }
        break;
      case FLOAT:
        for (Object value : values) {
          if (value == null) {
            continue;
          }
          buffer.putFloat((float) value);
        }
        break;
      case DOUBLE:
        for (Object value : values) {
          if (value == null) {
            continue;
          }
          buffer.putDouble((double) value);
        }
        break;
      case BINARY:
        for (Object value : values) {
          if (value == null) {
            continue;
          }
          buffer.putInt(((byte[]) value).length);
          buffer.put((byte[]) value);
        }
        break;
      default:
        throw new UnsupportedOperationException(dataType.toString());
    }
    buffer.flip();
    return buffer;
  }

  public static int getRowByteBufferSize(Object[] values, List<DataType> dataTypes) {
    int size = 0;
    for (int i = 0; i < dataTypes.size(); i++) {
      DataType dataType = dataTypes.get(i);
      Object value = values[i];
      if (value == null) {
        continue;
      }
      switch (dataType) {
        case BOOLEAN:
          size += 1;
          break;
        case INTEGER:
        case FLOAT:
          size += 4;
          break;
        case LONG:
        case DOUBLE:
          size += 8;
          break;
        case BINARY:
          size += 4 + ((byte[]) value).length;
          break;
        default:
          throw new UnsupportedOperationException(dataType.toString());
      }
    }
    return size;
  }

  public static int getColumnByteBufferSize(Object[] values, DataType dataType) {
    int size = 0;
    switch (dataType) {
      case BOOLEAN:
        for (Object value : values) {
          if (value != null) {
            size += 1;
          }
        }
        break;
      case INTEGER:
      case FLOAT:
        for (Object value : values) {
          if (value != null) {
            size += 4;
          }
        }
        break;
      case LONG:
      case DOUBLE:
        for (Object value : values) {
          if (value != null) {
            size += 8;
          }
        }
        break;
      case BINARY:
        for (Object value : values) {
          if (value != null) {
            size += 4 + ((byte[]) value).length;
          }
        }
        break;
      default:
        throw new UnsupportedOperationException(dataType.toString());
    }
    return size;
  }

  public static Object getValueFromByteBufferByDataType(ByteBuffer buffer, DataType dataType) {
    Object value;
    switch (dataType) {
      case BOOLEAN:
        value = buffer.get() == 1;
        break;
      case INTEGER:
        value = buffer.getInt();
        break;
      case LONG:
        value = buffer.getLong();
        break;
      case FLOAT:
        value = buffer.getFloat();
        break;
      case DOUBLE:
        value = buffer.getDouble();
        break;
      case BINARY:
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes, 0, length);
        value = bytes;
        break;
      default:
        throw new UnsupportedOperationException(dataType.toString());
    }
    return value;
  }

  public static byte[] getBytesFromValueByDataType(Object value, DataType dataType) {
    byte[] bytes;
    switch (dataType) {
      case BOOLEAN:
        bytes = new byte[1];
        bytes[0] = (byte) ((boolean) value ? 0x01 : 0x00);
        return bytes;
      case INTEGER:
        bytes = new byte[4];
        for (int i = 0; i < 4; i++) {
          bytes[i] = (byte) (((int) value >>> 8 * i) & 0xff);
        }
        return bytes;
      case LONG:
        bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
          bytes[i] = (byte) (((long) value >>> 8 * i) & 0xff);
        }
        return bytes;
      case FLOAT:
        int valueInt = Float.floatToIntBits((float) value);
        bytes = new byte[4];
        for (int i = 0; i < 4; i++) {
          bytes[i] = (byte) ((valueInt >>> 8 * i) & 0xff);
        }
        return bytes;
      case DOUBLE:
        long valueLong = Double.doubleToRawLongBits((double) value);
        bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
          bytes[i] = (byte) ((valueLong >>> 8 * i) & 0xff);
        }
        return bytes;
      case BINARY:
        return (byte[]) value;
      default:
        throw new UnsupportedOperationException(dataType.toString());
    }
  }

  public static DataSet getDataFromArrowData(List<ByteBuffer> dataList) {
    if (dataList == null) {
      return null;
    }
    List<Long> keys = new ArrayList<>();
    List<String> paths = new ArrayList<>();
    List<DataType> dataTypeList = new ArrayList<>();
    List<Map<String, String>> tagsList = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();
    boolean metaCollected = false;
    boolean hasKey = false;

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      for (ByteBuffer data : dataList) {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data.array());
            ArrowStreamReader reader = new ArrowStreamReader(byteArrayInputStream, allocator);
            VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
          if (!metaCollected) {
            root.getSchema()
                .getFields()
                .forEach(
                    field -> {
                      paths.add(getCompatibleFullName(field.getName(), field.getMetadata()));
                      dataTypeList.add(TypeUtils.toDataType(field.getType()));
                      tagsList.add(field.getMetadata());
                    });
            if (!paths.isEmpty() && paths.get(0).equals(GlobalConstant.KEY_NAME)) {
              hasKey = true;
              paths.remove(0);
              dataTypeList.remove(0);
              tagsList.remove(0);
            }
            metaCollected = true;
          }
          while (reader.loadNextBatch()) {
            int rowCnt = root.getRowCount();
            int colCnt = root.getFieldVectors().size();
            List<FieldVector> vectors = root.getFieldVectors();
            for (int i = 0; i < rowCnt; i++) {
              List<Object> row = new ArrayList<>();
              int start = 0;
              if (hasKey) {
                keys.add((Long) vectors.get(0).getObject(i));
                start++;
              }
              for (int j = start; j < colCnt; j++) {
                row.add(vectors.get(j).getObject(i));
              }
              values.add(row);
            }
          }
        } catch (IOException e) {
          throw new IginxRuntimeException(e);
        }
      }
    }
    return new DataSet(keys, paths, dataTypeList, tagsList, values, hasKey);
  }

  // 检测是否为函数调用结果
  public static final Pattern FUNC_CALL_PATTERN = Pattern.compile("(.*)\\((.*)\\)$");

  // 获取与原有基于 RowStream 的执行器兼容的 fullName
  public static String getCompatibleFullName(String name, Map<String, String> metadata) {
    Matcher matcher = FUNC_CALL_PATTERN.matcher(name);
    if (matcher.matches() && metadata != null && !metadata.isEmpty()) {
      String funcName = matcher.group(1);
      String pathName = matcher.group(2);
      return funcName + "(" + TagKVUtils.toFullName(pathName, metadata) + ")";
    } else {
      return TagKVUtils.toFullName(name, metadata);
    }
  }

  public static class DataSet {

    private final long[] keys;
    private final List<String> paths;
    private final List<DataType> dataTypeList;
    private final List<Map<String, String>> tagsList;
    private final List<List<Object>> values;
    private final int rowSize;
    private final int colSize;

    public DataSet(
        List<Long> keys,
        List<String> paths,
        List<DataType> dataTypeList,
        List<Map<String, String>> tagsList,
        List<List<Object>> values,
        boolean hasKey) {
      this.keys = hasKey ? keys.stream().mapToLong(Long::longValue).toArray() : null;
      this.paths = paths;
      this.dataTypeList = dataTypeList;
      this.tagsList = tagsList;
      this.values = values;
      this.rowSize = values.size();
      this.colSize = dataTypeList.size();
    }

    public long[] getKeys() {
      return keys;
    }

    public List<String> getPaths() {
      return paths;
    }

    public List<DataType> getDataTypeList() {
      return dataTypeList;
    }

    public List<Map<String, String>> getTagsList() {
      return tagsList;
    }

    public List<List<Object>> getValues() {
      return values;
    }

    public int getRowSize() {
      return rowSize;
    }

    public int getColSize() {
      return colSize;
    }

    public boolean hasKey() {
      return keys != null;
    }
  }
}
