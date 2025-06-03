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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.filesystem.common.IginxPaths;
import cn.edu.tsinghua.iginx.filesystem.common.Patterns;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.IOException;
import java.util.*;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import shaded.iginx.org.apache.parquet.schema.GroupType;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.PrimitiveType;
import shaded.iginx.org.apache.parquet.schema.Type;
import shaded.parquet.it.unimi.dsi.fastutil.longs.Long2IntLinkedOpenHashMap;
import shaded.parquet.it.unimi.dsi.fastutil.longs.LongArrayList;
import shaded.parquet.it.unimi.dsi.fastutil.longs.LongList;

public class ParquetFormatRowTable extends Table {

  private ParquetFormatRowTable(Header header, List<Row> rows) {
    super(header, rows);
  }

  public static ParquetFormatRowTable from(
      IginxParquetReader reader, @Nullable String prefix, Set<String> patterns) throws IOException {
    Builder builder = new Builder(reader.getProjectedSchema());
    while (true) {
      IginxGroup group = reader.read();
      if (group == null) {
        break;
      }
      long key = reader.getCurrentRowIndex();
      builder.add(key, group);
    }
    return builder.build(prefix, patterns);
  }

  public static class Builder {
    private final MessageType schema;
    // ValueBuffer or Object[] or List may be in buffer
    private final Object[] buffers;
    private long maxKey = -1;

    public Builder(MessageType schema) {
      this.schema = Objects.requireNonNull(schema);
      this.buffers = new Object[schema.getFieldCount()];
    }

    public Builder add(long key, IginxGroup group) {
      addToBuffer(this.buffers, key, group);
      if (key > maxKey) {
        maxKey = key;
      }
      return this;
    }

    @SuppressWarnings("unchecked")
    private static void addToBuffer(Object[] buffers, long key, IginxGroup group) {
      Object[] values = group.getData();
      for (int i = 0; i < values.length; i++) {
        Object value = values[i];
        if (value instanceof IginxGroup) {
          IginxGroup subGroup = (IginxGroup) value;
          if (buffers[i] == null) {
            buffers[i] = new Object[subGroup.getData().length];
          }
          addToBuffer((Object[]) buffers[i], key, subGroup);
        } else if (value instanceof List) {
          if (buffers[i] == null) {
            buffers[i] = new ArrayList<>();
          }
          List<Object> bufferList = (List<Object>) buffers[i];
          List<Object> valueList = (List<Object>) value;
          for (int j = 0; j < valueList.size(); j++) {
            Object valueItem = valueList.get(j);
            if (valueItem instanceof IginxGroup) {
              IginxGroup groupItem = (IginxGroup) valueItem;
              if (j == bufferList.size()) {
                bufferList.add(new Object[groupItem.getData().length]);
              }
              Object[] groupItemBuffers = (Object[]) bufferList.get(j);
              addToBuffer(groupItemBuffers, key, groupItem);
            } else {
              if (j == bufferList.size()) {
                bufferList.add(new ValueBuffer());
              }
              ((ValueBuffer) bufferList.get(j)).add(key, valueItem);
            }
          }
        } else {
          if (buffers[i] == null) {
            buffers[i] = new ValueBuffer();
          }
          ((ValueBuffer) buffers[i]).add(key, value);
        }
      }
    }

    public ParquetFormatRowTable build(@Nullable String prefix, Set<String> patterns) {
      List<Field> fields = new ArrayList<>();
      List<ValueBuffer> valueBuffers = new ArrayList<>();
      List<String> currentPrefix = new ArrayList<>(Arrays.asList(IginxPaths.split(prefix)));
      foreachBuffer(
          schema,
          buffers,
          currentPrefix,
          (typeName, valueBuffer) -> {
            String fieldName = IginxPaths.join(currentPrefix);
            if (!valueBuffer.isEmpty() && Patterns.match(patterns, fieldName)) {
              DataType type = ParquetFormatReader.toIginxType(typeName);
              Field field = new Field(fieldName, type);
              fields.add(field);
              valueBuffers.add(valueBuffer);
            }
          });

      Header header = new Header(Field.KEY, fields);
      List<Row> rows = getRowsFromBuffers(valueBuffers, header);

      return new ParquetFormatRowTable(header, rows);
    }

    @SuppressWarnings("unchecked")
    private static void foreachBuffer(
        GroupType type,
        Object[] buffers,
        List<String> prefix,
        BiConsumer<PrimitiveType.PrimitiveTypeName, ValueBuffer> consumer) {
      for (int i = 0; i < buffers.length; i++) {
        Object buffer = buffers[i];
        Type fieldType = type.getType(i);
        prefix.add(fieldType.getName());
        if (buffer instanceof Object[]) {
          foreachBuffer(fieldType.asGroupType(), (Object[]) buffer, prefix, consumer);
        } else if (buffer instanceof List) {
          List<Object> bufferList = (List<Object>) buffer;
          for (int j = 0; j < bufferList.size(); j++) {
            Object bufferItem = bufferList.get(j);
            prefix.add(String.valueOf(j));
            if (bufferItem instanceof Object[]) {
              foreachBuffer(fieldType.asGroupType(), (Object[]) bufferItem, prefix, consumer);
            } else {
              consumer.accept(
                  fieldType.asPrimitiveType().getPrimitiveTypeName(), (ValueBuffer) bufferItem);
            }
            prefix.remove(prefix.size() - 1);
          }
        } else {
          consumer.accept(fieldType.asPrimitiveType().getPrimitiveTypeName(), (ValueBuffer) buffer);
        }
        prefix.remove(prefix.size() - 1);
      }
    }

    private static List<Row> getRowsFromBuffers(List<ValueBuffer> valueBuffers, Header header) {
      Long2IntLinkedOpenHashMap keyToIndex = new Long2IntLinkedOpenHashMap();
      List<Row> rows = new ArrayList<>();

      for (int columnIndex = 0; columnIndex < valueBuffers.size(); columnIndex++) {
        final int columnIndexFinal = columnIndex;
        ValueBuffer valueBuffer = valueBuffers.get(columnIndex);
        valueBuffer.foreach(
            (key, value) -> {
              int rowIndex = keyToIndex.getOrDefault(key, -1);
              Object[] toSet;
              if (rowIndex == -1) {
                rowIndex = keyToIndex.size();
                toSet = new Object[valueBuffers.size()];
                rows.add(new Row(header, key, toSet));
                keyToIndex.put(key, rowIndex);
              } else {
                toSet = rows.get(rowIndex).getValues();
              }
              toSet[columnIndexFinal] = value;
            });
      }

      rows.sort(Comparator.comparing(Row::getKey));
      return rows;
    }
  }

  private static class ValueBuffer {
    private final LongList keys = new LongArrayList();
    private final List<Object> values = new ArrayList<>();

    public void add(long key, Object value) {
      keys.add(key);
      values.add(value);
    }

    interface LongObjConsumer {
      void accept(long key, Object value);
    }

    public void foreach(LongObjConsumer consumer) {
      for (int i = 0; i < keys.size(); i++) {
        consumer.accept(keys.getLong(i), values.get(i));
      }
    }

    public boolean isEmpty() {
      return keys.isEmpty();
    }
  }
}
