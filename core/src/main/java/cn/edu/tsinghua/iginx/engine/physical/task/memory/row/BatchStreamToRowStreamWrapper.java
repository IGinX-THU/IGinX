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
package cn.edu.tsinghua.iginx.engine.physical.task.memory.row;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskMetrics;
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import java.util.*;
import javax.annotation.Nullable;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;

public class BatchStreamToRowStreamWrapper implements RowStream {

  private final BatchStream previous;
  private final TaskMetrics taskMetrics;
  private final Queue<Row> rowCache = new ArrayDeque<>();
  private Header header;
  private int[] columnIndices;
  private Integer keyIndex;

  public BatchStreamToRowStreamWrapper(BatchStream previous, TaskMetrics taskMetrics) {
    this.previous = Objects.requireNonNull(previous);
    this.taskMetrics = Objects.requireNonNull(taskMetrics);
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (header == null || columnIndices == null) {
      BatchSchema schema = previous.getSchema();
      try (StopWatch watch = new StopWatch(taskMetrics::accumulateCpuTime)) {
        header = getHeader(schema);
        initColumnIndex(schema);
      }
    }
    return header;
  }

  public static Header getHeader(BatchSchema schema) {
    Field keyField = schema.hasKey() ? Field.KEY : null;
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < schema.getFieldCount(); i++) {
      if (schema.hasKey() && i == schema.getKeyIndex()) {
        continue;
      }
      fields.add(toIginxField(schema.getField(i)));
    }
    return new Header(keyField, fields);
  }

  public static Field toIginxField(org.apache.arrow.vector.types.pojo.Field field) {
    String fieldName = field.getName();
    DataType fieldType = Schemas.toDataType(field.getType());
    Map<String, String> tags = new HashMap<>(field.getMetadata());
    String fullFieldName = ByteUtils.getCompatibleFullName(fieldName, tags);
    return new Field(fieldName, fullFieldName, fieldType, tags);
  }

  private void initColumnIndex(BatchSchema schema) {
    List<Integer> columnIndices = new ArrayList<>();
    if (schema.hasKey()) {
      keyIndex = schema.getKeyIndex();
    } else {
      keyIndex = null;
    }
    for (int i = 0; i < schema.getFieldCount(); i++) {
      if (schema.hasKey() && i == schema.getKeyIndex()) {
        continue;
      }
      columnIndices.add(i);
    }
    this.columnIndices = columnIndices.stream().mapToInt(Integer::intValue).toArray();
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    tryFetchNextBatchIfNeed();
    return !rowCache.isEmpty();
  }

  @Override
  public Row next() throws PhysicalException {
    return rowCache.remove();
  }

  private void tryFetchNextBatchIfNeed() throws PhysicalException {
    while (rowCache.isEmpty() && previous.hasNext()) {
      try (Batch arrowBatch = previous.getNext()) {
        try (StopWatch watch = new StopWatch(taskMetrics::accumulateCpuTime)) {
          List<Row> rows = toRows(arrowBatch);
          taskMetrics.accumulateAffectRows(rows.size());
          rowCache.addAll(rows);
        }
      }
    }
  }

  private List<Row> toRows(Batch arrowBatch) throws PhysicalException {
    Header header = getHeader();

    BaseIntVector keyIndices = null;
    BigIntVector keyVector = null;

    if (keyIndex != null) {
      FieldVector vector = arrowBatch.getVector(keyIndex);
      DictionaryEncoding dictionary = vector.getField().getDictionary();
      if (dictionary != null) {
        keyIndices = (BaseIntVector) vector;
        keyVector =
            (BigIntVector)
                arrowBatch.getDictionaryProvider().lookup(dictionary.getId()).getVector();
      } else {
        keyVector = (BigIntVector) vector;
      }
    }

    BaseIntVector[] indices = new BaseIntVector[columnIndices.length];
    FieldVector[] valueVectors = new FieldVector[columnIndices.length];

    for (int i = 0; i < columnIndices.length; i++) {
      FieldVector vector = arrowBatch.getVector(columnIndices[i]);
      DictionaryEncoding dictionary = vector.getField().getDictionary();
      if (dictionary != null) {
        indices[i] = (BaseIntVector) vector;
        valueVectors[i] = arrowBatch.getDictionaryProvider().lookup(dictionary.getId()).getVector();
      } else {
        valueVectors[i] = vector;
      }
    }

    return toRows(
        header, null, keyIndices, keyVector, indices, valueVectors, arrowBatch.getRowCount());
  }

  private static List<Row> toRows(
      Header header,
      @Nullable BaseIntVector selection,
      @Nullable BaseIntVector keyIndices,
      @Nullable BigIntVector keyVector,
      BaseIntVector[] valueIndices,
      FieldVector[] valueVectors,
      int rowCount) {
    List<Row> rows = new ArrayList<>();
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      int selectionIndex = rowIndex;
      if (selection != null) {
        if (selection.getField().isNullable() && selection.isNull(rowIndex)) {
          continue;
        }
        selectionIndex = (int) selection.getValueAsLong(rowIndex);
      }
      Object[] values = new Object[valueVectors.length];
      for (int columnIndex = 0; columnIndex < valueVectors.length; columnIndex++) {
        int valueRowIndex;
        if (valueIndices[columnIndex] != null) {
          valueRowIndex = (int) valueIndices[columnIndex].getValueAsLong(rowIndex);
        } else {
          valueRowIndex = rowIndex;
        }
        values[columnIndex] = valueVectors[columnIndex].getObject(valueRowIndex);
      }
      if (keyVector == null) {
        rows.add(new Row(header, values));
      } else {
        int keyRowIndex;
        if (keyIndices != null) {
          keyRowIndex = (int) keyIndices.getValueAsLong(rowIndex);
        } else {
          keyRowIndex = rowIndex;
        }
        long key = keyVector.get(keyRowIndex);
        rows.add(new Row(header, key, values));
      }
    }
    return rows;
  }

  @Override
  public void close() throws PhysicalException {
    previous.close();
  }
}
