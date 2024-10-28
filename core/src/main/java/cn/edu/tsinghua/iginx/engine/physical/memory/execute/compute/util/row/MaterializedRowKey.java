package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.row;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import cn.edu.tsinghua.iginx.engine.shared.data.read.ColumnBuilder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class MaterializedRowKey {
  private final Object[] key;
  private final transient int hashCode;

  public MaterializedRowKey(Object[] key) {
    this.key = key;
    hashCode = Arrays.deepHashCode(key);
  }

  public static MaterializedRowKey of(RowCursor cursor) {
    Object[] key = ValueVectors.getObjects(cursor.getColumns(), cursor.getPosition());
    return new MaterializedRowKey(key);
  }

  public static VectorSchemaRoot merge(
      BufferAllocator allocator, Schema groupKeySchema, List<MaterializedRowKey> groupKeys)
      throws ComputeException {
    VectorSchemaRoot root = VectorSchemaRoot.create(groupKeySchema, allocator);
    List<ColumnBuilder> columnBuilders = new ArrayList<>();
    try {
      for (FieldVector fieldVector : root.getFieldVectors()) {
        if (!ColumnBuilder.support(fieldVector)) {
          throw new ComputeException("Unsupported type: " + fieldVector.getMinorType());
        }
        columnBuilders.add(ColumnBuilder.create(fieldVector));
        fieldVector.setInitialCapacity(groupKeys.size());
      }
    } catch (ComputeException e) {
      root.close();
      throw e;
    }
    for (MaterializedRowKey groupKey : groupKeys) {
      for (int i = 0; i < groupKey.key.length; i++) {
        columnBuilders.get(i).append(groupKey.key[i]);
      }
    }
    root.setRowCount(groupKeys.size());
    return root;
  }

  public Object[] getData() {
    return key;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MaterializedRowKey)) {
      return false;
    }
    MaterializedRowKey materializedRowKey = (MaterializedRowKey) o;
    return Arrays.deepEquals(key, materializedRowKey.key);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }
}
