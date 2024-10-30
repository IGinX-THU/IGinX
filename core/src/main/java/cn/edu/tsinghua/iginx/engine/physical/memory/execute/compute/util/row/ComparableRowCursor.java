package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.row;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.WillNotClose;

public class ComparableRowCursor extends RowCursor implements Comparable<RowCursor> {

  private final RowCursorComparator comparator;

  public ComparableRowCursor(@WillNotClose VectorSchemaRoot table) {
    this(table.getFieldVectors().toArray(new FieldVector[0]), 0, RowCursorComparator.of(table));
  }

  protected ComparableRowCursor(FieldVector[] columns, int index, RowCursorComparator comparator) {
    super(columns, index);
    this.comparator = comparator;
  }

  public ComparableRowCursor withPosition(int position) {
    return new ComparableRowCursor(this.columns, position, this.comparator);
  }

  @Override
  public int compareTo(RowCursor o) {
    return comparator.compare(this, o);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    RowCursor that = (RowCursor) o;
    return comparator.equals(this, that);
  }
}
