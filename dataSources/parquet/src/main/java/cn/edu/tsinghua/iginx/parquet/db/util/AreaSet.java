package cn.edu.tsinghua.iginx.parquet.db.util;

import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.*;

public class AreaSet<K extends Comparable<K>, F> {
  private final Map<F, RangeSet<K>> deletedRanges;

  private final Set<F> deletedColumns;

  private final RangeSet<K> deletedRows;

  public AreaSet() {
    this(new HashMap<>(), new HashSet<>(), TreeRangeSet.create());
  }

  private AreaSet(
      Map<F, RangeSet<K>> deletedRanges, Set<F> deletedColumns, RangeSet<K> deletedRows) {
    this.deletedRanges = deletedRanges;
    this.deletedColumns = deletedColumns;
    this.deletedRows = deletedRows;
  }

  public void add(Set<F> fields, RangeSet<K> ranges) {
    RangeSet<K> validRanges = TreeRangeSet.create(ranges);
    validRanges.removeAll(deletedRows);
    if (validRanges.isEmpty()) {
      return;
    }
    Set<F> validFields = new HashSet<>(fields);
    validFields.removeAll(deletedColumns);
    if (validFields.isEmpty()) {
      return;
    }
    for (F field : validFields) {
      deletedRanges
          .computeIfAbsent(Objects.requireNonNull(field), k -> TreeRangeSet.create())
          .addAll(validRanges);
    }
  }

  public void add(RangeSet<K> ranges) {
    if (ranges.isEmpty()) {
      return;
    }
    deletedRows.addAll(ranges);
    deletedRanges.values().forEach(rangeSet -> rangeSet.removeAll(ranges));
    deletedRanges.values().removeIf(RangeSet::isEmpty);
  }

  public void add(Set<F> fields) {
    if (fields.isEmpty()) {
      return;
    }
    deletedColumns.addAll(fields);
    deletedRanges.keySet().removeAll(fields);
  }

  public void addAll(AreaSet<K, F> areas) {
    add(areas.getKeys());
    add(areas.getFields());
    areas.getSegments().forEach((field, rangeSet) -> add(Collections.singleton(field), rangeSet));
  }

  public void clear() {
    deletedRows.clear();
    deletedColumns.clear();
    deletedRanges.clear();
  }

  public Map<F, RangeSet<K>> getSegments() {
    return deletedRanges;
  }

  public Set<F> getFields() {
    return deletedColumns;
  }

  public RangeSet<K> getKeys() {
    return deletedRows;
  }

  public boolean isEmpty() {
    return deletedRanges.isEmpty() && deletedColumns.isEmpty() && deletedRows.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AreaSet<?, ?> that = (AreaSet<?, ?>) o;
    return Objects.equals(deletedRanges, that.deletedRanges)
        && Objects.equals(deletedColumns, that.deletedColumns)
        && Objects.equals(deletedRows, that.deletedRows);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deletedRanges, deletedColumns, deletedRows);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", AreaSet.class.getSimpleName() + "[", "]")
        .add("deletedRanges=" + deletedRanges)
        .add("deletedColumns=" + deletedColumns)
        .add("deletedRows=" + deletedRows)
        .toString();
  }
}
