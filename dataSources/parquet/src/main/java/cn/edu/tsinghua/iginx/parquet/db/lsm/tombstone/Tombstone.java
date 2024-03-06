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

package cn.edu.tsinghua.iginx.parquet.db.lsm.tombstone;

import cn.edu.tsinghua.iginx.parquet.db.lsm.table.DataBuffer;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.*;
import javax.annotation.Nonnull;

public class Tombstone<K extends Comparable<K>, F> {
  private final Map<F, RangeSet<K>> deletedRanges;

  private final Set<F> deletedColumns;

  private final RangeSet<K> deletedRows;

  public Tombstone() {
    this(new HashMap<>(), new HashSet<>(), TreeRangeSet.create());
  }

  private Tombstone(
      Map<F, RangeSet<K>> deletedRanges, Set<F> deletedColumns, RangeSet<K> deletedRows) {
    this.deletedRanges = deletedRanges;
    this.deletedColumns = deletedColumns;
    this.deletedRows = deletedRows;
  }

  public void delete(@Nonnull Set<F> fields, @Nonnull RangeSet<K> ranges) {
    RangeSet<K> validRanges = TreeRangeSet.create(ranges);
    validRanges.removeAll(deletedRows);
    if (validRanges.isEmpty()) {
      return;
    }
    for (F deletedField : fields) {
      if (deletedColumns.contains(deletedField)) {
        continue;
      }
      deletedRanges
          .computeIfAbsent(Objects.requireNonNull(deletedField), k -> TreeRangeSet.create())
          .addAll(ranges);
    }
  }

  public void delete(@Nonnull RangeSet<K> ranges) {
    deletedRows.addAll(ranges);
    deletedRanges.values().forEach(rangeSet -> rangeSet.removeAll(ranges));
    deletedRanges.values().removeIf(RangeSet::isEmpty);
  }

  public void delete(@Nonnull Set<F> fields) {
    deletedColumns.addAll(fields);
    for (F field : fields) {
      deletedRanges.remove(field);
    }
  }

  public void delete(Tombstone<K, F> tombstone) {
    delete(tombstone.getDeletedRows());
    delete(tombstone.getDeletedColumns());
    tombstone
        .getDeletedRanges()
        .forEach((field, rangeSet) -> delete(Collections.singleton(field), rangeSet));
  }

  public void reset() {
    deletedRanges.clear();
  }

  public static <F, K extends Comparable<K>> void playback(
      Tombstone<K, F> tombstone, RangeSet<K> flushedRangeSet) {
    flushedRangeSet.removeAll(tombstone.getDeletedRows());
  }

  public static <K extends Comparable<K>, F, T> void playback(
      Tombstone<K, F> tombstone, Map<F, T> types) {
    for (F field : tombstone.getDeletedColumns()) {
      types.remove(field);
    }
  }

  public static <K extends Comparable<K>, F, V> void playback(
      Tombstone<K, F> tombstone, DataBuffer<K, F, V> buffer) {
    for (F field : tombstone.getDeletedColumns()) {
      buffer.remove(Collections.singleton(field));
    }
    buffer.remove(tombstone.getDeletedRows());
    for (Map.Entry<F, RangeSet<K>> entry : tombstone.getDeletedRanges().entrySet()) {
      F field = entry.getKey();
      RangeSet<K> rangeSet = entry.getValue();
      buffer.remove(Collections.singleton(field), rangeSet);
    }
  }

  public Tombstone<K, F> copy() {
    return new Tombstone<>(
        new HashMap<>(deletedRanges),
        new HashSet<>(deletedColumns),
        TreeRangeSet.create(deletedRows));
  }

  public Map<F, RangeSet<K>> getDeletedRanges() {
    return deletedRanges;
  }

  public Set<F> getDeletedColumns() {
    return deletedColumns;
  }

  public RangeSet<K> getDeletedRows() {
    return deletedRows;
  }

  public boolean isEmpty() {
    return deletedRanges.isEmpty() && deletedColumns.isEmpty() && deletedRows.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Tombstone<?, ?> that = (Tombstone<?, ?>) o;
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
    return new StringJoiner(", ", Tombstone.class.getSimpleName() + "{", "}")
        .add("deletedRanges=" + deletedRanges)
        .toString();
  }
}
