package cn.edu.tsinghua.iginx.parquet.db;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.*;
import javax.annotation.Nonnull;

public class RangeTombstone<K extends Comparable<K>, F> {

  private final Map<F, RangeSet<K>> deletedRanges = new HashMap<>();

  public void delete(@Nonnull Set<F> fields, @Nonnull RangeSet<K> ranges) {
    for (F deletedField : fields) {
      deletedRanges.computeIfAbsent(deletedField, k -> TreeRangeSet.create()).addAll(ranges);
    }
  }

  public void delete(@Nonnull RangeSet<K> ranges) {
    delete(Collections.singleton(null), ranges);
  }

  public void delete(@Nonnull Set<F> fields) {
    delete(fields, ImmutableRangeSet.of(Range.all()));
  }

  public void reset() {
    deletedRanges.clear();
  }

  public static <K extends Comparable<K>, F, V> void playback(
      RangeTombstone<K, F> tombstone, DataBuffer<K, F, V> buffer) {
    for (Map.Entry<F, RangeSet<K>> entry : tombstone.getDeletedRanges().entrySet()) {
      F field = entry.getKey();
      RangeSet<K> rangeSet = entry.getValue();
      if (field == null) {
        buffer.remove(rangeSet);
      } else if (rangeSet.encloses(Range.all())) {
        buffer.remove(Collections.singleton(field));
      } else {
        buffer.remove(Collections.singleton(field), rangeSet);
      }
    }
  }

  public Map<F, RangeSet<K>> getDeletedRanges() {
    return deletedRanges;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RangeTombstone<?, ?> that = (RangeTombstone<?, ?>) o;
    return Objects.equals(deletedRanges, that.deletedRanges);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deletedRanges);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", RangeTombstone.class.getSimpleName() + "{", "}")
        .add("deletedRanges=" + deletedRanges)
        .toString();
  }
}
