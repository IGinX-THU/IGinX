package cn.edu.tsinghua.iginx.parquet.entity;

import java.util.Comparator;
import java.util.Objects;
import javax.annotation.Nonnull;

public class Range<K extends Comparable<K>> {

  private static final Comparator<Comparable<Object>> LEFT_COMPARATOR =
      Comparator.nullsFirst(Comparator.naturalOrder());

  private static final Comparator<Comparable<Object>> RIGHT_COMPARATOR =
      Comparator.nullsLast(Comparator.naturalOrder());

  private final K first;

  private final K last;

  public Range(K first, K last) {
    this.first = first;
    this.last = last;
  }

  public K first() {
    return first;
  }

  public K last() {
    return last;
  }

  public boolean isEmpty() {
    return first != null && last != null && first.compareTo(last) > 0;
  }

  public boolean isFull() {
    return first == null && last == null;
  }

  @Nonnull
  public Range<K> intersect(@Nonnull Range<K> other) {
    K maxLeft = leftComparator().compare(first, other.first) > 0 ? first : other.first;
    K minRight = rightComparator().compare(last, other.last) < 0 ? last : other.last;
    return new Range<>(maxLeft, minRight);
  }

  @Nonnull
  public Range<K> union(@Nonnull Range<K> other) {
    K minLeft = leftComparator().compare(first, other.first) < 0 ? first : other.first;
    K maxRight = rightComparator().compare(last, other.last) > 0 ? last : other.last;
    return new Range<>(minLeft, maxRight);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Range<?> range = (Range<?>) o;
    return Objects.equals(first, range.first) && Objects.equals(last, range.last);
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, last);
  }

  @Override
  public String toString() {
    return "Range{" + "first=" + first + ", last=" + last + '}';
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <T extends Comparable> Comparator<T> leftComparator() {
    return (Comparator<T>) LEFT_COMPARATOR;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <T extends Comparable> Comparator<T> rightComparator() {
    return (Comparator<T>) RIGHT_COMPARATOR;
  }
}
