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
