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

import java.util.*;
import javax.annotation.Nonnull;

public class RangeSet<K extends Comparable<K>> implements Collection<Range<K>>, Cloneable {

  private TreeMap<K, K> lefts = new TreeMap<>(Range.leftComparator());

  private TreeMap<K, K> rights = new TreeMap<>(Range.rightComparator());

  public RangeSet() {}

  public RangeSet(K left, K right) {
    this.add(new Range<>(left, right));
  }

  @Override
  public int size() {
    return lefts.size();
  }

  @Override
  public boolean isEmpty() {
    return lefts.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  public Iterator<Range<K>> iterator() {
    assert lefts.size() == rights.size();

    return new Iterator<Range<K>>() {
      private final Iterator<Map.Entry<K, K>> iterator = lefts.entrySet().iterator();

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Range<K> next() {
        Map.Entry<K, K> nextEntry = iterator.next();
        return new Range<>(nextEntry.getKey(), nextEntry.getValue());
      }
    };
  }

  @Nonnull
  @Override
  public Object[] toArray() {
    Object[] result = new Object[lefts.size()];
    int i = 0;
    for (Range<K> range : this) {
      result[i++] = range;
    }
    return result;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public <T> T[] toArray(@Nonnull T[] a) {
    if (!a.getClass().getComponentType().equals(Range.class)) {
      throw new UnsupportedOperationException("Only support Range[]");
    }
    return (T[]) toArray();
  }

  @Override
  public boolean add(Range<K> range) {
    assert lefts.size() == rights.size();

    if (range == null || range.isEmpty()) {
      return false;
    }

    if (!lefts.isEmpty()) {
      range = connectNeighbor(range);
      removeOverLeap(range);
    }

    lefts.put(range.first(), range.last());
    rights.put(range.last(), range.first());
    return true;
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(@Nonnull Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(@Nonnull Collection<? extends Range<K>> c) {
    boolean result = false;
    for (Range<K> range : c) {
      result |= add(range);
    }
    return result;
  }

  @Override
  public boolean removeAll(@Nonnull Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(@Nonnull Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    lefts.clear();
    rights.clear();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RangeSet<?> rangeSet = (RangeSet<?>) o;
    return Objects.equals(lefts, rangeSet.lefts) && Objects.equals(rights, rangeSet.rights);
  }

  @Override
  public int hashCode() {
    return Objects.hash(lefts, rights);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", RangeSet.class.getSimpleName() + "[", "]")
        .add("lefts=" + lefts)
        .add("rights=" + rights)
        .toString();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object clone() {
    try {
      RangeSet<K> cloned = (RangeSet<K>) super.clone();
      cloned.lefts = (TreeMap<K, K>) lefts.clone();
      cloned.rights = (TreeMap<K, K>) rights.clone();
      return cloned;
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(e);
    }
  }

  @SuppressWarnings("rawtypes")
  private static final RangeSet FULL = new RangeSet<>(null, null);

  @SuppressWarnings("rawtypes")
  private static final RangeSet EMPTY = new RangeSet<>();

  @SuppressWarnings("unchecked")
  public static <K extends Comparable<K>> RangeSet<K> full() {
    return (RangeSet<K>) FULL;
  }

  @SuppressWarnings("unchecked")
  public static <K extends Comparable<K>> RangeSet<K> empty() {
    return (RangeSet<K>) EMPTY;
  }

  @Nonnull
  private Range<K> connectNeighbor(Range<K> range) {
    K leftResult = range.first();
    Map.Entry<K, K> leftLower = lefts.lowerEntry(range.first());
    if (leftLower != null
        && Range.rightComparator().compare(leftLower.getValue(), range.first()) >= 0) {
      leftResult = leftLower.getKey();
    }

    K rightResult = range.last();
    Map.Entry<K, K> rightHigher = rights.higherEntry(range.last());
    if (rightHigher != null
        && Range.leftComparator().compare(rightHigher.getValue(), range.last()) <= 0) {
      rightResult = rightHigher.getKey();
    }

    return new Range<>(leftResult, rightResult);
  }

  private void removeOverLeap(Range<K> range) {
    Map.Entry<K, K> upRight = lefts.ceilingEntry(range.first());
    Map.Entry<K, K> downLeft = rights.floorEntry(range.last());
    if (upRight != null
        && downLeft != null
        && Range.leftComparator().compare(upRight.getKey(), downLeft.getValue()) <= 0) {
      lefts.subMap(upRight.getKey(), true, downLeft.getValue(), true).clear();
      rights.subMap(upRight.getValue(), true, downLeft.getKey(), true).clear();
    }
  }
}
