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
package cn.edu.tsinghua.iginx.minio.tree.format;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.util.ArrayDeque;
import java.util.Deque;

/** Walks through keys in a range set in ascending order, keys are row indexes in [0, limit). */
final class KeyCursor {

  /** closed ranges in ascending order */
  private final Deque<long[]> ranges = new ArrayDeque<>();

  private long key = -1;

  KeyCursor(RangeSet<Long> keys, long limit) {
    for (Range<Long> range : keys.subRangeSet(Range.closedOpen(0L, limit)).asRanges()) {
      long lower = range.lowerEndpoint();
      if (range.lowerBoundType() == BoundType.OPEN) {
        lower++;
      }
      long upper = limit - 1;
      if (range.hasUpperBound()) {
        upper = range.upperEndpoint();
        if (range.upperBoundType() == BoundType.OPEN) {
          upper--;
        }
      }
      if (lower <= upper) {
        ranges.add(new long[] {lower, upper});
      }
    }
  }

  /** move to the next key, false if there is no more key */
  boolean advance() {
    long candidate = key + 1;
    while (!ranges.isEmpty()) {
      long[] range = ranges.peekFirst();
      if (candidate < range[0]) {
        candidate = range[0];
      }
      if (candidate <= range[1]) {
        key = candidate;
        return true;
      }
      ranges.pollFirst();
    }
    return false;
  }

  /** current key, valid after {@link #advance} returns true */
  long key() {
    return key;
  }

  /** for sequential scan: whether index is in the range set, indexes must be ascending */
  boolean contains(long index) {
    while (!ranges.isEmpty() && ranges.peekFirst()[1] < index) {
      ranges.pollFirst();
    }
    return !ranges.isEmpty() && ranges.peekFirst()[0] <= index;
  }

  /** for sequential scan: no index after the given one is in the range set */
  boolean exhausted(long index) {
    while (!ranges.isEmpty() && ranges.peekFirst()[1] < index) {
      ranges.pollFirst();
    }
    return ranges.isEmpty();
  }
}
