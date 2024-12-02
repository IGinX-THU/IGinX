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
package cn.edu.tsinghua.iginx.filesystem.common;

import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.util.ArrayList;
import java.util.List;

public class Ranges {
  private Ranges() {}

  public static Range<Long> toClosedLongRange(Range<Long> range) {
    long lower = Long.MIN_VALUE;
    if (range.hasLowerBound()) {
      lower = range.lowerEndpoint();
      if (BoundType.OPEN.equals(range.lowerBoundType())) {
        lower++;
      }
    }
    long upper = Long.MAX_VALUE;
    if (range.hasUpperBound()) {
      upper = range.upperEndpoint();
      if (BoundType.OPEN.equals(range.upperBoundType())) {
        upper--;
      }
    }
    return Range.closed(lower, upper);
  }

  public static List<KeyRange> toKeyRanges(RangeSet<Long> rangeset) {
    List<KeyRange> keyRanges = new ArrayList<>();
    for (Range<Long> range : rangeset.asRanges()) {
      Range<Long> closedRange = toClosedLongRange(range);
      KeyRange keyRange =
          new KeyRange(closedRange.lowerEndpoint(), true, closedRange.upperEndpoint(), true);
      keyRanges.add(keyRange);
    }
    return keyRanges;
  }

  public static boolean notEmpty(Range<Long> closedRange) {
    return false;
  }
}
