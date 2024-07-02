/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.parquet.manager.utils;

import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

public class RangeUtils {
  public static KeyInterval toKeyInterval(Range<Long> span) {
    if (span.isEmpty()) {
      return new KeyInterval(0, 0);
    }

    long begin = 0, end = Long.MAX_VALUE;
    if (span.hasLowerBound()) {
      begin = span.lowerEndpoint();
      if (span.lowerBoundType() == BoundType.OPEN) {
        begin = Math.max(begin, begin + 1);
      }
    }
    if (span.hasUpperBound()) {
      end = span.upperEndpoint();
      if (span.upperBoundType() == BoundType.CLOSED) {
        end = Math.max(end, end + 1);
      }
    }
    return new KeyInterval(begin, end);
  }
}
