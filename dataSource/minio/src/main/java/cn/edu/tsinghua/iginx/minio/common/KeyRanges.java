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
package cn.edu.tsinghua.iginx.minio.common;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import javax.annotation.Nullable;

/** Extracts a superset of keys that may satisfy a filter. */
public final class KeyRanges {

  private KeyRanges() {}

  public static RangeSet<Long> of(@Nullable Filter filter) {
    if (filter == null) {
      return ImmutableRangeSet.of(Range.all());
    }
    switch (filter.getType()) {
      case Bool:
        return ((BoolFilter) filter).isTrue()
            ? ImmutableRangeSet.of(Range.all())
            : ImmutableRangeSet.of();
      case Key:
        return of((KeyFilter) filter);
      case And:
        {
          RangeSet<Long> result = TreeRangeSet.create();
          result.add(Range.all());
          for (Filter child : ((AndFilter) filter).getChildren()) {
            result.removeAll(of(child).complement());
          }
          return result;
        }
      case Or:
        {
          RangeSet<Long> result = TreeRangeSet.create();
          for (Filter child : ((OrFilter) filter).getChildren()) {
            result.addAll(of(child));
          }
          return result;
        }
      default:
        return ImmutableRangeSet.of(Range.all());
    }
  }

  private static RangeSet<Long> of(KeyFilter filter) {
    long value = filter.getValue();
    switch (filter.getOp()) {
      case G:
        return ImmutableRangeSet.of(Range.greaterThan(value));
      case GE:
        return ImmutableRangeSet.of(Range.atLeast(value));
      case L:
        return ImmutableRangeSet.of(Range.lessThan(value));
      case LE:
        return ImmutableRangeSet.of(Range.atMost(value));
      case E:
        return ImmutableRangeSet.of(Range.singleton(value));
      case NE:
        return ImmutableRangeSet.<Long>builder()
            .add(Range.lessThan(value))
            .add(Range.greaterThan(value))
            .build();
      default:
        return ImmutableRangeSet.of(Range.all());
    }
  }

  public static boolean intersects(RangeSet<Long> ranges, long min, long max) {
    return !ranges.subRangeSet(Range.closed(min, max)).isEmpty();
  }
}
