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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.filestore.common;

import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Filters {

  private Filters() {
  }

  public static Filter toFilter(@Nullable List<KeyRange> keyRanges) {
    if (keyRanges == null) {
      return new BoolFilter(true);
    }
    List<Filter> rangeFilters = new ArrayList<>();
    for (KeyRange range : keyRanges) {
      Filter rangeFilter = toFilter(range);
      if (!isTrue(rangeFilter)) {
        rangeFilters.add(rangeFilter);
      }
    }
    return new OrFilter(rangeFilters);
  }

  public static Filter toFilter(KeyRange keyRanges) {
    long start = keyRanges.getBeginKey();
    boolean startInclusive = keyRanges.isIncludeBeginKey();
    long end = keyRanges.getEndKey();
    boolean endInclusive = keyRanges.isIncludeEndKey();

    boolean hasLowerBound = !(start == Long.MIN_VALUE && startInclusive);
    boolean hasUpperBound = !(end == Long.MAX_VALUE && endInclusive);

    if (hasLowerBound && hasUpperBound) {
      return new AndFilter(
          Arrays.asList(
              new KeyFilter(startInclusive ? Op.GE : Op.G, start),
              new KeyFilter(endInclusive ? Op.LE : Op.L, end)));
    } else if (hasLowerBound) {
      return new KeyFilter(startInclusive ? Op.GE : Op.G, start);
    } else if (hasUpperBound) {
      return new KeyFilter(endInclusive ? Op.LE : Op.L, end);
    } else {
      return new BoolFilter(true);
    }
  }

  public static boolean isTrue(@Nullable Filter filter) {
    return filter == null
        || (filter.getType() == FilterType.Bool && ((BoolFilter) filter).isTrue());
  }

  public static boolean isFalse(Filter filter) {
    return filter != null && filter.getType() == FilterType.Bool && !((BoolFilter) filter).isTrue();
  }

  public static Filter toFilter(KeyInterval keyInterval) {
    if (Objects.equals(keyInterval, KeyInterval.getDefaultKeyInterval())) {
      return new BoolFilter(true);
    }
    return new AndFilter(
        Arrays.asList(
            new KeyFilter(Op.GE, keyInterval.getStartKey()),
            new KeyFilter(Op.L, keyInterval.getEndKey())));
  }

  public static Filter nullableAnd(@Nullable Filter rangeFilter, @Nullable Filter selectFilter) {
    boolean rangeFilterIsTrue = isTrue(rangeFilter);
    boolean selectFilterIsTrue = isTrue(selectFilter);
    if (rangeFilterIsTrue && selectFilterIsTrue) {
      return new BoolFilter(true);
    } else if (rangeFilterIsTrue) {
      return selectFilter;
    } else if (selectFilterIsTrue) {
      return rangeFilter;
    } else {
      return new AndFilter(Arrays.asList(rangeFilter, selectFilter));
    }
  }

  public static RangeSet<Long> toRangeSet(@Nullable Filter filter) {
    if (isTrue(filter)) {
      return ImmutableRangeSet.of(Range.all());
    }
    switch (filter.getType()) {
      case Key: {
        KeyFilter keyFilter = (KeyFilter) filter;
        long value = keyFilter.getValue();
        switch (keyFilter.getOp()) {
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
            throw new IllegalArgumentException("Unsupported operator: " + keyFilter.getOp());
        }
      }
      case And: {
        AndFilter andFilter = (AndFilter) filter;
        RangeSet<Long> rangeSet = TreeRangeSet.create();
        for (Filter child : andFilter.getChildren()) {
          rangeSet.removeAll(toRangeSet(child).complement());
        }
        return rangeSet;
      }
      case Or: {
        RangeSet<Long> rangeSet = TreeRangeSet.create();
        OrFilter orFilter = (OrFilter) filter;
        for (Filter child : orFilter.getChildren()) {
          rangeSet.addAll(toRangeSet(child));
        }
        return rangeSet;
      }
      default:
        throw new IllegalArgumentException("Unsupported filter type: " + filter.getType());
    }
  }


}
