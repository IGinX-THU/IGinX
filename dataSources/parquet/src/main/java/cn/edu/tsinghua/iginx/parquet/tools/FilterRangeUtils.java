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

package cn.edu.tsinghua.iginx.parquet.tools;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.parquet.entity.Range;
import cn.edu.tsinghua.iginx.parquet.entity.RangeSet;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import javax.annotation.Nonnull;

public class FilterRangeUtils {
  private FilterRangeUtils() {}

  public static RangeSet<Long> rangeSetOf(Filter filter) {
    switch (filter.getType()) {
      case Key:
        return rangeSetOf((KeyFilter) filter);
      case And:
        return rangeSetOf((AndFilter) filter);
      case Or:
        return rangeSetOf((OrFilter) filter);
      case Value:
      case Path:
      case Bool:
      case Not:
      default:
        return new RangeSet<>(Long.MIN_VALUE, Long.MAX_VALUE);
    }
  }

  private static RangeSet<Long> rangeSetOf(KeyFilter filter) {
    RangeSet<Long> result = new RangeSet<>();
    Long leftNeighbor = getLeftNeighbor(filter.getValue());
    Long rightNeighbor = getRightNeighbor(filter.getValue());
    switch (filter.getOp()) {
      case E:
      case E_AND:
        result.add(new Range<>(filter.getValue(), filter.getValue()));
        break;
      case NE:
      case NE_AND:
        if (leftNeighbor != null) {
          result.add(new Range<>(Long.MIN_VALUE, leftNeighbor));
        }
        if (rightNeighbor != null) {
          result.add(new Range<>(rightNeighbor, Long.MAX_VALUE));
        }
        break;
      case G:
      case G_AND:
        if (rightNeighbor != null) {
          result.add(new Range<>(rightNeighbor, Long.MAX_VALUE));
        }
        break;
      case GE:
      case GE_AND:
        result.add(new Range<>(filter.getValue(), Long.MAX_VALUE));
        break;
      case L:
      case L_AND:
        if (leftNeighbor != null) {
          result.add(new Range<>(Long.MIN_VALUE, leftNeighbor));
        }
        break;
      case LE:
      case LE_AND:
        result.add(new Range<>(Long.MIN_VALUE, filter.getValue()));
        break;
      default:
        throw new IllegalArgumentException("unsupported operator: " + filter.getOp());
    }
    return result;
  }

  private static Long getLeftNeighbor(Long value) {
    if (value == Long.MIN_VALUE) {
      return null;
    }
    return value - 1;
  }

  private static Long getRightNeighbor(Long value) {
    if (value == Long.MAX_VALUE) {
      return null;
    }
    return value + 1;
  }

  private static RangeSet<Long> rangeSetOf(AndFilter filter) {
    List<Filter> children = filter.getChildren();
    List<Pair<Long, Long>> resultList = new ArrayList<>();
    resultList.add(new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
    for (Filter child : children) {
      List<Pair<Long, Long>> childRangesList = new ArrayList<>();
      RangeSet<Long> childRanges = rangeSetOf(child);
      for (Range<Long> childRange : childRanges) {
        childRangesList.add(new Pair<>(childRange.first(), childRange.last()));
      }
      resultList = intersectionSortedRanges(resultList, childRangesList);
    }
    RangeSet<Long> result = new RangeSet<>();
    for (Pair<Long, Long> range : resultList) {
      result.add(new Range<>(range.k, range.v));
    }
    return result;
  }

  private static List<Pair<Long, Long>> intersectionSortedRanges(
      List<Pair<Long, Long>> xRanges, List<Pair<Long, Long>> yRanges) {
    List<Pair<Long, Long>> result = new ArrayList<>();

    ListIterator<Pair<Long, Long>> xRangeItr = xRanges.listIterator();
    ListIterator<Pair<Long, Long>> yRangeItr = yRanges.listIterator();
    while (xRangeItr.hasNext() && yRangeItr.hasNext()) {
      Pair<Long, Long> xRange = xRangeItr.next();
      Pair<Long, Long> yRange = yRangeItr.next();
      if (xRange.v >= yRange.k && xRange.k <= yRange.v) {
        long rangeLeft = Long.max(xRange.k, yRange.k);
        long rangeRight = Long.min(xRange.v, yRange.v);
        result.add(new Pair<>(rangeLeft, rangeRight));
      }
      if (xRange.v < yRange.v) {
        yRangeItr.previous();
      } else {
        xRangeItr.previous();
      }
    }

    return result;
  }

  private static RangeSet<Long> rangeSetOf(OrFilter filter) {
    RangeSet<Long> result = new RangeSet<>();
    for (Filter child : filter.getChildren()) {
      result.addAll(rangeSetOf(child));
    }
    return result;
  }

  public static Filter filterOf(@Nonnull Range<Long> range) {
    if (range.first() == null && range.last() == null) {
      return new BoolFilter(true);
    }
    if (range.first() == null) {
      return new KeyFilter(Op.LE, range.last());
    }
    if (range.last() == null) {
      return new KeyFilter(Op.GE, range.first());
    }
    if (range.first().equals(range.last())) {
      return new KeyFilter(Op.E, range.first());
    }
    return new AndFilter(
        Arrays.asList(new KeyFilter(Op.GE, range.first()), new KeyFilter(Op.LE, range.last())));
  }
}
