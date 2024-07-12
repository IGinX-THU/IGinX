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
package cn.edu.tsinghua.iginx.redis.tools;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.BoolFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.OrFilter;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;

public class FilterUtils {

  private static final List<Pair<Long, Long>> EMPTY_RANGES = new ArrayList<>();

  private static final Pair<Long, Long> FULL_RANGE = new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE);

  private static final List<Pair<Long, Long>> FULL_RANGES = Collections.singletonList(FULL_RANGE);

  public static List<Pair<Long, Long>> keyRangesFrom(Filter filter) {
    if (filter == null) {
      return null;
    }
    List<Pair<Long, Long>> result = analyzeKeyRanges(filter);
    if (result.equals(FULL_RANGES)) {
      return null;
    }
    return result;
  }

  private static List<Pair<Long, Long>> analyzeKeyRanges(Filter filter) {
    switch (filter.getType()) {
      case Key:
        return analyzeKeyRanges((KeyFilter) filter);
      case Bool:
        return analyzeKeyRanges((BoolFilter) filter);
      case And:
        return analyzeKeyRanges((AndFilter) filter);
      case Or:
        return analyzeKeyRanges((OrFilter) filter);
      default:
        return FULL_RANGES;
    }
  }

  private static List<Pair<Long, Long>> analyzeKeyRanges(KeyFilter filter) {
    long value = filter.getValue();
    switch (filter.getOp()) {
      case GE:
      case GE_AND:
        return Collections.singletonList(new Pair<>(value, Long.MAX_VALUE));
      case G:
      case G_AND:
        if (value == Long.MAX_VALUE) {
          return EMPTY_RANGES;
        } else {
          return Collections.singletonList(new Pair<>(value + 1, Long.MAX_VALUE));
        }
      case LE:
      case LE_AND:
        return Collections.singletonList(new Pair<>(Long.MIN_VALUE, value));
      case L:
      case L_AND:
        if (value == Long.MIN_VALUE) {
          return EMPTY_RANGES;
        } else {
          return Collections.singletonList(new Pair<>(Long.MIN_VALUE, value - 1));
        }
      case E:
      case E_AND:
        return Collections.singletonList(new Pair<>(value, value));
      case NE:
      case NE_AND:
        if (value == Long.MIN_VALUE) {
          return Collections.singletonList(new Pair<>(value + 1, Long.MAX_VALUE));
        } else if (value == Long.MAX_VALUE) {
          return Collections.singletonList(new Pair<>(Long.MIN_VALUE, value - 1));
        } else {
          return Arrays.asList(
              new Pair<>(Long.MIN_VALUE, value - 1), new Pair<>(value, Long.MAX_VALUE));
        }
      default:
        return FULL_RANGES;
    }
  }

  private static List<Pair<Long, Long>> analyzeKeyRanges(BoolFilter filter) {
    if (filter.isTrue()) {
      return FULL_RANGES;
    } else {
      return EMPTY_RANGES;
    }
  }

  private static List<Pair<Long, Long>> analyzeKeyRanges(AndFilter filter) {
    List<Filter> children = filter.getChildren();
    List<Pair<Long, Long>> result = FULL_RANGES;
    for (Filter child : children) {
      List<Pair<Long, Long>> childRanges = analyzeKeyRanges(child);
      result = intersectionSortedRanges(result, childRanges);
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

  private static List<Pair<Long, Long>> analyzeKeyRanges(OrFilter filter) {
    List<Filter> children = filter.getChildren();
    List<List<Pair<Long, Long>>> childrenRanges = new ArrayList<>(children.size());
    for (Filter child : children) {
      childrenRanges.add(analyzeKeyRanges(child));
    }
    return unionSortedRanges(childrenRanges);
  }

  private static List<Pair<Long, Long>> unionSortedRanges(List<List<Pair<Long, Long>>> rangesList) {
    List<Pair<Long, Long>> allRanges = new ArrayList<>();
    for (List<Pair<Long, Long>> ranges : rangesList) {
      allRanges.addAll(ranges);
    }
    allRanges.sort(Comparator.comparing(Pair<Long, Long>::getK));

    List<Pair<Long, Long>> result = new ArrayList<>();
    Pair<Long, Long> lastRange = null;
    for (Pair<Long, Long> currRange : allRanges) {
      if (lastRange != null) {
        if (currRange.k <= lastRange.v) {
          lastRange.v = Long.max(currRange.v, lastRange.v);
          continue;
        } else {
          result.add(lastRange);
        }
      }
      lastRange = new Pair<>(currRange.k, currRange.v);
    }

    if (lastRange != null) {
      result.add(lastRange);
    }
    return result;
  }
}
