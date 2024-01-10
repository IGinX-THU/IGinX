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

package cn.edu.tsinghua.iginx.parquet.manager.data;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import com.google.common.collect.*;
import java.util.ArrayList;
import java.util.List;

class FilterRangeUtils {
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
        return ImmutableRangeSet.of(Range.closed(Long.MIN_VALUE, Long.MAX_VALUE));
    }
  }

  private static RangeSet<Long> rangeSetOf(KeyFilter filter) {
    RangeSet<Long> result = TreeRangeSet.create();
    switch (filter.getOp()) {
      case E:
      case E_AND:
        result.add(Range.singleton(filter.getValue()));
        break;
      case NE:
      case NE_AND:
        result.add(Range.all());
        result.remove(Range.singleton(filter.getValue()));
        break;
      case G:
      case G_AND:
        result.add(Range.greaterThan(filter.getValue()));
        break;
      case GE:
      case GE_AND:
        result.add(Range.atLeast(filter.getValue()));
        break;
      case L:
      case L_AND:
        result.add(Range.lessThan(filter.getValue()));
        break;
      case LE:
      case LE_AND:
        result.add(Range.atMost(filter.getValue()));
        break;
      default:
        throw new IllegalArgumentException("unsupported operator: " + filter.getOp());
    }
    return result;
  }

  private static RangeSet<Long> rangeSetOf(AndFilter filter) {
    RangeSet<Long> result = TreeRangeSet.create();
    result.add(Range.all());

    List<Filter> children = filter.getChildren();
    for (Filter child : children) {
      RangeSet<Long> childRanges = rangeSetOf(child);
      result.removeAll(childRanges.complement());
    }

    return result;
  }

  private static RangeSet<Long> rangeSetOf(OrFilter filter) {
    RangeSet<Long> result = TreeRangeSet.create();
    for (Filter child : filter.getChildren()) {
      result.addAll(rangeSetOf(child));
    }
    return result;
  }

  public static AndFilter filterOf(Range<Long> range) {
    List<Filter> filters = new ArrayList<>();
    if (range.hasLowerBound()) {
      Op op = range.lowerBoundType() == BoundType.CLOSED ? Op.GE : Op.G;
      filters.add(new KeyFilter(op, range.lowerEndpoint()));
    }
    if (range.hasUpperBound()) {
      Op op = range.upperBoundType() == BoundType.CLOSED ? Op.LE : Op.L;
      filters.add(new KeyFilter(op, range.upperEndpoint()));
    }
    return new AndFilter(filters);
  }
}
