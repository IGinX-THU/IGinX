package cn.edu.tsinghua.iginx.parquet.manager.data;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import com.google.common.collect.*;
import java.util.ArrayList;
import java.util.Arrays;
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

  public static Filter filterOf(Range<Long> range) {
    Filter lFilter = null;
    if (range.hasLowerBound()) {
      Op op = range.lowerBoundType() == BoundType.CLOSED ? Op.GE : Op.G;
      lFilter = new KeyFilter(op, range.lowerEndpoint());
    }

    Filter rFilter = null;
    if (range.hasUpperBound()) {
      Op op = range.upperBoundType() == BoundType.CLOSED ? Op.LE : Op.L;
      rFilter = new KeyFilter(op, range.upperEndpoint());
    }

    if (lFilter != null && rFilter != null) {
      if (range.isEmpty()) {
        return new BoolFilter(false);
      } else { // !range.isEmpty()
        return new AndFilter(Arrays.asList(lFilter, rFilter));
      }
    } else if (lFilter != null) { // rFilter == null
      return lFilter;
    } else if (rFilter != null) { // lFilter == null
      return rFilter;
    } else { // lFilter == null && rFilter == null
      return new BoolFilter(true);
    }
  }

  public static Filter filterOf(RangeSet<Long> rangeSet) {
    if (rangeSet.isEmpty()) {
      return new BoolFilter(false);
    } else if (rangeSet.encloses(Range.all())) {
      return new BoolFilter(true);
    } else if (rangeSet.asRanges().size() == 1) {
      return filterOf(rangeSet.asRanges().iterator().next());
    }

    List<Filter> subRangeFilters = new ArrayList<>();
    for (Range<Long> range : rangeSet.asRanges()) {
      Filter filter = filterOf(range);
      subRangeFilters.add(filter);
    }
    return new OrFilter(subRangeFilters);
  }
}
