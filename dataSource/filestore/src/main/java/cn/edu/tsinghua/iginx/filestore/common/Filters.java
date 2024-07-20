package cn.edu.tsinghua.iginx.filestore.common;

import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Filters {

  private Filters() {
  }

  public static Filter toFilter(List<KeyRange> keyRanges) {
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
      return new AndFilter(Arrays.asList(
          new KeyFilter(startInclusive ? Op.GE : Op.G, start),
          new KeyFilter(endInclusive ? Op.LE : Op.L, end)
      ));
    } else if (hasLowerBound) {
      return new KeyFilter(startInclusive ? Op.GE : Op.G, start);
    } else if (hasUpperBound) {
      return new KeyFilter(endInclusive ? Op.LE : Op.L, end);
    } else {
      return new BoolFilter(true);
    }
  }

  public static boolean isTrue(@Nullable Filter filter) {
    return filter == null || (filter.getType() == FilterType.Bool && ((BoolFilter) filter).isTrue());
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
}
