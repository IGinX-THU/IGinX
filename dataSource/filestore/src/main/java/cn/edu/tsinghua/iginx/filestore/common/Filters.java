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

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

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
      return new AndFilter(Arrays.asList(new KeyFilter(startInclusive ? Op.GE : Op.G, start), new KeyFilter(endInclusive ? Op.LE : Op.L, end)));
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

  public static boolean isFalse(Filter filter) {
    return filter != null && filter.getType() == FilterType.Bool && !((BoolFilter) filter).isTrue();
  }

  public static Filter toFilter(KeyInterval keyInterval) {
    if (Objects.equals(keyInterval, KeyInterval.getDefaultKeyInterval())) {
      return new BoolFilter(true);
    }
    return new AndFilter(Arrays.asList(new KeyFilter(Op.GE, keyInterval.getStartKey()), new KeyFilter(Op.L, keyInterval.getEndKey())));
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
            return ImmutableRangeSet.<Long>builder().add(Range.lessThan(value)).add(Range.greaterThan(value)).build();
          default:
            throw new IllegalArgumentException("Unsupported operator: " + keyFilter.getOp());
        }
      }
      case And: {
        AndFilter andFilter = (AndFilter) filter;
        RangeSet<Long> rangeSet = TreeRangeSet.create();
        rangeSet.add(Range.all());
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

  public static boolean match(@Nullable Filter filter, Predicate<Filter> remain) {
    if (isTrue(filter)) {
      return true;
    }

    boolean[] result = new boolean[1];

    filter.accept(new FilterVisitor() {

      private void test(Filter filter) {
        if (!remain.test(filter)) {
          result[0] = false;
        }
      }

      @Override
      public void visit(KeyFilter filter) {
        test(filter);
      }

      @Override
      public void visit(ValueFilter filter) {
        test(filter);
      }

      @Override
      public void visit(PathFilter filter) {
        test(filter);
      }

      @Override
      public void visit(AndFilter filter) {
      }

      @Override
      public void visit(OrFilter filter) {
      }

      @Override
      public void visit(NotFilter filter) {
      }

      @Override
      public void visit(BoolFilter filter) {
        test(filter);
      }

      @Override
      public void visit(ExprFilter filter) {
        test(filter);
      }
    });

    return result[0];
  }

  @Nullable
  public static Filter superSet(@Nullable Filter filter, Predicate<Filter> remain) {
    return superSet(filter, (Filter f) -> {
      if (f == null) {
        return null;
      }
      if (remain.test(f)) {
        return f;
      } else {
        return null;
      }
    });
  }

  public static Filter superSet(@Nullable Filter filter, Function<Filter, Filter> transform) {
    if (isTrue(filter) || isFalse(filter)) {
      return transform.apply(filter);
    }

    switch (filter.getType()) {
      case Not:
      case And:
      case Or:
        filter = LogicalFilterUtils.toCNF(filter);
    }

    switch (filter.getType()) {
      case Not:
        throw new IllegalStateException("Not filter should be removed before calling superSet");
      case And: {
        AndFilter andFilter = (AndFilter) filter;
        List<Filter> children = new ArrayList<>();
        for (Filter child : andFilter.getChildren()) {
          Filter superSet = superSet(child, transform);
          if (!isTrue(superSet)) {
            children.add(superSet);
          }
        }
        if (children.isEmpty()) {
          return null;
        } else if (children.size() == 1) {
          return children.get(0);
        } else {
          return new AndFilter(children);
        }
      }
      case Or: {
        OrFilter orFilter = (OrFilter) filter;
        List<Filter> oldChildren = orFilter.getChildren();
        if (oldChildren.isEmpty()) {
          throw new IllegalStateException("Or filter should not have empty children");
        }
        List<Filter> children = new ArrayList<>();
        for (Filter child : orFilter.getChildren()) {
          Filter superSet = superSet(child, transform);
          if (!isTrue(superSet)) {
            children.add(superSet);
          }
        }
        if (children.isEmpty()) {
          return null;
        } else if (children.size() == 1) {
          return children.get(0);
        } else {
          return new OrFilter(children);
        }
      }
      default:
        return transform.apply(filter);
    }
  }

  public static Predicate<Filter> nonKeyFilter() {
    return filter -> filter.getType() == FilterType.Key;
  }

  public static Set<String> getPaths(Filter filter) {
    return LogicalFilterUtils.getPathsFromFilter(filter); // Recursive
  }

  public static Filter matchWildcard(@Nullable Filter filter, Set<String> fields) {
    return superSet(filter, (Filter f) -> {
      if (f == null) {
        return null;
      }
      switch (f.getType()) {
        case Key:
        case Bool:
          return f;
        case Path:
          // TODO: Implement this
        case Value:
          // TODO: Implement this
        default:
          return null;
      }
    });
  }

  public static Predicate<Filter> startWith(@Nullable String prefix) {
    return (Filter f) -> {
      if (f == null) {
        return true;
      }
      switch (f.getType()) {
        case Key:
        case Bool:
          return true;
        case Path: {
          PathFilter pathFilter = (PathFilter) f;
          String pathA = pathFilter.getPathA();
          String pathB = pathFilter.getPathB();
          if (Patterns.isWildcard(pathA) || Patterns.isWildcard(pathB)) {
            return false;
          }
          return Patterns.startsWith(pathA, prefix) || Patterns.startsWith(pathB, prefix);
        }
        case Value: {
          ValueFilter valueFilter = (ValueFilter) f;
          String path = valueFilter.getPath();
          if (Patterns.isWildcard(path)) {
            return false;
          }
          return Patterns.startsWith(path, prefix);
        }
        default:
          return false;
      }
    };
  }

  public static boolean equals(Filter filter, Filter superSetFilter) {
    // TODO: Optimize this
    return Objects.equals(filter, superSetFilter);
  }
}
