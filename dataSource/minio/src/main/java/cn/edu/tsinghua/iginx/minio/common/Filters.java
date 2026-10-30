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

import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.BoolFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.OrFilter;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

public final class Filters {

  private Filters() {}

  /** null means no filter */
  public static boolean isTrue(@Nullable Filter filter) {
    return filter == null
        || (filter.getType() == FilterType.Bool && ((BoolFilter) filter).isTrue());
  }

  public static boolean isFalse(@Nullable Filter filter) {
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

  /** null if no key range is given */
  @Nullable
  public static Filter toFilter(@Nullable List<KeyRange> keyRanges) {
    if (keyRanges == null || keyRanges.isEmpty()) {
      return null;
    }
    List<Filter> filters = new ArrayList<>();
    for (KeyRange range : keyRanges) {
      filters.add(
          new AndFilter(
              Arrays.asList(
                  new KeyFilter(range.isIncludeBeginKey() ? Op.GE : Op.G, range.getBeginKey()),
                  new KeyFilter(range.isIncludeEndKey() ? Op.LE : Op.L, range.getEndKey()))));
    }
    return new OrFilter(filters);
  }

  public static Filter nullableAnd(@Nullable Filter left, @Nullable Filter right) {
    if (isTrue(left) && isTrue(right)) {
      return new BoolFilter(true);
    } else if (isTrue(left)) {
      return right;
    } else if (isTrue(right)) {
      return left;
    }
    return new AndFilter(Arrays.asList(left, right));
  }
}
