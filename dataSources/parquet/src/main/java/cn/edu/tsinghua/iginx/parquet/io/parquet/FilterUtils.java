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
package cn.edu.tsinghua.iginx.parquet.io.parquet;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.OrFilter;
import cn.edu.tsinghua.iginx.parquet.util.Constants;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.Objects;
import javax.annotation.Nonnull;
import shaded.iginx.org.apache.parquet.filter2.predicate.FilterApi;
import shaded.iginx.org.apache.parquet.filter2.predicate.FilterPredicate;
import shaded.iginx.org.apache.parquet.filter2.predicate.Operators;

class FilterUtils {

  @Nonnull
  public static Pair<FilterPredicate, Boolean> toFilterPredicate(@Nonnull Filter filter) {
    switch (filter.getType()) {
      case Key:
        return toFilterPredicate((KeyFilter) filter);
      case And:
        return toFilterPredicate((AndFilter) filter);
      case Or:
        return toFilterPredicate((OrFilter) filter);
      default:
        return new Pair<>(null, true);
    }
  }

  private static Pair<FilterPredicate, Boolean> toFilterPredicate(KeyFilter filter) {
    Pair<FilterPredicate, Boolean> result = new Pair<>(null, null);
    switch (filter.getOp()) {
      case GE:
      case GE_AND:
        result.k = FilterApi.gtEq(getKeyColumn(), filter.getValue());
        break;
      case G:
      case G_AND:
        result.k = FilterApi.gt(getKeyColumn(), filter.getValue());
        break;
      case LE:
      case LE_AND:
        result.k = FilterApi.ltEq(getKeyColumn(), filter.getValue());
        break;
      case L:
      case L_AND:
        result.k = FilterApi.lt(getKeyColumn(), filter.getValue());
        break;
      case E:
      case E_AND:
        result.k = FilterApi.eq(getKeyColumn(), filter.getValue());
        break;
      case NE:
      case NE_AND:
        result.k = FilterApi.notEq(getKeyColumn(), filter.getValue());
        break;
      default:
        result.v = true;
    }
    return result;
  }

  private static Operators.LongColumn getKeyColumn() {
    return FilterApi.longColumn(Constants.KEY_FIELD_NAME);
  }

  private static Pair<FilterPredicate, Boolean> toFilterPredicate(AndFilter filter) {
    if (filter.getChildren().stream()
        .map(FilterUtils::toFilterPredicate)
        .map(Pair::getV)
        .filter(Objects::nonNull)
        .anyMatch(v -> !v)) {
      return new Pair<>(null, false);
    }
    return filter.getChildren().stream()
        .map(FilterUtils::toFilterPredicate)
        .map(Pair::getK)
        .filter(Objects::nonNull)
        .reduce(FilterApi::and)
        .map(filterPredicate -> new Pair<FilterPredicate, Boolean>(filterPredicate, null))
        .orElseGet(() -> new Pair<>(null, true));
  }

  private static Pair<FilterPredicate, Boolean> toFilterPredicate(OrFilter filter) {
    if (filter.getChildren().stream()
        .map(FilterUtils::toFilterPredicate)
        .map(Pair::getV)
        .filter(Objects::nonNull)
        .anyMatch(v -> v)) {
      return new Pair<>(null, true);
    }
    return filter.getChildren().stream()
        .map(FilterUtils::toFilterPredicate)
        .map(Pair::getK)
        .filter(Objects::nonNull)
        .reduce(FilterApi::and)
        .map(filterPredicate -> new Pair<FilterPredicate, Boolean>(filterPredicate, null))
        .orElseGet(() -> new Pair<>(null, false));
  }
}
