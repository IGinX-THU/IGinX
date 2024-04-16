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

  
  public static Pair<FilterPredicate, Boolean> toFilterPredicate( Filter filter) {
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
