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
package cn.edu.tsinghua.iginx.mongodb.dummy;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.or;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

public class FilterUtils {
  public static final Filter EMTPY_FILTER = new AndFilter(new ArrayList<>());

  public static Filter removeSamePrefix(Filter filter, String prefix) {
    switch (filter.getType()) {
      case Value:
        return removeSamePrefix((ValueFilter) filter, prefix);
      case Path:
        return removeSamePrefix((PathFilter) filter, prefix);
      case And:
        return removeSamePrefix((AndFilter) filter, prefix);
      case Or:
        return removeSamePrefix((OrFilter) filter, prefix);
      case Not:
        return removeSamePrefix((NotFilter) filter, prefix);
      default:
        return filter;
    }
  }

  private static ValueFilter removeSamePrefix(ValueFilter filter, String prefix) {
    String path = filter.getPath();
    Op op = filter.getOp();
    Value value = filter.getValue();

    String suffix = NameUtils.getSuffix(path, prefix);
    return new ValueFilter(suffix, op, value);
  }

  private static PathFilter removeSamePrefix(PathFilter filter, String prefix) {
    Op op = filter.getOp();
    String pathA = filter.getPathA();
    String pathB = filter.getPathB();

    String suffixA = NameUtils.getSuffix(pathA, prefix);
    String suffixB = NameUtils.getSuffix(pathB, prefix);
    return new PathFilter(suffixA, op, suffixB);
  }

  private static AndFilter removeSamePrefix(AndFilter filter, String prefix) {
    List<Filter> children =
        filter.getChildren().stream()
            .map(f -> removeSamePrefix(f, prefix))
            .collect(Collectors.toList());
    return new AndFilter(children);
  }

  private static OrFilter removeSamePrefix(OrFilter filter, String prefix) {
    List<Filter> children =
        filter.getChildren().stream()
            .map(f -> removeSamePrefix(f, prefix))
            .collect(Collectors.toList());
    return new OrFilter(children);
  }

  private static NotFilter removeSamePrefix(NotFilter filter, String prefix) {
    Filter child = removeSamePrefix(filter.getChild(), prefix);
    return new NotFilter(child);
  }

  public static Bson toBson(Filter filter) {
    switch (filter.getType()) {
      case Value:
        return toBson((ValueFilter) filter);
      case Path:
        return toBson((PathFilter) filter);
      case Bool:
        return toBson((BoolFilter) filter);
      case And:
        return toBson((AndFilter) filter);
      case Or:
        return toBson((OrFilter) filter);
      default:
        throw new IllegalArgumentException("unsupported filter: " + filter);
    }
  }

  private static Bson toBson(ValueFilter filter) {
    String path = filter.getPath();
    checkPath(path);
    BsonValue value = TypeUtils.convert(filter.getValue());
    Bson filterBson =
        cn.edu.tsinghua.iginx.mongodb.tools.FilterUtils.fieldValueOp(filter.getOp(), path, value);
    if (value.isString()) {
      return filterBson;
    }

    String strValue = TypeUtils.toJson(value);
    Bson strFilter =
        cn.edu.tsinghua.iginx.mongodb.tools.FilterUtils.fieldValueOp(
            filter.getOp(), path, new BsonString(strValue));
    Bson symFilter =
        cn.edu.tsinghua.iginx.mongodb.tools.FilterUtils.fieldValueOp(
            filter.getOp(), path, new BsonSymbol(strValue));
    return or(filterBson, strFilter, symFilter);
  }

  private static Bson toBson(PathFilter filter) {
    String pathA = filter.getPathA();
    String pathB = filter.getPathB();
    checkPath(pathA);
    checkPath(pathB);

    return cn.edu.tsinghua.iginx.mongodb.tools.FilterUtils.fieldOp(filter.getOp(), pathA, pathB);
  }

  private static Bson toBson(BoolFilter filter) {
    if (filter.isTrue()) {
      return new Document();
    } else {
      throw new IllegalArgumentException("bool filter should be true");
    }
  }

  private static Bson toBson(AndFilter filter) {
    List<Bson> subFilterList =
        filter.getChildren().stream().map(FilterUtils::toBson).collect(Collectors.toList());
    if (subFilterList.isEmpty()) {
      throw new IllegalArgumentException("and filter should have at least one child");
    }
    return and(subFilterList);
  }

  private static Bson toBson(OrFilter filter) {
    List<Bson> subFilterList =
        filter.getChildren().stream().map(FilterUtils::toBson).collect(Collectors.toList());
    if (subFilterList.isEmpty()) {
      throw new IllegalArgumentException("or filter should have at least one child");
    }
    return or(subFilterList);
  }

  private static void checkPath(String path) {
    if (NameUtils.containNumberNode(path)) {
      throw new IllegalArgumentException("path contain number");
    }
  }
}
