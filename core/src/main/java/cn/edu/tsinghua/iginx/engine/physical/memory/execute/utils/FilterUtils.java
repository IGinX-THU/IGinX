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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils;

import static cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op.isEqualOp;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;
import java.util.stream.Collectors;

public class FilterUtils {

  public static boolean validate(Filter filter, Row row) throws PhysicalException {
    switch (filter.getType()) {
      case Or:
        OrFilter orFilter = (OrFilter) filter;
        for (Filter childFilter : orFilter.getChildren()) {
          if (validate(childFilter, row)) { // 任何一个子条件满足，都直接返回
            return true;
          }
        }
        return false;
      case Bool:
        BoolFilter booleanFilter = (BoolFilter) filter;
        return booleanFilter.isTrue();
      case And:
        AndFilter andFilter = (AndFilter) filter;
        for (Filter childFilter : andFilter.getChildren()) {
          if (!validate(childFilter, row)) { // 任何一个子条件不满足，都直接返回
            return false;
          }
        }
        return true;
      case Not:
        NotFilter notFilter = (NotFilter) filter;
        return !validate(notFilter.getChild(), row);
      case Key:
        KeyFilter keyFilter = (KeyFilter) filter;
        if (row.getKey() == Row.NON_EXISTED_KEY) {
          return false;
        }
        return validateTimeFilter(keyFilter, row);
      case Value:
        ValueFilter valueFilter = (ValueFilter) filter;
        return validateValueFilter(valueFilter, row);
      case Path:
        PathFilter pathFilter = (PathFilter) filter;
        return validatePathFilter(pathFilter, row);
      case Expr:
        ExprFilter exprFilter = (ExprFilter) filter;
        return validateExprFilter(exprFilter, row);
      default:
        break;
    }
    return false;
  }

  private static boolean validateTimeFilter(KeyFilter keyFilter, Row row) {
    long timestamp = row.getKey();
    switch (keyFilter.getOp()) {
      case E:
      case E_AND:
        return timestamp == keyFilter.getValue();
      case G:
      case G_AND:
        return timestamp > keyFilter.getValue();
      case L:
      case L_AND:
        return timestamp < keyFilter.getValue();
      case GE:
      case GE_AND:
        return timestamp >= keyFilter.getValue();
      case LE:
      case LE_AND:
        return timestamp <= keyFilter.getValue();
      case NE:
      case NE_AND:
        return timestamp != keyFilter.getValue();
      case LIKE:
      case LIKE_AND: // TODO: case label. should we return false?
        break;
    }
    return false;
  }

  private static boolean validateValueFilter(ValueFilter valueFilter, Row row)
      throws PhysicalException {
    String path = valueFilter.getPath();
    Value targetValue = valueFilter.getValue();
    if (targetValue.isNull()) { // targetValue是空值，则认为不可比较
      return false;
    }

    if (path.contains("*")) { // 带通配符的filter
      List<Value> valueList = row.getAsValueByPattern(path);
      if (Op.isOrOp(valueFilter.getOp())) {
        for (Value value : valueList) {
          if (value == null || value.isNull()) {
            continue; // 任何一个value是空值，则认为不可比较
          }
          if (validateValueCompare(valueFilter.getOp(), value, targetValue)) {
            return true; // 任何一个子条件满足，都直接返回true
          }
        }
        return false; // 所有子条件均不满足，返回false
      } else if (Op.isAndOp(valueFilter.getOp())) {
        for (Value value : valueList) {
          if (value == null || value.isNull()) {
            return false; // 任何一个value是空值，则认为不可比较
          }
          if (!validateValueCompare(valueFilter.getOp(), value, targetValue)) {
            return false; // 任何一个子条件不满足，都直接返回false
          }
        }
        return true; // 所有子条件均满足，返回true
      } else {
        throw new RuntimeException("Unknown op type: " + valueFilter.getOp());
      }
    } else {
      Value value = row.getAsValue(path);
      if (value == null || value.isNull()) { // value是空值，则认为不可比较
        return false;
      }
      return validateValueCompare(valueFilter.getOp(), value, targetValue);
    }
  }

  private static boolean validatePathFilter(PathFilter pathFilter, Row row)
      throws PhysicalException {
    Value valueA = row.getAsValue(pathFilter.getPathA());
    Value valueB = row.getAsValue(pathFilter.getPathB());
    if (valueA == null
        || valueA.isNull()
        || valueB == null
        || valueB.isNull()) { // 如果任何一个是空值，则认为不可比较
      return false;
    }
    return validateValueCompare(pathFilter.getOp(), valueA, valueB);
  }

  private static boolean validateExprFilter(ExprFilter exprFilter, Row row)
      throws PhysicalException {
    Expression exprA = exprFilter.getExpressionA();
    Expression exprB = exprFilter.getExpressionB();

    Value valueA = ExprUtils.calculateExpr(row, exprA);
    Value valueB = ExprUtils.calculateExpr(row, exprB);

    if (valueA == null
        || valueA.isNull()
        || valueB == null
        || valueB.isNull()) { // 如果任何一个是空值，则认为不可比较
      return false;
    }
    return validateValueCompare(exprFilter.getOp(), valueA, valueB);
  }

  public static boolean validateValueCompare(Op op, Value valueA, Value valueB)
      throws PhysicalException {
    if (valueA.getDataType() != valueB.getDataType()) {
      if (ValueUtils.isNumericType(valueA) && ValueUtils.isNumericType(valueB)) {
        valueA = ValueUtils.transformToDouble(valueA);
        valueB = ValueUtils.transformToDouble(valueB);
      } else { // 数值类型和非数值类型无法比较
        return false;
      }
    }

    switch (op) {
      case E:
      case E_AND:
        return ValueUtils.compare(valueA, valueB) == 0;
      case G:
      case G_AND:
        return ValueUtils.compare(valueA, valueB) > 0;
      case L:
      case L_AND:
        return ValueUtils.compare(valueA, valueB) < 0;
      case GE:
      case GE_AND:
        return ValueUtils.compare(valueA, valueB) >= 0;
      case LE:
      case LE_AND:
        return ValueUtils.compare(valueA, valueB) <= 0;
      case NE:
      case NE_AND:
        return ValueUtils.compare(valueA, valueB) != 0;
      case LIKE:
      case LIKE_AND:
        return ValueUtils.regexCompare(valueA, valueB);
    }
    return false;
  }

  public static List<Pair<String, String>> getJoinColumnsFromFilter(Filter filter) {
    List<Pair<String, String>> l = new ArrayList<>();
    switch (filter.getType()) {
      case And:
        AndFilter andFilter = (AndFilter) filter;
        for (Filter childFilter : andFilter.getChildren()) {
          l.addAll(getJoinColumnsFromFilter(childFilter));
        }
        break;
      case Path:
        l.add(getJoinColumnFromPathFilter((PathFilter) filter));
        break;
      default:
        break;
    }
    return l;
  }

  public static Pair<String, String> getJoinColumnFromPathFilter(PathFilter pathFilter) {
    if (isEqualOp(pathFilter.getOp())) {
      return new Pair<>(pathFilter.getPathA(), pathFilter.getPathB());
    }
    return null;
  }

  public static Pair<String, String> getJoinPathFromFilter(
      Filter filter, Header headerA, Header headerB) throws PhysicalException {
    if (!filter.getType().equals(FilterType.Path)) {
      throw new InvalidOperatorParameterException(
          "Unsupported hash join filter type: " + filter.getType());
    }
    PathFilter pathFilter = (PathFilter) filter;
    if (!isEqualOp(pathFilter.getOp())) {
      throw new InvalidOperatorParameterException(
          "Unsupported hash join filter op type: " + pathFilter.getOp());
    }
    if (headerA.indexOf(pathFilter.getPathA()) != -1
        && headerB.indexOf(pathFilter.getPathB()) != -1) {
      return new Pair<>(pathFilter.getPathA(), pathFilter.getPathB());
    } else if (headerA.indexOf(pathFilter.getPathB()) != -1
        && headerB.indexOf(pathFilter.getPathA()) != -1) {
      return new Pair<>(pathFilter.getPathB(), pathFilter.getPathA());
    } else {
      throw new InvalidOperatorParameterException("invalid hash join path filter input.");
    }
  }

  public static List<String> getAllPathsFromFilter(Filter filter) {
    if (filter == null) {
      return new ArrayList<>();
    }
    Set<String> paths = new HashSet<>();
    switch (filter.getType()) {
      case And:
        AndFilter andFilter = (AndFilter) filter;
        andFilter.getChildren().forEach(child -> paths.addAll(getAllPathsFromFilter(child)));
        break;
      case Or:
        OrFilter orFilter = (OrFilter) filter;
        orFilter.getChildren().forEach(child -> paths.addAll(getAllPathsFromFilter(child)));
        break;
      case Value:
        ValueFilter valueFilter = (ValueFilter) filter;
        paths.add(valueFilter.getPath());
        break;
      case Path:
        PathFilter pathFilter = (PathFilter) filter;
        paths.add(pathFilter.getPathA());
        paths.add(pathFilter.getPathB());
      default:
        break;
    }
    return new ArrayList<>(paths);
  }

  public static boolean canUseHashJoin(Filter filter) {
    if (filter == null) {
      return false;
    }
    switch (filter.getType()) {
      case Or:
        OrFilter orFilter = (OrFilter) filter;
        if (orFilter.getChildren().size() == 1) {
          return canUseHashJoin(orFilter.getChildren().get(0));
        } else {
          return false;
        }
      case And:
        AndFilter andFilter = (AndFilter) filter;
        for (Filter child : andFilter.getChildren()) {
          if (canUseHashJoin(child)) {
            return true;
          }
        }
        return false;
      case Not:
        NotFilter notFilter = (NotFilter) filter;
        return canUseHashJoin(notFilter.getChild());
      case Path:
        PathFilter pathFilter = (PathFilter) filter;
        return isEqualOp(pathFilter.getOp());
      case Key:
      case Value:
      case Bool:
        return false;
      default:
        throw new RuntimeException("Unexpected filter type: " + filter.getType());
    }
  }

  public static Filter combineTwoFilter(Filter baseFilter, Filter additionFilter) {
    if (baseFilter == null) {
      return additionFilter;
    } else if (baseFilter.getType().equals(FilterType.Bool)) {
      BoolFilter boolFilter = (BoolFilter) baseFilter;
      if (boolFilter.isTrue()) {
        return additionFilter;
      } else {
        return baseFilter;
      }
    } else if (baseFilter.getType().equals(FilterType.And)) {
      AndFilter andFilter = (AndFilter) baseFilter;
      List<Filter> filterList = new ArrayList<>(andFilter.getChildren());
      if (additionFilter.getType().equals(FilterType.And)) {
        AndFilter additionAndFilter = (AndFilter) additionFilter;
        filterList.addAll(additionAndFilter.getChildren());
      } else {
        filterList.add(additionFilter);
      }
      return new AndFilter(filterList.stream().distinct().collect(Collectors.toList()));
    } else {
      List<Filter> filterList = new ArrayList<>(Collections.singletonList(baseFilter));
      filterList.add(additionFilter);
      return new AndFilter(filterList);
    }
  }

  public static List<PathFilter> getEqualPathFilter(Filter filter) {
    List<PathFilter> pathFilters = new ArrayList<>();
    filter.accept(
        new FilterVisitor() {
          @Override
          public void visit(AndFilter filter) {}

          @Override
          public void visit(OrFilter filter) {}

          @Override
          public void visit(NotFilter filter) {}

          @Override
          public void visit(KeyFilter filter) {}

          @Override
          public void visit(ValueFilter filter) {}

          @Override
          public void visit(PathFilter filter) {
            if (isEqualOp(filter.getOp())) {
              pathFilters.add(filter);
            }
          }

          @Override
          public void visit(BoolFilter filter) {}

          @Override
          public void visit(ExprFilter filter) {}
        });
    return pathFilters;
  }
}
