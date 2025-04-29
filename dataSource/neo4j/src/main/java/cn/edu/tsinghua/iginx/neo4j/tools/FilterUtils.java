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
package cn.edu.tsinghua.iginx.neo4j.tools;

import static cn.edu.tsinghua.iginx.neo4j.tools.TagKVUtils.splitFullName;

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FilterUtils {

  private static final List<Pair<Long, Long>> EMPTY_RANGES = new ArrayList<>();

  private static final Pair<Long, Long> FULL_RANGE =
      new Pair<>((long) Integer.MIN_VALUE, Long.MAX_VALUE);

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
        return Collections.singletonList(new Pair<>((long) Integer.MIN_VALUE, value));
      case L:
      case L_AND:
        if (value == (long) Integer.MIN_VALUE) {
          return EMPTY_RANGES;
        } else {
          return Collections.singletonList(new Pair<>((long) Integer.MIN_VALUE, value - 1));
        }
      case E:
      case E_AND:
        return Collections.singletonList(new Pair<>(value, value));
      case NE:
      case NE_AND:
        if (value == (long) Integer.MIN_VALUE) {
          return Collections.singletonList(new Pair<>(value + 1, Long.MAX_VALUE));
        } else if (value == Long.MAX_VALUE) {
          return Collections.singletonList(new Pair<>((long) Integer.MIN_VALUE, value - 1));
        } else {
          return Arrays.asList(
              new Pair<>((long) Integer.MIN_VALUE, value - 1),
              new Pair<>(value + 1, Long.MAX_VALUE));
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

  public static boolean filterContainsType(List<FilterType> types, Filter filter) {
    boolean res = false;
    if (types.contains(filter.getType())) {
      return true;
    }
    switch (filter.getType()) {
      case And:
        List<Filter> andChildren = ((AndFilter) filter).getChildren();
        for (Filter child : andChildren) {
          res |= filterContainsType(types, child);
        }
        break;
      case Or:
        List<Filter> orChildren = ((OrFilter) filter).getChildren();
        for (Filter child : orChildren) {
          res |= filterContainsType(types, child);
        }
        break;
      case Not:
        Filter notChild = ((NotFilter) filter).getChild();
        res |= filterContainsType(types, notChild);
        break;
      default:
        break;
    }
    return res;
  }

  public static Filter expandFilter(Filter filter, String key) {
    switch (filter.getType()) {
      case And:
        List<Filter> andChildren = ((AndFilter) filter).getChildren();
        for (Filter child : andChildren) {
          Filter newFilter = expandFilter(child, key);
          andChildren.set(andChildren.indexOf(child), newFilter);
        }
        return new AndFilter(andChildren);
      case Or:
        List<Filter> orChildren = ((OrFilter) filter).getChildren();
        for (Filter child : orChildren) {
          Filter newFilter = expandFilter(child, key);
          orChildren.set(orChildren.indexOf(child), newFilter);
        }
        return new OrFilter(orChildren);
      case Not:
        Filter notChild = ((NotFilter) filter).getChild();
        Filter newFilter = expandFilter(notChild, key);
        return new NotFilter(newFilter);
      case In:
        InFilter inFilter = (InFilter) filter;
        String inPath = inFilter.getPath();
        if (inPath.contains("*")) {
          return new InFilter(key, inFilter.getInOp(), inFilter.getValues());
        }
        return filter;
      case Path:
      case Value:
      case Bool:
      case Key:
      default:
        break;
    }
    return filter;
  }

  public static Filter expandFilter(
      Filter filter, Map<String, Map<String, String>> labelToProperties) {
    List<List<String>> fullColumnNamesList = new ArrayList<>();
    for (Map.Entry<String, Map<String, String>> entry : labelToProperties.entrySet()) {
      List<String> fullColumnNames = new ArrayList<>();
      for (Map.Entry<String, String> property : entry.getValue().entrySet()) {
        fullColumnNames.add(entry.getKey() + "." + property.getKey());
      }
      fullColumnNamesList.add(fullColumnNames);
    }
    filter = expandFilter(filter, fullColumnNamesList);
    filter = LogicalFilterUtils.mergeTrue(filter);
    return filter;
  }

  private static Filter expandFilter(Filter filter, List<List<String>> columnNamesList) {
    switch (filter.getType()) {
      case And:
        List<Filter> andChildren = ((AndFilter) filter).getChildren();
        for (Filter child : andChildren) {
          Filter newFilter = expandFilter(child, columnNamesList);
          andChildren.set(andChildren.indexOf(child), newFilter);
        }
        return new AndFilter(andChildren);
      case Or:
        List<Filter> orChildren = ((OrFilter) filter).getChildren();
        for (Filter child : orChildren) {
          Filter newFilter = expandFilter(child, columnNamesList);
          orChildren.set(orChildren.indexOf(child), newFilter);
        }
        return new OrFilter(orChildren);
      case Not:
        Filter notChild = ((NotFilter) filter).getChild();
        Filter newFilter = expandFilter(notChild, columnNamesList);
        return new NotFilter(newFilter);
      case Value:
        ValueFilter valueFilter = ((ValueFilter) filter);
        String path = valueFilter.getPath();
        List<String> matchedPaths = getMatchedPath(path, columnNamesList);
        if (matchedPaths.isEmpty()) {
          return new BoolFilter(true);
        } else if (matchedPaths.size() == 1) {
          return new ValueFilter(matchedPaths.get(0), valueFilter.getOp(), valueFilter.getValue());
        } else {
          List<Filter> newFilters = new ArrayList<>();
          for (String matched : matchedPaths) {
            newFilters.add(new ValueFilter(matched, valueFilter.getOp(), valueFilter.getValue()));
          }
          if (Op.isOrOp(valueFilter.getOp())) {
            return new OrFilter(newFilters);
          } else {
            return new AndFilter(newFilters);
          }
        }
      case In:
        InFilter inFilter = (InFilter) filter;
        String inPath = inFilter.getPath();
        if (inPath.contains("*")) {
          List<String> matchedPath = getMatchedPath(inPath, columnNamesList);
          if (matchedPath.size() == 0) {
            return new BoolFilter(true);
          } else if (matchedPath.size() == 1) {
            return new InFilter(matchedPath.get(0), inFilter.getInOp(), inFilter.getValues());
          } else {
            List<Filter> inChildren = new ArrayList<>();
            for (String matched : matchedPath) {
              inChildren.add(new InFilter(matched, inFilter.getInOp(), inFilter.getValues()));
            }

            if (inFilter.getInOp().isOrOp()) {
              return new OrFilter(inChildren);
            }
            return new AndFilter(inChildren);
          }
        }

        return filter;
      case Path:
        String pathA = ((PathFilter) filter).getPathA();
        String pathB = ((PathFilter) filter).getPathB();
        if (pathA.contains("*")) {
          List<String> matchedPath = getMatchedPath(pathA, columnNamesList);
          if (matchedPath.size() == 0) {
            return new BoolFilter(true);
          } else if (matchedPath.size() == 1) {
            PathFilter newPathFilter =
                new PathFilter(
                    matchedPath.get(0),
                    ((PathFilter) filter).getOp(),
                    ((PathFilter) filter).getPathB());
            if (pathB.contains("*")) {
              return expandFilter(newPathFilter, columnNamesList);
            }
            return newPathFilter;
          } else {
            List<Filter> andPathChildren = new ArrayList<>();
            for (String matched : matchedPath) {
              andPathChildren.add(
                  new PathFilter(
                      matched, ((PathFilter) filter).getOp(), ((PathFilter) filter).getPathB()));
            }
            if (Op.isOrOp(((PathFilter) filter).getOp())) {
              filter = new OrFilter(andPathChildren);
            } else {
              filter = new AndFilter(andPathChildren);
            }
          }
        }

        if (pathB.contains("*")) {
          if (filter.getType() != FilterType.Path) {
            return expandFilter(filter, columnNamesList);
          }

          List<String> matchedPath = getMatchedPath(pathB, columnNamesList);
          if (matchedPath.size() == 0) {
            return new BoolFilter(true);
          } else if (matchedPath.size() == 1) {
            return new PathFilter(
                ((PathFilter) filter).getPathA(),
                ((PathFilter) filter).getOp(),
                matchedPath.get(0));
          } else {
            List<Filter> andPathChildren = new ArrayList<>();
            for (String matched : matchedPath) {
              andPathChildren.add(
                  new PathFilter(
                      ((PathFilter) filter).getPathA(), ((PathFilter) filter).getOp(), matched));
            }
            if (Op.isOrOp(((ValueFilter) filter).getOp())) {
              return new OrFilter(andPathChildren);
            }
            return new AndFilter(andPathChildren);
          }
        }

        return filter;

      case Bool:
      case Key:
      default:
        break;
    }
    return filter;
  }

  private static List<String> getMatchedPath(String path, List<List<String>> columnNamesList) {
    List<String> matchedPath = new ArrayList<>();
    path = StringUtils.reformatPath(path);
    Pattern pattern = Pattern.compile("^(unit.*\\.)?" + path + "$");
    for (List<String> columnNames : columnNamesList) {
      for (String columnName : columnNames) {
        Matcher matcher = pattern.matcher(splitFullName(columnName).k);
        if (matcher.find()) {
          matchedPath.add(columnName);
        }
      }
    }
    return matchedPath;
  }
}
