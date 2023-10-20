package cn.edu.tsinghua.iginx.engine.logical.utils;

import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExprUtils {

  public static Filter toDNF(Filter filter) {
    filter = removeNot(filter);
    filter = removeSingleFilter(filter);
    FilterType type = filter.getType();
    switch (type) {
      case Key:
      case Value:
      case Path:
        return filter;
      case Not:
        throw new SQLParserException("Get DNF failed, filter has not-subFilter.");
      case And:
        return toDNF((AndFilter) filter);
      case Or:
        return toDNF((OrFilter) filter);
      default:
        throw new SQLParserException("Get DNF failed, token type is: " + filter.getType());
    }
  }

  private static Filter toDNF(AndFilter andFilter) {
    List<Filter> children = andFilter.getChildren();
    List<Filter> dnfChildren = new ArrayList<>();
    children.forEach(child -> dnfChildren.add(toDNF(child)));

    boolean childrenWithoutOr = true;
    for (Filter child : dnfChildren) {
      if (child.getType().equals(FilterType.Or)) {
        childrenWithoutOr = false;
        break;
      }
    }

    List<Filter> newChildren = new ArrayList<>();
    if (childrenWithoutOr) {
      dnfChildren.forEach(
          child -> {
            if (FilterType.isLeafFilter(child.getType())) {
              newChildren.add(child);
            } else {
              newChildren.addAll(((AndFilter) child).getChildren());
            }
          });
      return new AndFilter(newChildren);
    } else {
      newChildren.addAll(getConjunctions(dnfChildren));
      return new OrFilter(newChildren);
    }
  }

  private static Filter toDNF(OrFilter orFilter) {
    List<Filter> children = orFilter.getChildren();
    List<Filter> newChildren = new ArrayList<>();
    children.forEach(
        child -> {
          Filter newChild = toDNF(child);
          if (FilterType.isLeafFilter(newChild.getType())
              || newChild.getType().equals(FilterType.And)) {
            newChildren.add(newChild);
          } else {
            newChildren.addAll(((OrFilter) newChild).getChildren());
          }
        });
    return new OrFilter(newChildren);
  }

  private static List<Filter> getConjunctions(List<Filter> filters) {
    List<Filter> cur = getAndChild(filters.get(0));
    for (int i = 1; i < filters.size(); i++) {
      cur = getConjunctions(cur, getAndChild(filters.get(i)));
    }
    return cur;
  }

  private static List<Filter> getConjunctions(List<Filter> first, List<Filter> second) {
    List<Filter> ret = new ArrayList<>();
    for (Filter firstFilter : first) {
      for (Filter secondFilter : second) {
        ret.add(
            mergeToConjunction(
                new ArrayList<>(Arrays.asList(firstFilter.copy(), secondFilter.copy()))));
      }
    }
    return ret;
  }

  private static Filter mergeToConjunction(List<Filter> filters) {
    List<Filter> children = new ArrayList<>();
    filters.forEach(
        child -> {
          if (FilterType.isLeafFilter(child.getType())) {
            children.add(child);
          } else {
            children.addAll(((AndFilter) child).getChildren());
          }
        });
    return new AndFilter(children);
  }

  private static List<Filter> getAndChild(Filter filter) {
    if (filter.getType().equals(FilterType.Or)) {
      return ((OrFilter) filter).getChildren();
    } else {
      return Collections.singletonList(filter);
    }
  }

  public static Filter toCNF(Filter filter) {
    filter = removeNot(filter);
    filter = removeSingleFilter(filter);
    FilterType type = filter.getType();
    switch (type) {
      case Key:
      case Value:
      case Path:
        return filter;
      case Not:
        throw new SQLParserException("Get CNF failed, filter has not-subFilter.");
      case And:
        return toCNF((AndFilter) filter);
      case Or:
        return toCNF((OrFilter) filter);
      default:
        throw new SQLParserException("Get CNF failed, token type is: " + filter.getType());
    }
  }

  private static Filter toCNF(AndFilter andFilter) {
    List<Filter> children = andFilter.getChildren();
    List<Filter> newChildren = new ArrayList<>();
    children.forEach(
        child -> {
          Filter newChild = toDNF(child);
          if (FilterType.isLeafFilter(newChild.getType())
              || newChild.getType().equals(FilterType.Or)) {
            newChildren.add(newChild);
          } else {
            newChildren.addAll(((AndFilter) newChild).getChildren());
          }
        });
    return new AndFilter(newChildren);
  }

  private static Filter toCNF(OrFilter orFilter) {
    List<Filter> children = orFilter.getChildren();
    List<Filter> cnfChildren = new ArrayList<>();
    children.forEach(child -> cnfChildren.add(toCNF(child)));

    boolean childrenWithoutAnd = true;
    for (Filter child : cnfChildren) {
      if (child.getType().equals(FilterType.And)) {
        childrenWithoutAnd = false;
        break;
      }
    }

    List<Filter> newChildren = new ArrayList<>();
    if (childrenWithoutAnd) {
      cnfChildren.forEach(
          child -> {
            if (FilterType.isLeafFilter(child.getType())) {
              newChildren.add(child);
            } else {
              newChildren.addAll(((AndFilter) child).getChildren());
            }
          });
      return new OrFilter(newChildren);
    } else {
      newChildren.addAll(getDisjunctions(cnfChildren));
      return new AndFilter(newChildren);
    }
  }

  private static List<Filter> getDisjunctions(List<Filter> filters) {
    List<Filter> cur = getOrChild(filters.get(0));
    for (int i = 1; i < filters.size(); i++) {
      cur = getDisjunctions(cur, getOrChild(filters.get(i)));
    }
    return cur;
  }

  private static List<Filter> getDisjunctions(List<Filter> first, List<Filter> second) {
    List<Filter> ret = new ArrayList<>();
    for (Filter firstFilter : first) {
      for (Filter secondFilter : second) {
        ret.add(
            mergeToDisjunction(
                new ArrayList<>(Arrays.asList(firstFilter.copy(), secondFilter.copy()))));
      }
    }
    return ret;
  }

  private static Filter mergeToDisjunction(List<Filter> filters) {
    List<Filter> children = new ArrayList<>();
    filters.forEach(
        child -> {
          if (FilterType.isLeafFilter(child.getType())) {
            children.add(child);
          } else {
            children.addAll(((OrFilter) child).getChildren());
          }
        });
    return new OrFilter(children);
  }

  private static List<Filter> getOrChild(Filter filter) {
    if (filter.getType().equals(FilterType.And)) {
      return ((AndFilter) filter).getChildren();
    } else {
      return Collections.singletonList(filter);
    }
  }

  public static Filter removeSingleFilter(Filter filter) {
    if (filter.getType().equals(FilterType.Or)) {
      List<Filter> children = ((OrFilter) filter).getChildren();
      for (int i = 0; i < children.size(); i++) {
        Filter childWithoutSingle = removeSingleFilter(children.get(i));
        children.set(i, childWithoutSingle);
      }
      return children.size() == 1 ? children.get(0) : filter;
    } else if (filter.getType().equals(FilterType.And)) {
      List<Filter> children = ((AndFilter) filter).getChildren();
      for (int i = 0; i < children.size(); i++) {
        Filter childWithoutSingle = removeSingleFilter(children.get(i));
        children.set(i, childWithoutSingle);
      }
      return children.size() == 1 ? children.get(0) : filter;
    } else if (filter.getType().equals(FilterType.Not)) {
      NotFilter notFilter = (NotFilter) filter;
      notFilter.setChild(removeSingleFilter(notFilter.getChild()));
      return filter;
    }
    return filter;
  }

  public static Filter removeNot(Filter filter) {
    FilterType type = filter.getType();
    switch (type) {
      case Key:
      case Value:
      case Path:
        return filter;
      case And:
        return removeNot((AndFilter) filter);
      case Or:
        return removeNot((OrFilter) filter);
      case Not:
        return removeNot((NotFilter) filter);
      default:
        throw new SQLParserException(String.format("Unknown token [%s] in reverse filter.", type));
    }
  }

  private static Filter removeNot(AndFilter andFilter) {
    List<Filter> andChildren = andFilter.getChildren();
    for (int i = 0; i < andChildren.size(); i++) {
      Filter childWithoutNot = removeNot(andChildren.get(i));
      andChildren.set(i, childWithoutNot);
    }
    return andFilter;
  }

  private static Filter removeNot(OrFilter orFilter) {
    List<Filter> orChildren = orFilter.getChildren();
    for (int i = 0; i < orChildren.size(); i++) {
      Filter childWithoutNot = removeNot(orChildren.get(i));
      orChildren.set(i, childWithoutNot);
    }
    return orFilter;
  }

  private static Filter removeNot(NotFilter notFilter) {
    return reverseFilter(notFilter.getChild());
  }

  private static Filter reverseFilter(Filter filter) {
    if (filter == null) return null;

    FilterType type = filter.getType();
    switch (filter.getType()) {
      case Key:
        ((KeyFilter) filter).reverseFunc();
        return filter;
      case Value:
        ((ValueFilter) filter).reverseFunc();
        return filter;
      case Path:
        ((PathFilter) filter).reverseFunc();
        return filter;
      case And:
        List<Filter> andChildren = ((AndFilter) filter).getChildren();
        for (int i = 0; i < andChildren.size(); i++) {
          Filter childWithoutNot = reverseFilter(andChildren.get(i));
          andChildren.set(i, childWithoutNot);
        }
        return new OrFilter(andChildren);
      case Or:
        List<Filter> orChildren = ((OrFilter) filter).getChildren();
        for (int i = 0; i < orChildren.size(); i++) {
          Filter childWithoutNot = reverseFilter(orChildren.get(i));
          orChildren.set(i, childWithoutNot);
        }
        return new AndFilter(orChildren);
      case Not:
        return removeNot(((NotFilter) filter).getChild());
      default:
        throw new SQLParserException(String.format("Unknown token [%s] in reverse filter.", type));
    }
  }

  public static List<KeyRange> getKeyRangesFromFilter(Filter filter) {
    filter = toDNF(filter);
    List<KeyRange> keyRanges = new ArrayList<>();
    extractKeyRange(keyRanges, filter);
    return unionKeyRanges(keyRanges);
  }

  private static void extractKeyRange(List<KeyRange> keyRanges, Filter f) {
    FilterType type = f.getType();
    switch (type) {
      case Value:
      case Path:
        break;
      case Key:
        keyRanges.add(getKeyRangesFromKeyFilter((KeyFilter) f));
        break;
      case And:
        KeyRange range = getKeyRangeFromAndFilter((AndFilter) f);
        if (range != null) keyRanges.add(range);
        break;
      case Or:
        List<KeyRange> ranges = getKeyRangeFromOrFilter((OrFilter) f);
        if (!ranges.isEmpty()) {
          keyRanges.addAll(ranges);
        }
        break;
      default:
        throw new SQLParserException(String.format("Illegal token [%s] in extractKeyRange.", type));
    }
  }

  private static List<KeyRange> getKeyRangeFromOrFilter(OrFilter filter) {
    List<KeyRange> keyRanges = new ArrayList<>();
    filter.getChildren().forEach(f -> extractKeyRange(keyRanges, f));
    return unionKeyRanges(keyRanges);
  }

  private static KeyRange getKeyRangeFromAndFilter(AndFilter filter) {
    List<KeyRange> keyRanges = new ArrayList<>();
    filter.getChildren().forEach(f -> extractKeyRange(keyRanges, f));
    return intersectKeyRanges(keyRanges);
  }

  private static KeyRange getKeyRangesFromKeyFilter(KeyFilter filter) {
    switch (filter.getOp()) {
      case L:
      case L_AND:
        return new KeyRange(0, filter.getValue());
      case LE:
      case LE_AND:
        return new KeyRange(0, filter.getValue() + 1);
      case G:
      case G_AND:
        return new KeyRange(filter.getValue() + 1, Long.MAX_VALUE);
      case GE:
      case GE_AND:
        return new KeyRange(filter.getValue(), Long.MAX_VALUE);
      case E:
      case E_AND:
        return new KeyRange(filter.getValue(), filter.getValue() + 1);
      case NE:
      case NE_AND:
        throw new SQLParserException("Not support [!=] in delete clause.");
      default:
        throw new SQLParserException(
            String.format("Unknown op [%s] in getKeyRangesFromKeyFilter.", filter.getOp()));
    }
  }

  private static List<KeyRange> unionKeyRanges(List<KeyRange> keyRanges) {
    if (keyRanges == null || keyRanges.isEmpty()) return new ArrayList<>();
    keyRanges.sort(
        (tr1, tr2) -> {
          long diff = tr1.getBeginKey() - tr2.getBeginKey();
          return diff == 0 ? 0 : diff > 0 ? 1 : -1;
        });

    List<KeyRange> res = new ArrayList<>();

    KeyRange cur = keyRanges.get(0);
    for (int i = 1; i < keyRanges.size(); i++) {
      KeyRange union = unionTwoKeyRanges(cur, keyRanges.get(i));
      if (union == null) {
        res.add(cur);
        cur = keyRanges.get(i);
      } else {
        cur = union;
      }
    }
    res.add(cur);
    return res;
  }

  private static KeyRange unionTwoKeyRanges(KeyRange first, KeyRange second) {
    if (first.getEndKey() < second.getBeginKey() || first.getBeginKey() > second.getEndKey()) {
      return null;
    }
    long begin = Math.min(first.getBeginKey(), second.getBeginKey());
    long end = Math.max(first.getEndKey(), second.getEndKey());
    return new KeyRange(begin, end);
  }

  private static KeyRange intersectKeyRanges(List<KeyRange> keyRanges) {
    if (keyRanges == null || keyRanges.isEmpty()) return null;
    KeyRange ret = keyRanges.get(0);
    for (int i = 1; i < keyRanges.size(); i++) {
      ret = intersectTwoKeyRanges(ret, keyRanges.get(i));
    }
    return ret;
  }

  private static KeyRange intersectTwoKeyRanges(KeyRange first, KeyRange second) {
    if (first == null || second == null) return null;
    if (first.getEndKey() < second.getBeginKey() || first.getBeginKey() > second.getEndKey())
      return null;

    long begin = Math.max(first.getBeginKey(), second.getBeginKey());
    long end = Math.min(first.getEndKey(), second.getEndKey());
    return new KeyRange(begin, end);
  }

  public static Filter getSubFilterFromFragment(Filter filter, ColumnsInterval columnsInterval) {
    Filter filterWithoutNot = removeNot(filter);
    Filter filterWithTrue = setTrue(filterWithoutNot, columnsInterval);
    return mergeTrue(filterWithTrue);
  }

  /** 判断filter的path是否是聚合函数或UDF函数 */
  private static boolean isFunction(String path) {
    String pattern =
        "[^!&()+=|'%`;,<>?\\t\\n\u2E80\u2E81\u2E82\u2E83\u2E84\u2E85\\x00-\\x1F\\x7F\\[\\]\\-*\\s]\\(.+\\)";
    Matcher matcher = Pattern.compile(pattern).matcher(path);
    return matcher.find();
  }

  /** 判断filter的path是否在当前的ColumnsRange中 */
  private static boolean columnRangeContainPath(ColumnsInterval interval, String path) {
    if ((interval.getStartColumn() == null || interval.getStartColumn().compareTo(path) <= 0)
        && (interval.getEndColumn() == null || interval.getEndColumn().compareTo(path) > 0)) {
      return true;
    } else if (!path.contains("*")) {
      return false;
    }

    // 对带*通配符的path进行处理
    String pathPrefix = path.substring(0, path.indexOf("*"));
    String startColumnPrefix = interval.getStartColumn() == null ? "" : interval.getStartColumn();
    startColumnPrefix =
        startColumnPrefix.length() <= pathPrefix.length()
            ? startColumnPrefix
            : startColumnPrefix.substring(0, pathPrefix.length());
    String endColumnPrefix = interval.getEndColumn() == null ? "" : interval.getEndColumn();
    endColumnPrefix =
        endColumnPrefix.length() <= pathPrefix.length()
            ? endColumnPrefix
            : endColumnPrefix.substring(0, pathPrefix.length());
    if ((startColumnPrefix.isEmpty() || startColumnPrefix.compareTo(pathPrefix) <= 0)
        && (endColumnPrefix.isEmpty() || endColumnPrefix.compareTo(pathPrefix) > 0)) {
      return true;
    }

    String endColumnSuffix = interval.getEndColumn() == null ? "" : interval.getEndColumn();
    endColumnSuffix =
        endColumnSuffix.length() <= pathPrefix.length()
            ? ""
            : endColumnSuffix.substring(pathPrefix.length());
    boolean endColumnSuffixIsAllSharp = endColumnSuffix.matches("^#+$");

    String startColumn = interval.getStartColumn() == null ? "" : interval.getStartColumn();
    String endColumn = interval.getEndColumn() == null ? "" : interval.getEndColumn();
    String endColumnCutStartColumn =
        endColumn.length() <= startColumn.length() ? "" : endColumn.substring(startColumn.length());
    boolean endColumnCutStartColumnIsAllSharp = endColumnCutStartColumn.matches("^#+$");
    if (!endColumnSuffix.isEmpty()
        && endColumnPrefix.compareTo(pathPrefix) == 0
        && !endColumnSuffixIsAllSharp
        && (endColumn.startsWith(startColumn) || !endColumnCutStartColumnIsAllSharp)) {
      return true;
    }

    return false;
  }

  private static Filter setTrue(Filter filter, ColumnsInterval columnsInterval) {
    switch (filter.getType()) {
      case Or:
        List<Filter> orChildren = ((OrFilter) filter).getChildren();
        for (int i = 0; i < orChildren.size(); i++) {
          Filter childFilter = setTrue(orChildren.get(i), columnsInterval);
          orChildren.set(i, childFilter);
        }
        return new OrFilter(orChildren);
      case And:
        List<Filter> andChildren = ((AndFilter) filter).getChildren();
        for (int i = 0; i < andChildren.size(); i++) {
          Filter childFilter = setTrue(andChildren.get(i), columnsInterval);
          andChildren.set(i, childFilter);
        }
        return new AndFilter(andChildren);
      case Value:
        String path = ((ValueFilter) filter).getPath();
        if (isFunction(path)) {
          return new BoolFilter(true);
        }
        if (!columnRangeContainPath(columnsInterval, path)) {
          return new BoolFilter(true);
        }
        return filter;
      case Path:
        String pathA = ((PathFilter) filter).getPathA();
        String pathB = ((PathFilter) filter).getPathB();
        // 如果filter中含有聚合函数，忽略，设置为true
        if (isFunction(pathA) || isFunction(pathB)) {
          return new BoolFilter(true);
        }
        if (!columnRangeContainPath(columnsInterval, pathA)
            || !columnRangeContainPath(columnsInterval, pathB)) {
          return new BoolFilter(true);
        }
        return filter;
      default:
        return filter;
    }
  }

  public static Filter mergeTrue(Filter filter) {
    switch (filter.getType()) {
      case Or:
        List<Filter> orChildren = ((OrFilter) filter).getChildren();
        for (int i = 0; i < orChildren.size(); i++) {
          Filter childFilter = mergeTrue(orChildren.get(i));
          orChildren.set(i, childFilter);
        }
        for (Filter childFilter : orChildren) {
          if (childFilter.getType() == FilterType.Bool && ((BoolFilter) childFilter).isTrue()) {
            return new BoolFilter(true);
          }
        }
        return new OrFilter(orChildren);
      case And:
        List<Filter> andChildren = ((AndFilter) filter).getChildren();
        for (int i = 0; i < andChildren.size(); i++) {
          Filter childFilter = mergeTrue(andChildren.get(i));
          andChildren.set(i, childFilter);
        }
        List<Filter> removedList = new ArrayList<>();
        for (Filter childFilter : andChildren) {
          if (childFilter.getType() == FilterType.Bool && ((BoolFilter) childFilter).isTrue()) {
            removedList.add(childFilter);
          }
        }
        for (Filter removed : removedList) {
          andChildren.remove(removed);
        }
        if (andChildren.size() == 0) {
          return new BoolFilter(true);
        } else if (andChildren.size() == 1) {
          return andChildren.get(0);
        } else {
          return new AndFilter(andChildren);
        }
      default:
        return filter;
    }
  }

  /** 去除filter中不符合Project的Pattern的Path，这部分无法下推 */
  public static Filter removePathByPatterns(Filter filter, List<String> patterns) {
    Filter newFilter = removePath(filter, patterns);
    return mergeTrue(newFilter);
  }

  private static boolean isInPatterns(String path, List<String> patterns) {
    for (String pattern : patterns) {
      String regexPattern = pattern;
      regexPattern = regexPattern.replaceAll("[.^${}]", "\\\\$0");
      regexPattern = regexPattern.replace("*", ".*");
      regexPattern = "^" + regexPattern + "$";
      Matcher matcher = Pattern.compile(regexPattern).matcher(path);
      if (matcher.find()) {
        return true;
      }
    }
    return false;
  }

  private static Filter removePath(Filter filter, List<String> patterns) {
    switch (filter.getType()) {
      case Or:
        List<Filter> orChildren = ((OrFilter) filter).getChildren();
        for (Filter orChild : orChildren) {
          Filter newFilter = removePath(orChild, patterns);
          orChildren.set(orChildren.indexOf(orChild), newFilter);
        }
        break;
      case And:
        List<Filter> andChildren = ((AndFilter) filter).getChildren();
        for (Filter andChild : andChildren) {
          Filter newFilter = removePath(andChild, patterns);
          andChildren.set(andChildren.indexOf(andChild), newFilter);
        }
        break;
      case Value:
        String path = ((ValueFilter) filter).getPath();
        if (!isInPatterns(path, patterns)) {
          return new BoolFilter(true);
        }
        return filter;
      case Path:
        String pathA = ((PathFilter) filter).getPathA();
        String pathB = ((PathFilter) filter).getPathB();
        if (!isInPatterns(pathA, patterns) || !isInPatterns(pathB, patterns)) {
          return new BoolFilter(true);
        }
        return filter;
      default:
        return filter;
    }
    return filter;
  }

  /** 合并两个filter,并且去除重复的Filter */
  public static Filter mergeFilter(Filter filter1, Filter filter2) {
    if (filter1 == null) {
      return filter2;
    }
    if (filter2 == null) {
      return filter1;
    }

    // filter有重复的情况
    List<Filter> andChildren1 = new ArrayList<>();
    List<Filter> andChildren2 = new ArrayList<>();

    if (filter1.getType() == FilterType.And) {
      andChildren1.addAll(((AndFilter) filter1).getChildren());
    } else {
      andChildren1.add(filter1);
    }
    if (filter2.getType() == FilterType.And) {
      andChildren2.addAll(((AndFilter) filter2).getChildren());
    } else {
      andChildren2.add(filter2);
    }

    List<Filter> andChildren = new ArrayList<>(andChildren1);
    for (Filter child2 : andChildren2) {
      boolean isExist = false;
      for (Filter child1 : andChildren1) {
        if (child1.toString().equals(child2.toString())) {
          isExist = true;
          break;
        }
      }
      if (!isExist) {
        andChildren.add(child2);
      }
    }

    return new AndFilter(andChildren);
  }
}
