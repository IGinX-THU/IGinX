package cn.edu.tsinghua.iginx.mongodb.tools;

import static com.mongodb.client.model.Filters.*;

import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.mongodb.client.model.Filters;
import java.util.*;
import javax.annotation.Nullable;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

public class FilterUtils {

  public static Bson interval(KeyInterval range) {
    Bson left = gte("_id", range.getStartKey());
    Bson right = lt("_id", range.getEndKey());
    return and(left, right);
  }

  public static Bson ranges(List<KeyRange> ranges) {
    List<Bson> rangeFilters = new ArrayList<>();
    for (KeyRange range : ranges) {
      List<Bson> bounds = new ArrayList<>();
      if (range.isIncludeBeginKey()) {
        bounds.add(gte("_id", range.getBeginKey()));
      } else {
        bounds.add(gt("_id", range.getBeginKey()));
      }
      if (range.isIncludeEndKey()) {
        bounds.add(lte("_id", range.getEndKey()));
      } else {
        bounds.add(lt("_id", range.getEndKey()));
      }
      rangeFilters.add(and(bounds));
    }
    return or(rangeFilters);
  }

  public static Bson getPreFilter(Filter filter) {
    List<Bson> rangeFilters = new ArrayList<>();
    for (Pair<Long, Long> range : extractRanges(filter)) {
      if (range.k < range.v) {
        Bson leftBound = Filters.gte("_id", range.k);
        Bson rightBound = Filters.lte("_id", range.v);
        Bson rangeFilter = Filters.and(leftBound, rightBound);
        rangeFilters.add(rangeFilter);
      } else if (range.k.equals(range.v)) {
        Bson rangeFilter = Filters.eq("_id", range.k);
        rangeFilters.add(rangeFilter);
      }
    }
    return Filters.or(rangeFilters);
  }

  private static final List<Pair<Long, Long>> EMPTY_RANGES = new ArrayList<>();

  private static final Pair<Long, Long> FULL_RANGE = new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE);

  private static final List<Pair<Long, Long>> FULL_RANGES = Collections.singletonList(FULL_RANGE);

  private static List<Pair<Long, Long>> extractRanges(Filter filter) {
    switch (filter.getType()) {
      case Key:
        return extractRanges((KeyFilter) filter);
      case Bool:
        return extractRanges((BoolFilter) filter);
      case And:
        return extractRanges((AndFilter) filter);
      case Or:
        return extractRanges((OrFilter) filter);
      default:
        return FULL_RANGES;
    }
  }

  private static List<Pair<Long, Long>> extractRanges(KeyFilter filter) {
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
        return Collections.singletonList(new Pair<>(Long.MIN_VALUE, value));
      case L:
      case L_AND:
        if (value == Long.MIN_VALUE) {
          return EMPTY_RANGES;
        } else {
          return Collections.singletonList(new Pair<>(Long.MIN_VALUE, value - 1));
        }
      case E:
      case E_AND:
        return Collections.singletonList(new Pair<>(value, value));
      case NE:
      case NE_AND:
        if (value == Long.MIN_VALUE) {
          return Collections.singletonList(new Pair<>(value + 1, Long.MAX_VALUE));
        } else if (value == Long.MAX_VALUE) {
          return Collections.singletonList(new Pair<>(Long.MIN_VALUE, value - 1));
        } else {
          return Arrays.asList(
              new Pair<>(Long.MIN_VALUE, value - 1), new Pair<>(value, Long.MAX_VALUE));
        }
      default:
        return FULL_RANGES;
    }
  }

  private static List<Pair<Long, Long>> extractRanges(BoolFilter filter) {
    if (filter.isTrue()) {
      return FULL_RANGES;
    } else {
      return EMPTY_RANGES;
    }
  }

  private static List<Pair<Long, Long>> extractRanges(AndFilter filter) {
    List<Filter> children = filter.getChildren();
    List<Pair<Long, Long>> result = FULL_RANGES;
    for (Filter child : children) {
      List<Pair<Long, Long>> childRanges = extractRanges(child);
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

  private static List<Pair<Long, Long>> extractRanges(OrFilter filter) {
    List<Filter> children = filter.getChildren();
    List<List<Pair<Long, Long>>> childrenRanges = new ArrayList<>(children.size());
    for (Filter child : children) {
      childrenRanges.add(extractRanges(child));
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

  @Nullable
  public static Bson getPostFilter(Filter filter, Map<Field, String> renamedFields) {
    switch (filter.getType()) {
      case Key:
      case Expr:
      case Not:
        return null;
      case Value:
        return getFilter((ValueFilter) filter, renamedFields);
      case Path:
        return getFilter((PathFilter) filter, renamedFields);
      case Bool:
        return getFilter((BoolFilter) filter);
      case And:
        return getPostFilter((AndFilter) filter, renamedFields);
      case Or:
        return getFilter((OrFilter) filter, renamedFields);
      default:
        throw new IllegalStateException("unexpected filter type: " + filter.getType());
    }
  }

  @Nullable
  public static Bson getFilter(Filter filter, Map<Field, String> renamedFields) {
    switch (filter.getType()) {
      case Key:
        return getFilter((KeyFilter) filter);
      case Value:
        return getFilter((ValueFilter) filter, renamedFields);
      case Path:
        return getFilter((PathFilter) filter, renamedFields);
      case Expr:
      case Not:
        return null;
      case Bool:
        return getFilter((BoolFilter) filter);
      case And:
        return getPostFilter((AndFilter) filter, renamedFields);
      case Or:
        return getFilter((OrFilter) filter, renamedFields);
      default:
        throw new IllegalStateException("unexpected filter type: " + filter.getType());
    }
  }

  private static Bson getFilter(KeyFilter filter) {
    return fieldValueOp(filter.getOp(), "_id", new BsonInt64(filter.getValue()));
  }

  @Nullable
  private static Bson getFilter(ValueFilter filter, Map<Field, String> renamedFields) {
    String pattern = filter.getPath();
    Collection<String> names = getMatchNames(pattern, renamedFields);
    List<Bson> subFilters = new ArrayList<>();
    for (String name : names) {
      Value value = filter.getValue();
      BsonValue bsonValue = TypeUtils.toBsonValue(value.getDataType(), value.getValue());
      Bson subFilter = fieldValueOp(filter.getOp(), name, bsonValue);
      subFilters.add(subFilter);
    }
    if (subFilters.size() == 1) {
      return subFilters.get(0);
    } else if (subFilters.isEmpty()) {
      return null;
    } else {
      return unionComparisonFilters(filter.getOp(), subFilters);
    }
  }

  private static List<String> getMatchNames(String pattern, Map<Field, String> renamedFields) {
    List<String> patterns = Collections.singletonList(pattern);
    List<Field> matchedFields = NameUtils.match(renamedFields.keySet(), patterns, null);
    List<String> matchedNames = new ArrayList<>();
    for (Field field : matchedFields) {
      matchedNames.add(renamedFields.get(field));
    }
    return matchedNames;
  }

  public static Bson fieldValueOp(Op op, String fieldName, BsonValue value) {
    if (fieldName.contains("*")) {
      throw new IllegalArgumentException("wildcard is not support");
    }
    switch (op) {
      case GE:
      case GE_AND:
        return gte(fieldName, value);
      case G:
      case G_AND:
        return gt(fieldName, value);
      case LE:
      case LE_AND:
        return lte(fieldName, value);
      case L:
      case L_AND:
        return lt(fieldName, value);
      case E:
      case E_AND:
        return eq(fieldName, value);
      case NE:
      case NE_AND:
        return ne(fieldName, value);
      case LIKE:
      case LIKE_AND:
        // why append a '$' to the pattern
        // for example:
        //   match "sadaa" with /^.*[s|d]/,
        //   mongodb return true, but java return false
        return expr(
            new Document(
                "$regexMatch",
                new Document("input", "$" + fieldName)
                    .append("regex", value.asString().getValue() + "$")));
    }
    throw new IllegalStateException("unexpected Filter op: " + op);
  }

  @Nullable
  private static Bson getFilter(PathFilter filter, Map<Field, String> renamedFields) {
    List<String> namesA = getMatchNames(filter.getPathA(), renamedFields);
    List<String> namesB = getMatchNames(filter.getPathB(), renamedFields);

    if (namesA.size() > 1 && namesB.size() > 1) {
      throw new IllegalArgumentException("undefined filter: " + filter);
    }

    List<Bson> subFilters = new ArrayList<>();
    if (namesA.size() == 1) {
      String nameA = namesA.get(0);
      for (String nameB : namesB) {
        subFilters.add(fieldOp(filter.getOp(), nameA, nameB));
      }
    } else if (namesB.size() == 1) {
      String nameB = namesB.get(0);
      for (String nameA : namesA) {
        subFilters.add(fieldOp(filter.getOp(), nameA, nameB));
      }
    }

    if (subFilters.size() == 1) {
      return subFilters.get(0);
    } else if (subFilters.isEmpty()) {
      return null;
    } else {
      return unionComparisonFilters(filter.getOp(), subFilters);
    }
  }

  private static Bson unionComparisonFilters(Op op, List<Bson> subFilters) {
    switch (op) {
      case GE:
      case G:
      case LE:
      case L:
      case E:
      case NE:
      case LIKE:
        return or(subFilters);
      case GE_AND:
      case G_AND:
      case LE_AND:
      case L_AND:
      case E_AND:
      case NE_AND:
      case LIKE_AND:
        return and(subFilters);
      default:
        throw new IllegalArgumentException("unexpected Filter op: " + op);
    }
  }

  public static Bson fieldOp(Op op, String fieldA, String fieldB) {
    List<String> fields = Arrays.asList("$" + fieldA, "$" + fieldB);
    if (fieldA.contains("*") || fieldB.contains("*")) {
      throw new IllegalArgumentException("wildcard is not support");
    }
    switch (op) {
      case GE:
      case GE_AND:
        return expr(new Document("$gte", fields));
      case G:
      case G_AND:
        return expr(new Document("$gt", fields));
      case LE:
      case LE_AND:
        return expr(new Document("$lte", fields));
      case L:
      case L_AND:
        return expr(new Document("$lt", fields));
      case E:
      case E_AND:
        return expr(new Document("$eq", fields));
      case NE:
      case NE_AND:
        return expr(new Document("$ne", fields));
      case LIKE:
      case LIKE_AND:
        Bson pattern = new Document("$concat", Arrays.asList(fields.get(0), "$"));
        return expr(
            new Document(
                "$regexMatch", new Document("input", fields.get(0)).append("regex", pattern)));
    }
    throw new IllegalArgumentException("unexpected Filter op: " + op);
  }

  private static Bson getFilter(BoolFilter filter) {
    if (filter.isTrue()) {
      return new Document();
    } else {
      return nor(new Document());
    }
  }

  @Nullable
  private static Bson getPostFilter(AndFilter filter, Map<Field, String> renamedFields) {
    List<Bson> subFilterList = new ArrayList<>();
    for (Filter child : filter.getChildren()) {
      Bson childFilter = getPostFilter(child, renamedFields);
      if (childFilter != null) {
        subFilterList.add(childFilter);
      }
    }
    if (subFilterList.isEmpty()) {
      return null;
    }
    return and(subFilterList);
  }

  @Nullable
  private static Bson getFilter(OrFilter filter, Map<Field, String> renamedFields) {
    List<Bson> subFilterList = new ArrayList<>();
    for (Filter child : filter.getChildren()) {
      Bson childFilter = getFilter(child, renamedFields);
      if (childFilter != null) {
        subFilterList.add(childFilter);
      }
    }
    if (subFilterList.isEmpty()) {
      return null;
    }
    return or(subFilterList);
  }
}
