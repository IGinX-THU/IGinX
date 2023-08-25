package cn.edu.tsinghua.iginx.mongodb.tools;

import static com.mongodb.client.model.Filters.*;

import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.*;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.mongodb.entity.MongoId;
import cn.edu.tsinghua.iginx.mongodb.entity.MongoPoint;
import cn.edu.tsinghua.iginx.mongodb.query.Query;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.*;
import java.util.stream.Collectors;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

public class FilterUtils {

  public static Bson interval(KeyInterval range) {
    String fieldName = "_id." + MongoId.KEY_SUBFIELD;
    Bson left = gte(fieldName, range.getStartKey());

    if (range.getEndKey() == Long.MAX_VALUE) return left;

    Bson right = lt(fieldName, range.getEndKey());
    return or(and(left, right), eq(fieldName, MongoId.PLACE_HOLDER_KEY));
  }

  public static Bson ranges(List<KeyRange> ranges) {
    return or(
        ranges.stream()
            .map(
                range -> {
                  List<Bson> bounds = new ArrayList<>();
                  if (range.isIncludeBeginKey()) {
                    bounds.add(gte("_id." + MongoId.KEY_SUBFIELD, range.getBeginKey()));
                  } else {
                    bounds.add(gt("_id." + MongoId.KEY_SUBFIELD, range.getBeginKey()));
                  }
                  if (range.isIncludeEndKey()) {
                    bounds.add(lte("_id." + MongoId.KEY_SUBFIELD, range.getEndKey()));
                  } else {
                    bounds.add(lt("_id." + MongoId.KEY_SUBFIELD, range.getEndKey()));
                  }
                  return and(bounds);
                })
            .collect(Collectors.toList()));
  }

  public static Bson nonFieldExcept(Object input, String exceptFieldName) {
    //      {
    //         $expr: {
    //            $eq: [
    //               {},
    //               {
    //                  $unsetField:{
    //                     input: <input>,
    //                     field: <exceptFieldName>
    //                  }
    //               }
    //            ]
    //         }
    //      }

    return expr(
        new Document(
            "$eq", Arrays.asList(new Document(), Query.Utils.unsetField(input, exceptFieldName))));
  }

  public static Bson withTags(TagFilter filter) {
    switch (filter.getType()) {
      case Base:
        return withTags((BaseTagFilter) filter);
      case And:
        return withTags((AndTagFilter) filter);
      case Or:
        return withTags((OrTagFilter) filter);
      case BasePrecise:
        return withTags((BasePreciseTagFilter) filter);
      case Precise:
        return withTags((PreciseTagFilter) filter);
      case WithoutTag:
        return withoutTags();
    }
    throw new IllegalArgumentException("unexpected TagFilter type: " + filter.getType());
  }

  private static Bson withTags(BaseTagFilter filter) {
    String fieldName = "_id." + NameUtils.encodeTagK(filter.getTagKey());
    String tagV = filter.getTagValue();
    if (NameUtils.isWildcard(tagV)) {
      String pattern = tagV.replaceAll("\\*", ".*");
      return regex(fieldName, pattern);
    } else {
      return eq(fieldName, tagV);
    }
  }

  private static Bson withTags(AndTagFilter filter) {
    return and(
        filter.getChildren().stream().map(FilterUtils::withTags).collect(Collectors.toList()));
  }

  private static Bson withTags(OrTagFilter filter) {
    return or(
        filter.getChildren().stream().map(FilterUtils::withTags).collect(Collectors.toList()));
  }

  private static Bson withTags(BasePreciseTagFilter filter) {
    Map<String, String> encodedTags = new HashMap<>();
    filter.getTags().forEach((k, v) -> encodedTags.put(NameUtils.encodeTagK(k), v));

    return expr(
        new Document(
            "$eq",
            Arrays.asList(encodedTags, Query.Utils.unsetField("$_id", MongoId.KEY_SUBFIELD))));
  }

  private static Bson withTags(PreciseTagFilter filter) {
    return or(
        filter.getChildren().stream().map(FilterUtils::withTags).collect(Collectors.toList()));
  }

  private static Bson withoutTags() {
    return nonFieldExcept("$_id", MongoId.KEY_SUBFIELD);
  }

  public static Bson match(Filter filter) {
    switch (filter.getType()) {
      case Key:
        return match((KeyFilter) filter);
      case Value:
        return match((ValueFilter) filter);
      case Path:
        return match((PathFilter) filter);
      case Bool:
        return match((BoolFilter) filter);
      case And:
        return match((AndFilter) filter);
      case Or:
        return match((OrFilter) filter);
      case Not:
        return match((NotFilter) filter);
    }
    throw new IllegalArgumentException("unexpected Filter type: " + filter.getType());
  }

  private static Bson match(KeyFilter filter) {
    return fieldValueOp(
        filter.getOp(),
        "_id." + MongoId.KEY_SUBFIELD,
        new MongoPoint(DataType.LONG, filter.getValue()));
  }

  private static Bson match(ValueFilter filter) {
    String fieldName = NameUtils.encodePath(filter.getPath());
    Value rawValue = filter.getValue();
    MongoPoint point = new MongoPoint(rawValue.getDataType(), rawValue.getValue());

    if (NameUtils.isWildcard(fieldName)) {
      return wildcardValueOp(filter.getOp(), fieldName, point);
    } else {
      return fieldValueOp(filter.getOp(), fieldName, point);
    }
  }

  private static Bson fieldValueOp(Op op, String fieldName, MongoPoint point) {
    BsonValue value = point.getBsonValue();
    switch (op) {
      case GE:
        return gte(fieldName, value);
      case G:
        return gt(fieldName, value);
      case LE:
        return lte(fieldName, value);
      case L:
        return lt(fieldName, value);
      case E:
        return eq(fieldName, value);
      case NE:
        return ne(fieldName, value);
      case LIKE:
        // for example:
        //   use /^.*[s|d]/ match "sadaa",
        //   mongodb return true,
        //   but java return false
        return expr(Query.Utils.regexMatch("$" + fieldName, value.asString().getValue()));
    }
    throw new IllegalArgumentException("unexpected Filter op: " + op);
  }

  private static Bson wildcardValueOp(Op op, String fieldName, MongoPoint point) {
    // for example:
    //   {
    //      $expr:{
    //         $eq:[
    //            {},
    //            {
    //               $arrayToObject:{
    //                  $filter: {
    //                     input: {
    //                        $objectToArray:{
    //                           $unsetField:{
    //                              input: "$$ROOT",
    //                              field: "_id"
    //                           }
    //                        }
    //                     },
    //                     cond: {
    //                        $and:[
    //                           {
    //                              $regexMatch: {
    //                                 input: "$$this.k",
    //                                 regex: "fhdh_.*_dhbd$",
    //                              }
    //                           },
    //                           {
    //                              $not:[
    //                                 {
    //                                    $gt: ["$$this.v", 200]
    //                                 }
    //                              ]
    //                           }
    //                        ]
    //                     }
    //                  }
    //               }
    //            }
    //         ]
    //      }
    //   }

    Bson opCond = exprOp(op, Arrays.asList("$$this.v", point.getBsonValue()));
    Bson notOpCond = new Document("$not", Collections.singletonList(opCond));

    Bson re = Query.Utils.regexMatch("$$this.k", fieldName);
    Bson cond = new Document("$and", Arrays.asList(re, notOpCond));

    Bson toMatch = new Document("$objectToArray", Query.Utils.unsetField("$$ROOT", "_id"));
    Bson notMatched = new Document("$arrayToObject", Query.Utils.filter(toMatch, cond));
    return new Document("$expr", new Document("$eq", Arrays.asList(new Document(), notMatched)));
  }

  private static Bson match(PathFilter filter) {
    String pathA = NameUtils.encodePath(filter.getPathA());
    String pathB = NameUtils.encodePath(filter.getPathB());
    List<String> paths = Arrays.asList(pathA, pathB);

    if (paths.stream().anyMatch(NameUtils::isWildcard)) {
      throw new IllegalArgumentException("undefined operation: " + filter);
    }

    List<Object> exprList = paths.stream().map(path -> "$" + path).collect(Collectors.toList());
    return expr(exprOp(filter.getOp(), exprList));
  }

  private static Bson exprOp(Op op, List<Object> exprList) {
    switch (op) {
      case GE:
        return new Document("$gte", exprList);
      case G:
        return new Document("$gt", exprList);
      case LE:
        return new Document("$lte", exprList);
      case L:
        return new Document("$lt", exprList);
      case E:
        return new Document("$eq", exprList);
      case NE:
        return new Document("$ne", exprList);
      case LIKE:
        return Query.Utils.regexMatch(exprList.get(0), (String) exprList.get(1));
    }
    throw new IllegalArgumentException("unexpected Filter op: " + op);
  }

  private static Bson match(BoolFilter filter) {
    if (filter.isTrue()) {
      return new Document();
    } else {
      return nor(new Document());
    }
  }

  private static Bson match(AndFilter filter) {
    return and(filter.getChildren().stream().map(FilterUtils::match).collect(Collectors.toList()));
  }

  private static Bson match(OrFilter filter) {
    return or(filter.getChildren().stream().map(FilterUtils::match).collect(Collectors.toList()));
  }

  private static Bson match(NotFilter filter) {
    return nor(match(filter.getChild()));
  }
}
