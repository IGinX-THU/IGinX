package cn.edu.tsinghua.iginx.parquet.tools;

import cn.edu.tsinghua.iginx.parquet.db.RangeTombstone;
import cn.edu.tsinghua.iginx.parquet.entity.Constants;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.io.StringReader;
import java.util.Collections;
import javax.json.*;

public class SerializeRangeTombstoneUtils {

  private static final String ALL_FIELD_NAME = Constants.KEY_FIELD_NAME;
  private static final String RANGE_UPPER_NAME = "upper";
  private static final String RANGE_LOWER_NAME = "lower";
  private static final String RANGE_BOUND_NAME = "bound";
  private static final String RANGE_INCLUSIVE_NAME = "inclusive";

  public static String serialize(RangeTombstone<Long, String> o) {
    JsonObjectBuilder rootBuilder = Json.createObjectBuilder();
    o.getDeletedRanges()
        .forEach(
            (field, ranges) -> {
              JsonArrayBuilder builder = Json.createArrayBuilder();
              jsonBuild(builder, ranges.asRanges());
              if (field == null) {
                field = ALL_FIELD_NAME;
              }
              rootBuilder.add(field, builder);
            });
    JsonObject object = rootBuilder.build();
    return object.toString();
  }

  private static void jsonBuild(JsonArrayBuilder builder, Iterable<Range<Long>> ranges) {
    for (Range<Long> range : ranges) {
      JsonObjectBuilder rangeBuilder = Json.createObjectBuilder();
      if (range.hasUpperBound()) {
        JsonObjectBuilder upperBuilder = jsonBuild(range.upperEndpoint(), range.upperBoundType());
        rangeBuilder.add(RANGE_UPPER_NAME, upperBuilder);
      }
      if (range.hasLowerBound()) {
        JsonObjectBuilder lowerBuilder = jsonBuild(range.lowerEndpoint(), range.lowerBoundType());
        rangeBuilder.add(RANGE_LOWER_NAME, lowerBuilder);
      }
      builder.add(rangeBuilder);
    }
  }

  private static JsonObjectBuilder jsonBuild(Long bound, BoundType type) {
    JsonObjectBuilder builder = Json.createObjectBuilder();
    builder.add(RANGE_BOUND_NAME, bound);
    builder.add(RANGE_INCLUSIVE_NAME, type == BoundType.CLOSED);
    return builder;
  }

  public static RangeTombstone<Long, String> deserialize(String s) {
    StringReader sr = new StringReader(s);
    JsonReader reader = Json.createReader(sr);
    JsonObject object = reader.readObject();
    RangeTombstone<Long, String> rangeTombstone = new RangeTombstone<Long, String>();
    object.forEach(
        (field, value) -> {
          RangeSet<Long> rangeSet = jsonParseRangeSet((JsonArray) value);
          if (field.equals(ALL_FIELD_NAME)) {
            field = null;
          }
          rangeTombstone.delete(Collections.singleton(field), rangeSet);
        });
    return rangeTombstone;
  }

  private static RangeSet<Long> jsonParseRangeSet(JsonArray array) {
    ImmutableRangeSet.Builder<Long> builder = ImmutableRangeSet.builder();
    for (JsonValue value : array) {
      Range<Long> range = jsonParseRange((JsonObject) value);
      builder.add(range);
    }
    return builder.build();
  }

  private static Range<Long> jsonParseRange(JsonObject o) {
    JsonObject upper = o.getJsonObject(RANGE_UPPER_NAME);
    JsonObject lower = o.getJsonObject(RANGE_LOWER_NAME);
    if (upper == null && lower == null) {
      return Range.all();
    } else if (upper == null) {
      long bound = lower.getJsonNumber(RANGE_BOUND_NAME).longValue();
      boolean inclusive = lower.getBoolean(RANGE_INCLUSIVE_NAME);
      BoundType type = inclusive ? BoundType.CLOSED : BoundType.OPEN;
      return Range.downTo(bound, type);
    } else if (lower == null) {
      long bound = upper.getJsonNumber(RANGE_BOUND_NAME).longValue();
      boolean inclusive = upper.getBoolean(RANGE_INCLUSIVE_NAME);
      BoundType type = inclusive ? BoundType.CLOSED : BoundType.OPEN;
      return Range.upTo(bound, type);
    } else {
      long lBound = lower.getJsonNumber(RANGE_BOUND_NAME).longValue();
      boolean lInclusive = lower.getBoolean(RANGE_INCLUSIVE_NAME);
      BoundType lType = lInclusive ? BoundType.CLOSED : BoundType.OPEN;
      long uBound = upper.getJsonNumber(RANGE_BOUND_NAME).longValue();
      boolean uInclusive = upper.getBoolean(RANGE_INCLUSIVE_NAME);
      BoundType uType = uInclusive ? BoundType.CLOSED : BoundType.OPEN;
      return Range.range(lBound, lType, uBound, uType);
    }
  }
}
