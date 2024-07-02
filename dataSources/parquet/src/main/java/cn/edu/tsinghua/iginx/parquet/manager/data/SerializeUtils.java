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
package cn.edu.tsinghua.iginx.parquet.manager.data;

import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.util.Constants;
import com.google.common.collect.*;
import java.io.StringReader;
import java.util.Collections;
import java.util.Map;
import javax.json.*;

public class SerializeUtils {

  private static final String ALL_FIELD_NAME = Constants.KEY_FIELD_NAME;
  private static final String RANGE_UPPER_NAME = "upper";
  private static final String RANGE_LOWER_NAME = "lower";
  private static final String RANGE_BOUND_NAME = "bound";
  private static final String RANGE_INCLUSIVE_NAME = "inclusive";

  public static <K extends Comparable<K>, F> String serialize(
      AreaSet<K, F> o, ObjectFormat<K> keyFormat, ObjectFormat<F> fieldFormat) {
    JsonObjectBuilder rootBuilder = Json.createObjectBuilder();
    for (F f : o.getFields()) {
      String field = fieldFormat.format(f);
      rootBuilder.addNull(field);
    }

    JsonArrayBuilder allFieldRangeBuilder = jsonBuild(o.getKeys().asRanges(), keyFormat);
    rootBuilder.add(ALL_FIELD_NAME, allFieldRangeBuilder);

    for (Map.Entry<F, RangeSet<K>> entry : o.getSegments().entrySet()) {
      F f = entry.getKey();
      String field = fieldFormat.format(f);

      RangeSet<K> ranges = entry.getValue();
      JsonArrayBuilder builder = jsonBuild(ranges.asRanges(), keyFormat);
      rootBuilder.add(field, builder);
    }

    JsonObject object = rootBuilder.build();
    return object.toString();
  }

  private static <K extends Comparable<K>> JsonArrayBuilder jsonBuild(
      Iterable<Range<K>> ranges, ObjectFormat<K> format) {
    JsonArrayBuilder builder = Json.createArrayBuilder();
    for (Range<K> range : ranges) {
      JsonObjectBuilder rangeBuilder = jsonBuild(range, format);
      builder.add(rangeBuilder);
    }
    return builder;
  }

  private static <K extends Comparable<K>> JsonObjectBuilder jsonBuild(
      Range<K> range, ObjectFormat<K> format) {
    JsonObjectBuilder rangeBuilder = Json.createObjectBuilder();
    if (range.hasUpperBound()) {
      JsonObjectBuilder upperBuilder =
          jsonBuild(range.upperEndpoint(), range.upperBoundType(), format);
      rangeBuilder.add(RANGE_UPPER_NAME, upperBuilder);
    }
    if (range.hasLowerBound()) {
      JsonObjectBuilder lowerBuilder =
          jsonBuild(range.lowerEndpoint(), range.lowerBoundType(), format);
      rangeBuilder.add(RANGE_LOWER_NAME, lowerBuilder);
    }
    return rangeBuilder;
  }

  private static <K> JsonObjectBuilder jsonBuild(K bound, BoundType type, ObjectFormat<K> format) {
    JsonObjectBuilder builder = Json.createObjectBuilder();
    builder.add(RANGE_BOUND_NAME, format.format(bound));
    builder.add(RANGE_INCLUSIVE_NAME, type == BoundType.CLOSED);
    return builder;
  }

  public static <K extends Comparable<K>, F> AreaSet<K, F> deserializeRangeTombstone(
      String s, ObjectFormat<K> keyFormat, ObjectFormat<F> fieldFormat) {
    StringReader sr = new StringReader(s);
    JsonReader reader = Json.createReader(sr);
    JsonObject object = reader.readObject();
    AreaSet<K, F> areas = new AreaSet<>();

    for (Map.Entry<String, JsonValue> entry : object.entrySet()) {
      String fieldStr = entry.getKey();
      F field = fieldFormat.parse(fieldStr);
      JsonValue value = entry.getValue();
      if (value.getValueType() == JsonValue.ValueType.NULL) {
        areas.add(Collections.singleton(field));
        continue;
      }
      RangeSet<K> rangeSet = jsonParseRangeSet((JsonArray) value, keyFormat);
      if (field.equals(ALL_FIELD_NAME)) {
        areas.add(rangeSet);
        continue;
      }
      areas.add(Collections.singleton(field), rangeSet);
    }
    return areas;
  }

  private static <K extends Comparable<K>> RangeSet<K> jsonParseRangeSet(
      JsonArray array, ObjectFormat<K> format) {
    TreeRangeSet<K> rangeSet = TreeRangeSet.create();
    for (JsonValue value : array) {
      Range<K> range = jsonParseRange((JsonObject) value, format);
      rangeSet.add(range);
    }
    return ImmutableRangeSet.copyOf(rangeSet);
  }

  private static <K extends Comparable<K>> Range<K> jsonParseRange(
      JsonObject o, ObjectFormat<K> format) {
    JsonObject upper = o.getJsonObject(RANGE_UPPER_NAME);
    JsonObject lower = o.getJsonObject(RANGE_LOWER_NAME);
    if (upper == null && lower == null) {
      return Range.all();
    } else if (upper == null) {
      K bound = format.parse(lower.getString(RANGE_BOUND_NAME));
      boolean inclusive = lower.getBoolean(RANGE_INCLUSIVE_NAME);
      BoundType type = inclusive ? BoundType.CLOSED : BoundType.OPEN;
      return Range.downTo(bound, type);
    } else if (lower == null) {
      K bound = format.parse(upper.getString(RANGE_BOUND_NAME));
      boolean inclusive = upper.getBoolean(RANGE_INCLUSIVE_NAME);
      BoundType type = inclusive ? BoundType.CLOSED : BoundType.OPEN;
      return Range.upTo(bound, type);
    } else {
      K lBound = format.parse(lower.getString(RANGE_BOUND_NAME));
      boolean lInclusive = lower.getBoolean(RANGE_INCLUSIVE_NAME);
      BoundType lType = lInclusive ? BoundType.CLOSED : BoundType.OPEN;
      K uBound = format.parse(upper.getString(RANGE_BOUND_NAME));
      boolean uInclusive = upper.getBoolean(RANGE_INCLUSIVE_NAME);
      BoundType uType = uInclusive ? BoundType.CLOSED : BoundType.OPEN;
      return Range.range(lBound, lType, uBound, uType);
    }
  }

  public static <K extends Comparable<K>> String serialize(
      RangeSet<K> keyRange, ObjectFormat<K> format) {
    JsonArrayBuilder rangeBuilder = jsonBuild(keyRange.asRanges(), format);
    JsonArray array = rangeBuilder.build();
    return array.toString();
  }

  public static <K extends Comparable<K>> RangeSet<K> deserializeRangeSet(
      String s, ObjectFormat<K> format) {
    StringReader sr = new StringReader(s);
    JsonReader reader = Json.createReader(sr);
    JsonArray array = reader.readArray();
    return jsonParseRangeSet(array, format);
  }
}
