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

package cn.edu.tsinghua.iginx.parquet.db.lsm.tombstone;

import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ObjectFormat;
import cn.edu.tsinghua.iginx.parquet.shared.Constants;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
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
      Tombstone<K, F> o, ObjectFormat<K> keyFormat, ObjectFormat<F> fieldFormat) {
    JsonObjectBuilder rootBuilder = Json.createObjectBuilder();

    for (F f : o.getDeletedColumns()) {
      String field = fieldFormat.format(f);
      rootBuilder.addNull(field);
    }

    JsonArrayBuilder allFieldRangeBuilder = jsonBuild(o.getDeletedRows().asRanges(), keyFormat);
    rootBuilder.add(ALL_FIELD_NAME, allFieldRangeBuilder);

    for (Map.Entry<F, RangeSet<K>> entry : o.getDeletedRanges().entrySet()) {
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

  public static <K extends Comparable<K>, F> Tombstone<K, F> deserializeRangeTombstone(
      String s, ObjectFormat<K> keyFormat, ObjectFormat<F> fieldFormat) {
    StringReader sr = new StringReader(s);
    JsonReader reader = Json.createReader(sr);
    JsonObject object = reader.readObject();
    Tombstone<K, F> tombstone = new Tombstone<>();

    for (Map.Entry<String, JsonValue> entry : object.entrySet()) {
      String fieldStr = entry.getKey();
      F field = fieldFormat.parse(fieldStr);
      JsonValue value = entry.getValue();
      if (value.getValueType() == JsonValue.ValueType.NULL) {
        tombstone.delete(Collections.singleton(field));
        continue;
      }
      RangeSet<K> rangeSet = jsonParseRangeSet((JsonArray) value, keyFormat);
      if (field.equals(ALL_FIELD_NAME)) {
        tombstone.delete(rangeSet);
        continue;
      }
      tombstone.delete(Collections.singleton(field), rangeSet);
    }
    return tombstone;
  }

  private static <K extends Comparable<K>> RangeSet<K> jsonParseRangeSet(
      JsonArray array, ObjectFormat<K> format) {
    ImmutableRangeSet.Builder<K> builder = ImmutableRangeSet.builder();
    for (JsonValue value : array) {
      Range<K> range = jsonParseRange((JsonObject) value, format);
      builder.add(range);
    }
    return builder.build();
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
