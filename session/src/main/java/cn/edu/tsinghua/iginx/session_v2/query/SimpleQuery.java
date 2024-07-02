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
package cn.edu.tsinghua.iginx.session_v2.query;

import cn.edu.tsinghua.iginx.session_v2.Arguments;
import java.util.*;

public class SimpleQuery extends Query {

  private final long startKey;

  private final long endKey;

  private final String timePrecision;

  private SimpleQuery(
      Set<String> measurements,
      List<Map<String, List<String>>> tagsList,
      long startKey,
      long endKey) {
    super(Collections.unmodifiableSet(measurements), tagsList);
    this.startKey = startKey;
    this.endKey = endKey;
    this.timePrecision = null;
  }

  private SimpleQuery(
      Set<String> measurements,
      List<Map<String, List<String>>> tagsList,
      long startKey,
      long endKey,
      String timePrecision) {
    super(Collections.unmodifiableSet(measurements), tagsList);
    this.startKey = startKey;
    this.endKey = endKey;
    this.timePrecision = timePrecision;
  }

  public static SimpleQuery.Builder builder() {
    return new Builder();
  }

  public long getStartKey() {
    return startKey;
  }

  public long getEndKey() {
    return endKey;
  }

  public String getTimePrecision() {
    return timePrecision;
  }

  public static class Builder {

    private final Set<String> measurements;

    private final List<Map<String, List<String>>> tagsList;

    private long startKey;

    private long endKey;

    private String timePrecision;

    private Builder() {
      this.measurements = new HashSet<>();
      this.tagsList = new ArrayList<>();
      this.startKey = 0L;
      this.endKey = Long.MAX_VALUE;
      this.timePrecision = null;
    }

    public SimpleQuery.Builder addMeasurement(String measurement) {
      Arguments.checkNonEmpty(measurement, "measurement");
      this.measurements.add(measurement);
      return this;
    }

    public SimpleQuery.Builder addMeasurements(Set<String> measurements) {
      measurements.forEach(measurement -> Arguments.checkNonEmpty(measurement, "measurement"));
      this.measurements.addAll(measurements);
      return this;
    }

    public SimpleQuery.Builder addTags(Map<String, List<String>> tags) {
      this.tagsList.add(tags);
      return this;
    }

    public SimpleQuery.Builder addTagsList(List<Map<String, List<String>>> tagsList) {
      Arguments.checkListNonEmpty(tagsList, "tagsList");
      this.tagsList.addAll(tagsList);
      return this;
    }

    public SimpleQuery.Builder startKey(long startKey) {
      if (startKey < 0) {
        throw new IllegalArgumentException("startKey must greater than zero.");
      }
      if (startKey >= endKey) {
        throw new IllegalArgumentException("startKey must less than endKey.");
      }
      this.startKey = startKey;
      return this;
    }

    public SimpleQuery.Builder endKey(long endKey) {
      if (endKey < 0) {
        throw new IllegalArgumentException("endKey mush greater than zero.");
      }
      if (endKey <= startKey) {
        throw new IllegalArgumentException("endKey must greater than startKey.");
      }
      this.endKey = endKey;
      return this;
    }

    public SimpleQuery.Builder timePrecision(String timePrecision) {
      Arguments.checkNotNull(timePrecision, "timePrecision");
      this.timePrecision = timePrecision;
      return this;
    }

    public SimpleQuery build() {
      if (this.measurements.isEmpty()) {
        throw new IllegalStateException("simple query at least has one measurement.");
      }
      return new SimpleQuery(measurements, tagsList, startKey, endKey, timePrecision);
    }
  }
}
