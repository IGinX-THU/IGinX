/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
