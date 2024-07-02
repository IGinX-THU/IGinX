package cn.edu.tsinghua.iginx.session_v2.query;

import cn.edu.tsinghua.iginx.session_v2.Arguments;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import java.util.*;

public class AggregateQuery extends Query {

  private final long startKey;

  private final long endKey;

  private final AggregateType aggregateType;

  private final String timePrecision;

  public AggregateQuery(
      Set<String> measurements,
      List<Map<String, List<String>>> tagsList,
      long startKey,
      long endKey,
      AggregateType aggregateType) {
    super(measurements, tagsList);
    this.startKey = startKey;
    this.endKey = endKey;
    this.aggregateType = aggregateType;
    this.timePrecision = null;
  }

  public AggregateQuery(
      Set<String> measurements,
      List<Map<String, List<String>>> tagsList,
      long startKey,
      long endKey,
      AggregateType aggregateType,
      String timePrecision) {
    super(measurements, tagsList);
    this.startKey = startKey;
    this.endKey = endKey;
    this.aggregateType = aggregateType;
    this.timePrecision = timePrecision;
  }

  public static AggregateQuery.Builder builder() {
    return new AggregateQuery.Builder();
  }

  public long getStartKey() {
    return startKey;
  }

  public long getEndKey() {
    return endKey;
  }

  public AggregateType getAggregateType() {
    return aggregateType;
  }

  public String getTimePrecision() {
    return timePrecision;
  }

  public static class Builder {

    private final Set<String> measurements;

    private final List<Map<String, List<String>>> tagsList;

    private long startKey;

    private long endKey;

    private AggregateType aggregateType;

    private String timePrecision;

    private Builder() {
      this.measurements = new HashSet<>();
      this.tagsList = new ArrayList<>();
      this.startKey = 0L;
      this.endKey = Long.MAX_VALUE;
      this.timePrecision = null;
    }

    public AggregateQuery.Builder addMeasurement(String measurement) {
      Arguments.checkNonEmpty(measurement, "measurement");
      this.measurements.add(measurement);
      return this;
    }

    public AggregateQuery.Builder addMeasurements(Set<String> measurements) {
      measurements.forEach(measurement -> Arguments.checkNonEmpty(measurement, "measurement"));
      this.measurements.addAll(measurements);
      return this;
    }

    // 每次添加的map是一个and表达式，不同map之间是or表达式
    public AggregateQuery.Builder addTags(Map<String, List<String>> tags) {
      this.tagsList.add(tags);
      return this;
    }

    public AggregateQuery.Builder addTagsList(List<Map<String, List<String>>> tagsList) {
      Arguments.checkListNonEmpty(tagsList, "tagsList");
      this.tagsList.addAll(tagsList);
      return this;
    }

    public AggregateQuery.Builder startKey(long startKey) {
      if (startKey < 0) {
        throw new IllegalArgumentException("startKey must greater than zero.");
      }
      if (startKey >= endKey) {
        throw new IllegalArgumentException("startKey must less than endKey.");
      }
      this.startKey = startKey;
      return this;
    }

    public AggregateQuery.Builder endKey(long endKey) {
      if (endKey < 0) {
        throw new IllegalArgumentException("endKey mush greater than zero.");
      }
      if (endKey <= startKey) {
        throw new IllegalArgumentException("endKey must greater than startKey.");
      }
      this.endKey = endKey;
      return this;
    }

    public AggregateQuery.Builder aggregate(AggregateType aggregateType) {
      Arguments.checkNotNull(aggregateType, "aggregateType");
      this.aggregateType = aggregateType;
      return this;
    }

    public AggregateQuery.Builder timePrecision(String timePrecision) {
      Arguments.checkNotNull(timePrecision, "timePrecision");
      this.timePrecision = timePrecision;
      return this;
    }

    public AggregateQuery build() {
      if (this.measurements.isEmpty()) {
        throw new IllegalStateException("simple query at least has one measurement.");
      }
      if (this.aggregateType == null) {
        throw new IllegalStateException("aggregate type should not be null.");
      }
      return new AggregateQuery(
          measurements, tagsList, startKey, endKey, aggregateType, timePrecision);
    }
  }
}
