package cn.edu.tsinghua.iginx.session_v2.query;

import cn.edu.tsinghua.iginx.session_v2.Arguments;
import java.util.*;

public class LastQuery extends Query {

  private final long startKey;

  private final String timePrecision;

  public LastQuery(
      Set<String> measurements, List<Map<String, List<String>>> tagsList, long startKey) {
    super(measurements, tagsList);
    this.startKey = startKey;
    this.timePrecision = null;
  }

  public LastQuery(
      Set<String> measurements,
      List<Map<String, List<String>>> tagsList,
      long startKey,
      String timePrecision) {
    super(measurements, tagsList);
    this.startKey = startKey;
    this.timePrecision = timePrecision;
  }

  public long getStartKey() {
    return startKey;
  }

  public static LastQuery.Builder builder() {
    return new LastQuery.Builder();
  }

  public String getTimePrecision() {
    return timePrecision;
  }

  public static class Builder {

    private final Set<String> measurements;

    private final List<Map<String, List<String>>> tagsList;

    private long startKey;

    private String timePrecision;

    private Builder() {
      this.measurements = new HashSet<>();
      this.tagsList = new ArrayList<>();
      this.startKey = 0L;
      this.timePrecision = null;
    }

    public LastQuery.Builder addMeasurement(String measurement) {
      Arguments.checkNonEmpty(measurement, "measurement");
      this.measurements.add(measurement);
      return this;
    }

    public LastQuery.Builder addMeasurements(Set<String> measurements) {
      measurements.forEach(measurement -> Arguments.checkNonEmpty(measurement, "measurement"));
      this.measurements.addAll(measurements);
      return this;
    }

    public LastQuery.Builder addTags(Map<String, List<String>> tags) {
      this.tagsList.add(tags);
      return this;
    }

    public LastQuery.Builder addTagsList(List<Map<String, List<String>>> tagsList) {
      Arguments.checkListNonEmpty(tagsList, "tagsList");
      this.tagsList.addAll(tagsList);
      return this;
    }

    public LastQuery.Builder startKey(long startKey) {
      if (startKey < 0) {
        throw new IllegalArgumentException("startKey must greater than zero.");
      }
      this.startKey = startKey;
      return this;
    }

    public LastQuery.Builder timePrecision(String timePrecision) {
      Arguments.checkNotNull(timePrecision, "timePrecision");
      this.timePrecision = timePrecision;
      return this;
    }

    public LastQuery build() {
      if (this.measurements.isEmpty()) {
        throw new IllegalStateException("last query at least has one measurement.");
      }
      return new LastQuery(measurements, tagsList, startKey, timePrecision);
    }
  }
}
