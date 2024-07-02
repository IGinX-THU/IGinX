package cn.edu.tsinghua.iginx.session_v2.query;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class Query {

  protected final Set<String> measurements;

  protected final List<Map<String, List<String>>> tagsList;

  public Query(Set<String> measurements) {
    this(measurements, null);
  }

  public Query(Set<String> measurements, List<Map<String, List<String>>> tagsList) {
    this.measurements = measurements;
    this.tagsList = tagsList;
  }

  public Set<String> getMeasurements() {
    return measurements;
  }

  public List<Map<String, List<String>>> getTagsList() {
    return tagsList;
  }
}
