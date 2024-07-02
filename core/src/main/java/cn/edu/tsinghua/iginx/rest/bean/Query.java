package cn.edu.tsinghua.iginx.rest.bean;

import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class Query {
  private Long startAbsolute;
  private Long endAbsolute;
  private Long cacheTime;
  private String timeZone;
  private TimePrecision timePrecision;
  private List<QueryMetric> queryMetrics = new ArrayList<>();

  public void addQueryMetrics(QueryMetric queryMetric) {
    this.queryMetrics.add(queryMetric);
  }

  public void addFirstAggregator() {
    for (QueryMetric metric : queryMetrics) {
      metric.addFirstAggregator();
    }
  }

  public void setNullNewAnno() {
    for (QueryMetric metric : queryMetrics) {
      metric.setNewAnnotationLimit(new AnnotationLimit());
    }
  }
}
