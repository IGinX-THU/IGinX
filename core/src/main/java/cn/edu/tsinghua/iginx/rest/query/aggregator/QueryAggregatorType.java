package cn.edu.tsinghua.iginx.rest.query.aggregator;

public enum QueryAggregatorType {
  MAX("max"),
  MIN("min"),
  SUM("sum"),
  COUNT("count"),
  AVG("avg"),
  FIRST("first"),
  LAST("last"),
  DEV("dev"),
  DIFF("diff"),
  DIV("div"),
  FILTER("filter"),
  SAVE_AS("save_as"),
  RATE("rate"),
  SAMPLER("sampler"),
  PERCENTILE("percentile"),
  SHOW_COLUMNS("show_columns"),
  NONE("");
  private final String type;

  QueryAggregatorType(String type) {
    this.type = type;
  }

  public String getType() {
    return type;
  }
}
