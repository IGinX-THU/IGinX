package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.thrift.AggregateType;

public class QueryAggregatorCount extends QueryAggregator {
  public QueryAggregatorCount() {
    super(QueryAggregatorType.COUNT, AggregateType.COUNT);
  }
}
