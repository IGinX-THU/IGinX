package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.thrift.AggregateType;

public class QueryAggregatorSum extends QueryAggregator {
  public QueryAggregatorSum() {
    super(QueryAggregatorType.SUM, AggregateType.SUM);
  }
}
