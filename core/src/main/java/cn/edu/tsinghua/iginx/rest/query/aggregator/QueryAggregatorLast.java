package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.thrift.AggregateType;

public class QueryAggregatorLast extends QueryAggregator {
  public QueryAggregatorLast() {
    super(QueryAggregatorType.LAST, AggregateType.LAST_VALUE);
  }
}
