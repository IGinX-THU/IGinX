package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.thrift.AggregateType;

public class QueryAggregatorFirst extends QueryAggregator {
  public QueryAggregatorFirst() {
    super(QueryAggregatorType.FIRST, AggregateType.FIRST_VALUE);
  }
}
