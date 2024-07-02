package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.thrift.AggregateType;

public class QueryAggregatorMin extends QueryAggregator {
  public QueryAggregatorMin() {
    super(QueryAggregatorType.MIN, AggregateType.MIN);
  }
}
