package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.thrift.AggregateType;

public class QueryAggregatorMax extends QueryAggregator {
  public QueryAggregatorMax() {
    super(QueryAggregatorType.MAX, AggregateType.MAX);
  }
}
