package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.thrift.AggregateType;

public class QueryAggregatorAvg extends QueryAggregator {

  public QueryAggregatorAvg() {
    super(QueryAggregatorType.AVG, AggregateType.AVG);
  }
}
