package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.bean.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryShowColumns extends QueryAggregator {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryShowColumns.class);

  public QueryShowColumns() {
    super(QueryAggregatorType.SHOW_COLUMNS);
  }

  public QueryResultDataset doAggregate(RestSession session) {
    QueryResultDataset queryResultDataset = new QueryResultDataset();
    try {
      SessionQueryDataSet sessionQueryDataSet = session.showColumns();
      queryResultDataset.setPaths(getPathsFromShowColumns(sessionQueryDataSet));
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
    }
    return queryResultDataset;
  }
}
