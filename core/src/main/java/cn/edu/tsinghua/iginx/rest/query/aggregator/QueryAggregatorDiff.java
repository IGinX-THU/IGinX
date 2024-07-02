package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.RestUtils;
import cn.edu.tsinghua.iginx.rest.bean.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryAggregatorDiff extends QueryAggregator {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryAggregatorDiff.class);

  public QueryAggregatorDiff() {
    super(QueryAggregatorType.DIFF);
  }

  public <T extends Number> QueryResultDataset processData(
      QueryResultDataset queryResultDataset,
      SessionQueryDataSet sessionQueryDataSet,
      int n,
      int m) {
    T last = null;
    T now = null;
    int datapoints = 0;

    for (int i = 0; i < n; i++) {
      for (int j = 0; j < m; j++) {
        Object value = sessionQueryDataSet.getValues().get(i).get(j);
        if (value != null) {
          if (now == null) {
            now = (T) value;
          }
          datapoints += 1;
        }
      }
      if (i != 0) {
        queryResultDataset.add(sessionQueryDataSet.getKeys()[i], subtract(now, last));
      }
      last = now;
      now = null;
    }
    queryResultDataset.setSampleSize(datapoints);
    return queryResultDataset;
  }

  private <T extends Number> Number subtract(T now, T last) {
    if (now instanceof Long && last instanceof Long) {
      return now.longValue() - last.longValue();
    } else if (now instanceof Double && last instanceof Double) {
      return now.doubleValue() - last.doubleValue();
    }
    throw new IllegalArgumentException("Unsupported data type");
  }

  @Override
  public QueryResultDataset doAggregate(
      RestSession session,
      List<String> paths,
      List<Map<String, List<String>>> tagList,
      long startKey,
      long endKey) {
    QueryResultDataset queryResultDataset = new QueryResultDataset();
    try {
      SessionQueryDataSet sessionQueryDataSet = session.queryData(paths, startKey, endKey, tagList);
      queryResultDataset.setPaths(getPathsFromSessionQueryDataSet(sessionQueryDataSet));
      DataType type = RestUtils.checkType(sessionQueryDataSet);
      int n = sessionQueryDataSet.getKeys().length;
      int m = sessionQueryDataSet.getPaths().size();
      switch (type) {
        case LONG:
          queryResultDataset = processData(queryResultDataset, sessionQueryDataSet, n, m);
          break;
        case DOUBLE:
          queryResultDataset = processData(queryResultDataset, sessionQueryDataSet, n, m);
          break;
        default:
          throw new Exception("Unsupported data type");
      }
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
    }
    return queryResultDataset;
  }
}
