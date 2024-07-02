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

public class QueryAggregatorDev extends QueryAggregator {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryAggregatorDev.class);

  public QueryAggregatorDev() {
    super(QueryAggregatorType.DEV);
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
        case DOUBLE:
          int datapoints = 0;
          double sum = 0, sum2 = 0;
          long cnt = 0;
          for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
              if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
                sum += (double) sessionQueryDataSet.getValues().get(i).get(j);
                sum2 += Math.pow((double) sessionQueryDataSet.getValues().get(i).get(j), 2);
                cnt += 1;
                datapoints += 1;
              }
            }
            if (i == n - 1
                || RestUtils.getInterval(sessionQueryDataSet.getKeys()[i], startKey, getDur())
                    != RestUtils.getInterval(
                        sessionQueryDataSet.getKeys()[i + 1], startKey, getDur())) {
              queryResultDataset.add(
                  RestUtils.getIntervalStart(sessionQueryDataSet.getKeys()[i], startKey, getDur()),
                  sum2 / cnt - Math.pow(sum / cnt, 2));
              sum = 0;
              sum2 = 0;
              cnt = 0;
            }
          }
          queryResultDataset.setSampleSize(datapoints);
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
