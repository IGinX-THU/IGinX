package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.bean.Metric;
import cn.edu.tsinghua.iginx.rest.bean.QueryResultDataset;
import cn.edu.tsinghua.iginx.rest.insert.DataPointsParser;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryAggregatorSaveAs extends QueryAggregator {
  public QueryAggregatorSaveAs() {
    super(QueryAggregatorType.SAVE_AS);
  }

  @Override
  public QueryResultDataset doAggregate(
      RestSession session,
      List<String> paths,
      List<Map<String, List<String>>> tagList,
      long startKey,
      long endKey) {
    DataPointsParser parser = new DataPointsParser();
    List<Metric> metrics = new ArrayList<>();
    Metric ins = new Metric();
    String name = paths.get(0).split("\\.")[paths.get(0).split("\\.").length - 1];
    ins.setName(getMetric_name());
    ins.addTag("saved_from", name);
    QueryResultDataset queryResultDataset = new QueryResultDataset();
    SessionQueryDataSet sessionQueryDataSet = session.queryData(paths, startKey, endKey, tagList);
    queryResultDataset.setPaths(getPathsFromSessionQueryDataSet(sessionQueryDataSet));
    int n = sessionQueryDataSet.getKeys().length;
    int m = sessionQueryDataSet.getPaths().size();
    int datapoints = 0;
    for (int i = 0; i < n; i++) {
      boolean flag = false;
      for (int j = 0; j < m; j++) {
        if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
          if (!flag) {
            queryResultDataset.add(
                sessionQueryDataSet.getKeys()[i], sessionQueryDataSet.getValues().get(i).get(j));
            flag = true;
            ins.addKey(sessionQueryDataSet.getKeys()[i]);
            ins.addValue(sessionQueryDataSet.getValues().get(i).get(j).toString());
          }
          datapoints += 1;
        }
      }
    }
    queryResultDataset.setSampleSize(datapoints);
    metrics.add(ins);
    parser.setMetricList(metrics);
    parser.sendData();

    return queryResultDataset;
  }
}
