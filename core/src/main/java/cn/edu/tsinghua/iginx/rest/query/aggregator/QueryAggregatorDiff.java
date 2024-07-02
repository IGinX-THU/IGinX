/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
