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

public class QueryAggregatorFilter extends QueryAggregator {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryAggregatorFilter.class);

  public QueryAggregatorFilter() {
    super(QueryAggregatorType.FILTER);
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
      int datapoints = 0;
      switch (type) {
        case LONG:
          for (int i = 0; i < n; i++) {
            Long now = null;
            for (int j = 0; j < m; j++) {
              if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
                if (now == null
                    && filted((long) sessionQueryDataSet.getValues().get(i).get(j), getFilter())) {
                  now = (long) sessionQueryDataSet.getValues().get(i).get(j);
                }
                datapoints += 1;
              }
            }
            if (now != null) {
              queryResultDataset.add(sessionQueryDataSet.getKeys()[i], now);
            }
          }
          queryResultDataset.setSampleSize(datapoints);
          break;
        case DOUBLE:
          for (int i = 0; i < n; i++) {
            Double nowd = null;
            for (int j = 0; j < m; j++) {
              if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
                if (nowd == null
                    && filted(
                        (double) sessionQueryDataSet.getValues().get(i).get(j), getFilter())) {
                  nowd = (double) sessionQueryDataSet.getValues().get(i).get(j);
                }
                datapoints += 1;
              }
            }
            if (nowd != null) {
              queryResultDataset.add(sessionQueryDataSet.getKeys()[i], nowd);
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

  boolean filted(double now, Filter filter) {
    switch (filter.getOp()) {
      case "gt":
        return now > filter.getValue();
      case "gte":
        return now >= filter.getValue();
      case "eq":
        return now == filter.getValue();
      case "ne":
        return now != filter.getValue();
      case "lte":
        return now <= filter.getValue();
      case "lt":
        return now < filter.getValue();
      default:
        return false;
    }
  }
}
