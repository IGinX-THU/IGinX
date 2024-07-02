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
import cn.edu.tsinghua.iginx.rest.bean.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class QueryAggregator {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryAggregator.class);
  private Double divisor;
  private Long dur;
  private double percentile;
  private long unit;
  private String metric_name;
  private Filter filter;
  private QueryAggregatorType type;
  private AggregateType aggregateType;

  protected QueryAggregator(QueryAggregatorType type) {
    this(type, null);
  }

  protected QueryAggregator(QueryAggregatorType type, AggregateType aggregateType) {
    this.type = type;
    this.aggregateType = aggregateType;
  }

  public Double getDivisor() {
    return divisor;
  }

  public void setDivisor(Double divisor) {
    this.divisor = divisor;
  }

  public Long getDur() {
    return dur;
  }

  public void setDur(Long dur) {
    this.dur = dur;
  }

  public double getPercentile() {
    return percentile;
  }

  public void setPercentile(double percentile) {
    this.percentile = percentile;
  }

  public long getUnit() {
    return unit;
  }

  public void setUnit(long unit) {
    this.unit = unit;
  }

  public String getMetric_name() {
    return metric_name;
  }

  public void setMetric_name(String metric_name) {
    this.metric_name = metric_name;
  }

  public Filter getFilter() {
    return filter;
  }

  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  public QueryAggregatorType getType() {
    return type;
  }

  public void setType(QueryAggregatorType type) {
    this.type = type;
  }

  public QueryResultDataset doAggregate(
      RestSession session,
      List<String> paths,
      List<Map<String, List<String>>> tagList,
      long startKey,
      long endKey) {
    return doAggregate(
        session, paths, tagList, startKey, endKey, TimeUtils.DEFAULT_TIMESTAMP_PRECISION);
  }

  public QueryResultDataset doAggregate(
      RestSession session,
      List<String> paths,
      List<Map<String, List<String>>> tagList,
      long startKey,
      long endKey,
      TimePrecision timePrecision) {
    SessionQueryDataSet sessionQueryDataSet = null;
    try {
      if (type == QueryAggregatorType.NONE) {
        sessionQueryDataSet = session.queryData(paths, startKey, endKey, tagList, timePrecision);
      } else if (aggregateType != null) {
        sessionQueryDataSet =
            session.downsampleQuery(
                paths, tagList, startKey, endKey, aggregateType, getDur(), timePrecision);
      }
    } catch (Exception e) {
      // TODO: more precise exception catch
      LOGGER.error("unexpected error: ", e);
      return new QueryResultDataset();
    }
    if (sessionQueryDataSet == null) {
      // type != QueryAggregatorType.NONE) and aggregateType == null
      throw new RuntimeException("Unsupported Query");
    }
    QueryResultDataset queryResultDataset = new QueryResultDataset();
    queryResultDataset.setPaths(getPathsFromSessionQueryDataSet(sessionQueryDataSet));
    int n = sessionQueryDataSet.getKeys().length;
    int m = 0;
    if (sessionQueryDataSet.getPaths() != null) {
      m = sessionQueryDataSet.getPaths().size();
    }
    int datapoints = 0;
    for (int j = 0; j < m; j++) {
      List<Object> value = new ArrayList<>();
      List<Long> time = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
          value.add(sessionQueryDataSet.getValues().get(i).get(j));
          long timeRes = sessionQueryDataSet.getKeys()[i];
          time.add(timeRes);
          queryResultDataset.add(timeRes, sessionQueryDataSet.getValues().get(i).get(j));
          datapoints += 1;
        }
      }
      queryResultDataset.addValueLists(value);
      queryResultDataset.addKeyLists(time);
    }
    queryResultDataset.setSampleSize(datapoints);
    return queryResultDataset;
  }

  public List<String> getPathsFromSessionQueryDataSet(SessionQueryDataSet sessionQueryDataSet) {
    List<String> ret = new ArrayList<>();
    List<Boolean> notNull = new ArrayList<>();
    int n = sessionQueryDataSet.getKeys().length;
    int m = 0;
    if (sessionQueryDataSet.getPaths() != null) {
      m = sessionQueryDataSet.getPaths().size();
    }
    for (int i = 0; i < m; i++) {
      notNull.add(false);
    }
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < m; j++) {
        if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
          notNull.set(j, true);
        }
      }
    }
    for (int i = 0; i < m; i++) {
      if (notNull.get(i)) {
        ret.add(sessionQueryDataSet.getPaths().get(i));
      }
    }
    return ret;
  }

  public List<String> getPathsFromShowColumns(SessionQueryDataSet sessionQueryDataSet) {
    List<String> ret = new ArrayList<>();
    List<Boolean> notNull = new ArrayList<>();
    int m = sessionQueryDataSet.getPaths().size();
    for (int i = 0; i < m; i++) {
      notNull.add(false);
    }
    for (int i = 0; i < m; i++) {
      ret.add(sessionQueryDataSet.getPaths().get(i));
    }
    return ret;
  }
}
