package cn.edu.tsinghua.iginx.rest.query;

import static cn.edu.tsinghua.iginx.rest.RestUtils.*;

import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.bean.Query;
import cn.edu.tsinghua.iginx.rest.bean.QueryMetric;
import cn.edu.tsinghua.iginx.rest.bean.QueryResult;
import cn.edu.tsinghua.iginx.rest.bean.QueryResultDataset;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregator;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorNone;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryShowColumns;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryExecutor {
  public static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);
  private Query query;

  private final RestSession session = new RestSession();

  public QueryExecutor(Query query) {
    this.query = query;
  }

  public QueryResult executeShowColumns() throws Exception {
    QueryResult ret = new QueryResult();
    try {
      session.openSession();
      ret.addResultSet(new QueryShowColumns().doAggregate(session));
      session.closeSession();
    } catch (Exception e) {
      LOGGER.error("Error occurred during executing", e);
      throw e;
    }
    return ret;
  }

  public QueryResult execute(boolean isDelete) throws Exception {
    QueryResult ret = new QueryResult();
    try {
      session.openSession();
      for (QueryMetric queryMetric : query.getQueryMetrics()) {
        List<String> paths = new ArrayList<>();
        paths.add(queryMetric.getName());
        if (isDelete) {
          RestSession session = new RestSession();
          session.openSession();
          session.deleteDataInColumns(
              paths,
              queryMetric.getTags(),
              query.getStartAbsolute(),
              query.getEndAbsolute(),
              query.getTimePrecision());
          session.closeSession();
        } else if (queryMetric.getAggregators().isEmpty()) {
          ret.addResultSet(
              new QueryAggregatorNone()
                  .doAggregate(
                      session,
                      paths,
                      queryMetric.getTags(),
                      query.getStartAbsolute(),
                      query.getEndAbsolute(),
                      query.getTimePrecision()),
              queryMetric,
              new QueryAggregatorNone());
        } else {
          for (QueryAggregator queryAggregator : queryMetric.getAggregators()) {
            ret.addResultSet(
                queryAggregator.doAggregate(
                    session,
                    paths,
                    queryMetric.getTags(),
                    query.getStartAbsolute(),
                    query.getEndAbsolute(),
                    query.getTimePrecision()),
                queryMetric,
                queryAggregator);
          }
        }
      }
      session.closeSession();
    } catch (Exception e) {
      LOGGER.error("Error occurred during executing", e);
      throw e;
    }

    return ret;
  }

  private String getStringFromObject(Object val) {
    String valStr;
    if (val instanceof byte[]) {
      valStr = new String((byte[]) val);
    } else {
      valStr = String.valueOf(val.toString());
    }
    return valStr;
  }

  // 结果通过引用传出
  public void queryAnno(QueryResult anno) throws Exception {
    QueryResult title, description;
    Query titleQuery = new Query();
    Query descriptionQuery = new Query();
    boolean hasTitle = false, hasDescription = false;
    try {
      for (int i = 0; i < anno.getQueryResultDatasets().size(); i++) {
        QueryResultDataset data = anno.getQueryResultDatasets().get(i);
        if (data.getKeyLists().isEmpty()) continue;
        int subLen = data.getKeyLists().size();
        for (int j = 0; j < subLen; j++) {
          List<Long> timeList = data.getKeyLists().get(j);
          for (int z = timeList.size() - 1; z >= 0; z--) {
            // 这里减小了对时间查询的范围
            if (timeList.get(z) < DESCRIPTION_KEY) break;

            // 将多种类型转换为Long
            Long annoTime = getLongVal(data.getValueLists().get(j).get(z));

            if (timeList.get(z).equals(TITLE_KEY)) {
              hasTitle = true;
              titleQuery.setStartAbsolute(annoTime);
              titleQuery.setEndAbsolute(annoTime + 1L);
            } else if (timeList.get(z).equals(DESCRIPTION_KEY)) {
              hasDescription = true;
              descriptionQuery.setStartAbsolute(annoTime);
              descriptionQuery.setEndAbsolute(annoTime + 1L);
            }
          }

          QueryMetric metric = new QueryMetric();
          metric.setName(ANNOTATION_SEQUENCE);
          List<QueryMetric> metrics = new ArrayList<>();
          metrics.add(metric);
          if (hasTitle) {
            titleQuery.setQueryMetrics(metrics);
            titleQuery.setTimePrecision(TimePrecision.NS);
            this.query = titleQuery;
            title = execute(false);
            anno.getQueryResultDatasets()
                .get(i)
                .addTitle(
                    getStringFromObject(title.getQueryResultDatasets().get(0).getValues().get(0)));
          } else {
            anno.getQueryResultDatasets().get(i).addTitle(new String());
          }
          if (hasDescription) {
            descriptionQuery.setQueryMetrics(metrics);
            descriptionQuery.setTimePrecision(TimePrecision.NS);
            this.query = descriptionQuery;
            description = execute(false);
            anno.getQueryResultDatasets()
                .get(i)
                .addDescription(
                    getStringFromObject(
                        description.getQueryResultDatasets().get(0).getValues().get(0)));
          } else {
            anno.getQueryResultDatasets().get(i).addDescription(new String());
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Error occurred during executing", e);
      throw e;
    }
  }

  public void deleteMetric() throws Exception {
    RestSession restSession = new RestSession();
    restSession.openSession();
    for (QueryMetric metric : query.getQueryMetrics()) {
      restSession.deleteColumn(metric.getName(), metric.getTags());
    }
    restSession.closeSession();
  }

  // 错误返回-1
  public Long getLongVal(Object val) {
    switch (judgeObjectType(val)) {
      case BINARY:
        return Long.valueOf(new String((byte[]) val));
      case DOUBLE:
        return Math.round((Double) (val));
      case LONG:
        return (Long) val;
      default:
        return -1L; // 尽量不要传null
    }
  }
}
