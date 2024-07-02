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
package cn.edu.tsinghua.iginx.rest;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.CLEAR_DUMMY_DATA_CAUTION;
import static cn.edu.tsinghua.iginx.rest.RestUtils.*;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.exception.StatementExecutionException;
import cn.edu.tsinghua.iginx.rest.bean.*;
import cn.edu.tsinghua.iginx.rest.insert.InsertWorker;
import cn.edu.tsinghua.iginx.rest.query.QueryExecutor;
import cn.edu.tsinghua.iginx.rest.query.QueryParser;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
public class MetricsResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsResource.class);

  private static final String INSERT_URL = "api/v1/datapoints";
  private static final String INSERT_ANNOTATION_URL = "api/v1/datapoints/annotations";
  private static final String ADD_ANNOTATION_URL = "api/v1/datapoints/annotations/add";
  private static final String UPDATE_ANNOTATION_URL = "api/v1/datapoints/annotations/update";
  private static final String QUERY_URL = "api/v1/datapoints/query";
  private static final String QUERY_ANNOTATION_URL = "api/v1/datapoints/query/annotations";
  private static final String QUERY_ANNOTATION_DATA_URL =
      "api/v1/datapoints/query/annotations/data";
  private static final String DELETE_URL = "api/v1/datapoints/delete";
  private static final String DELETE_ANNOTATION_URL = "api/v1/datapoints/annotations/delete";
  private static final String DELETE_METRIC_URL = "api/v1/metric/{metricName}";
  private static final String GRAFANA_OK = "";
  private static final String GRAFANA_QUERY = "query";
  private static final String GRAFANA_STRING = "annotations";
  private static final String ERROR_PATH = "{string : .+}";

  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final ExecutorService threadPool =
      Executors.newFixedThreadPool(config.getAsyncRestThreadPool());

  @Inject
  public MetricsResource() {}

  @GET
  @Path(GRAFANA_OK)
  public Response OK() {
    return setHeaders(Response.status(Status.OK)).build();
  }

  @POST
  @Path(GRAFANA_QUERY)
  public Response grafanaQuery(String jsonStr) {
    try {
      if (jsonStr == null) {
        throw new Exception("query json must not be null or empty");
      }
      QueryParser parser = new QueryParser();
      Query query = parser.parseGrafanaQueryMetric(jsonStr);
      QueryExecutor executor = new QueryExecutor(query);
      QueryResult result = executor.execute(false);
      String entity = parser.parseResultToGrafanaJson(result);
      return setHeaders(Response.status(Status.OK).entity(entity + "\n")).build();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      return setHeaders(
              Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n"))
          .build();
    }
  }

  @POST
  @Path(INSERT_ANNOTATION_URL)
  public void insertAnnotation(
      @Context HttpHeaders httpheaders,
      final InputStream stream,
      @Suspended final AsyncResponse asyncResponse) {
    threadPool.execute(new InsertWorker(asyncResponse, httpheaders, stream, false));
  }

  @POST
  @Path(ADD_ANNOTATION_URL)
  public Response addAnnotation(
      @Context HttpHeaders httpheaders,
      final InputStream stream,
      @Suspended final AsyncResponse asyncResponse) {
    try {
      String str = inputStreamToString(stream);
      appendAnno(str, httpheaders, asyncResponse);
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      return setHeaders(
              Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n"))
          .build();
    }
    return setHeaders(Response.status(Status.OK).entity("add annotation OK")).build();
  }

  @POST
  @Path(UPDATE_ANNOTATION_URL)
  public Response updateAnnotation(
      @Context HttpHeaders httpheaders,
      final InputStream stream,
      @Suspended final AsyncResponse asyncResponse) {
    try {
      String str = inputStreamToString(stream);
      updateAnno(str, httpheaders, asyncResponse);
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      return setHeaders(Response.status(Status.BAD_REQUEST).entity(e.toString().trim())).build();
    }
    return setHeaders(Response.status(Status.OK).entity("update annotation OK")).build();
  }

  @POST
  @Path(GRAFANA_STRING)
  public Response grafanaAnnotation(String jsonStr) {
    try {
      return postQuery(jsonStr, true, false, true);
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      return setHeaders(
              Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n"))
          .build();
    }
  }

  @POST
  @Path(QUERY_ANNOTATION_URL)
  public Response queryAnnotation(String jsonStr) {
    try {
      return postQuery(jsonStr, true, false, false);
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      return setHeaders(
              Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n"))
          .build();
    }
  }

  @POST
  @Path(QUERY_ANNOTATION_DATA_URL)
  public Response queryAnnotationData(String jsonStr) {
    try {
      return postQuery(jsonStr, true, true, false);
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      return setHeaders(
              Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n"))
          .build();
    }
  }

  @POST
  @Path(ERROR_PATH)
  public Response postErrorPath(@PathParam("string") String str) {
    return setHeaders(Response.status(Status.NOT_FOUND).entity("Wrong path\n")).build();
  }

  @GET
  @Path(ERROR_PATH)
  public Response getErrorPath(@PathParam("string") String str) {
    return setHeaders(Response.status(Status.NOT_FOUND).entity("Wrong path\n")).build();
  }

  @POST
  @Path(INSERT_URL)
  public void add(
      @Context HttpHeaders httpheaders,
      final InputStream stream,
      @Suspended final AsyncResponse asyncResponse) {
    threadPool.execute(new InsertWorker(asyncResponse, httpheaders, stream, false));
  }

  @POST
  @Path(QUERY_URL)
  public Response postQuery(final InputStream stream) {
    try {
      String str = inputStreamToString(stream);
      return postQuery(str, false, false, false);
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      return setHeaders(
              Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n"))
          .build();
    }
  }

  @POST
  @Path(DELETE_URL)
  public Response postDelete(final InputStream stream) {
    try {
      String str = inputStreamToString(stream);
      return postDelete(str);
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      return setHeaders(
              Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n"))
          .build();
    }
  }

  @POST
  @Path(DELETE_ANNOTATION_URL)
  public Response postDeleteAnno(final InputStream stream) {
    try {
      String str = inputStreamToString(stream);
      return postAnnoDelete(str);
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      return setHeaders(
              Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n"))
          .build();
    }
  }

  @DELETE
  @Path(DELETE_METRIC_URL)
  public Response metricDelete(@PathParam("metricName") String metricName) {
    try {
      deleteMetric(metricName);
      return setHeaders(Response.status(Status.OK)).build();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      return setHeaders(
              Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n"))
          .build();
    }
  }

  static Response.ResponseBuilder setHeaders(Response.ResponseBuilder responseBuilder) {
    responseBuilder.header("Access-Control-Allow-Origin", "*");
    responseBuilder.header("Access-Control-Allow-Methods", "POST");
    responseBuilder.header("Access-Control-Allow-Headers", "accept, content-type");
    responseBuilder.header("Pragma", "no-cache");
    responseBuilder.header("Cache-Control", "no-cache");
    responseBuilder.header("Expires", 0);
    return (responseBuilder);
  }

  private static String inputStreamToString(InputStream inputStream) throws Exception {
    StringBuilder buffer = new StringBuilder();
    InputStreamReader inputStreamReader =
        new InputStreamReader(inputStream, StandardCharsets.UTF_8);
    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
    String str;
    while ((str = bufferedReader.readLine()) != null) {
      buffer.append(str);
    }
    bufferedReader.close();
    inputStreamReader.close();
    inputStream.close();
    return buffer.toString();
  }

  private QueryResult normalQuery(Query query) throws Exception {
    QueryExecutor executor = new QueryExecutor(query);
    return executor.execute(false);
  }

  private QueryResult annoDataQuery(Query query, QueryParser parser) throws Exception {
    QueryExecutor executor = new QueryExecutor(null);
    // 调用SHOW COLUMNS
    QueryResult columns = executor.executeShowColumns();
    // 筛选路径信息，正常信息路径，生成单个query
    Query queryAnnoData = getAnnoDataQueryFromColumns(query, columns);
    // 先查询title信息
    // 查询anno的title以及dsp信息
    queryAnnoData.setTimePrecision(TimePrecision.NS);
    QueryResult resultAnno = getAnno(queryAnnoData, 1L, MAX_KEY);

    // 添加cat信息
    parser.getAnnoCategory(resultAnno);
    // 筛选出符合title信息的序列
    return getAnnoDataQueryFromTitle(resultAnno);
  }

  private QueryResult annoQuery(QueryParser parser, String jsonStr) throws Exception {
    // 查找出所有符合tagkv的序列路径
    Query queryBase = parser.parseAnnotationQueryMetric(jsonStr, false);

    // 通过first聚合查询，先查找出所以路径集合
    queryBase.addFirstAggregator();
    queryBase.setStartAbsolute(1L);
    queryBase.setEndAbsolute(TOP_KEY);
    queryBase.setTimePrecision(TimePrecision.NS);
    QueryExecutor executorPath = new QueryExecutor(queryBase);
    QueryResult resultPath = executorPath.execute(false);

    // 重新组织路径，将聚合查询符号删除
    parser.removeAggPath(resultPath);
    // 分离cat路径信息
    Query queryAnno = parser.splitAnnoPathToQuery(resultPath);

    // 查询anno的title以及dsp信息
    queryAnno.setTimePrecision(TimePrecision.NS);
    return getAnno(queryAnno, DESCRIPTION_KEY, MAX_KEY);
  }

  private Response postQuery(
      String jsonStr, boolean isAnnotation, boolean isAnnoData, boolean isGrafana) {
    try {
      if (jsonStr == null) {
        throw new Exception("query json must not be null or empty");
      }
      QueryParser parser = new QueryParser();
      String entity;
      Query query =
          isAnnotation
              ? parser.parseAnnotationQueryMetric(jsonStr, isGrafana)
              : parser.parseQueryMetric(jsonStr);
      if (!isAnnotation) {
        QueryResult result = normalQuery(query);
        entity = parser.parseResultToJson(result, false);
      } else if (isAnnoData) {
        QueryResult result = annoDataQuery(query, parser);
        entity = parser.parseAnnoDataResultToJson(result);
      } else { // 只查询anno信息
        QueryResult result = annoQuery(parser, jsonStr);
        parser.getAnnoCategory(result);
        entity = parser.parseAnnoResultToJson(result);
      }
      return setHeaders(Response.status(Status.OK).entity(entity + "\n")).build();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      return setHeaders(
              Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n"))
          .build();
    }
  }

  // 传入queryAnno信息，即要查找的序列信息，返回anno信息
  private QueryResult getAnno(Query queryAnno, long startKey, long endKey) throws Exception {
    // 查找title以及description信息
    queryAnno.setStartAbsolute(startKey); // LHZ这里可以不用查出所有数据，可以优化
    queryAnno.setEndAbsolute(endKey);
    QueryExecutor executorAnno = new QueryExecutor(queryAnno); // 要确认下是否annotation信息在查找时会影响结果
    QueryResult resultAnno = executorAnno.execute(false);

    try {
      executorAnno.queryAnno(resultAnno);
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      throw e;
    }
    return resultAnno;
  }

  private QueryResult getAnnoDataQueryFromTitle(QueryResult result) {
    QueryResult ret = new QueryResult();
    int len = result.getQueryResultDatasets().size();
    for (int i = 0; i < len; i++) {
      QueryResultDataset dataRet = new QueryResultDataset();
      QueryResultDataset data = result.getQueryResultDatasets().get(i);
      String title = result.getQueryMetrics().get(i).getAnnotationLimit().getTitle();
      for (int j = 0; j < data.getPaths().size(); j++) {
        if (title.equals(".*")
            || title.isEmpty()
            || (data.getTitles().get(j) != null && data.getTitles().get(j).equals(title))) {
          dataRet.addPath(data.getPaths().get(j));
          if (!data.getDescriptions().isEmpty()) {
            dataRet.addDescription(data.getDescriptions().get(j));
          }
          if (!data.getCategoryLists().isEmpty()) {
            dataRet.addCategory(data.getCategoryLists().get(j));
          }
          if (!data.getTitles().isEmpty()) {
            dataRet.addTitle(data.getTitles().get(j));
          }
          if (!data.getValueLists().isEmpty()) {
            dataRet.addValueLists(data.getValueLists().get(j));
          }
          if (!data.getKeyLists().isEmpty()) {
            dataRet.addKeyLists(data.getKeyLists().get(j));
          }
        }
      }
      ret.addQueryResultDataset(dataRet);
      ret.addQueryMetric(result.getQueryMetrics().get(i));
      ret.addQueryAggregator(result.getQueryAggregators().get(i));
    }
    return ret;
  }

  private Query getAnnoDataQueryFromColumns(Query query, QueryResult result) {
    Query ret = new Query();
    QueryParser parser = new QueryParser();
    // 筛选出符合全部anno信息的路径
    for (int i = 0; i < query.getQueryMetrics().size(); i++) {
      List<String> paths = new ArrayList<>();
      for (int j = 0; j < result.getQueryResultDatasets().size(); j++) {
        QueryResultDataset data = result.getQueryResultDatasets().get(j);
        paths =
            parser.getPrefixPaths(
                query.getQueryMetrics().get(i).getAnnotationLimit().getTag(), data.getPaths());
      }
      for (String pathStr : paths) {
        QueryMetric metric = parser.parseQueryResultAnnoDataPaths(pathStr);
        metric.setQueryOriPath(pathStr);
        metric.setAnnotationLimit(query.getQueryMetrics().get(i).getAnnotationLimit());
        ret.addQueryMetrics(metric);
      }
    }
    return ret;
  }

  private Response postAnnoDelete(String jsonStr) {
    try {
      // 查找出所有符合tagkv的序列路径
      QueryParser parser = new QueryParser();
      Query queryBase = parser.parseAnnotationQueryMetric(jsonStr, false);

      // 加入category路径信息
      Query querySp = parser.addAnnoTags(queryBase);
      querySp.setStartAbsolute(1L);
      querySp.setEndAbsolute(TOP_KEY);
      querySp.setTimePrecision(TimePrecision.NS);
      QueryExecutor executorPath = new QueryExecutor(querySp);
      QueryResult resultAll = executorPath.execute(false);

      // 路径筛选，这里代码的意义是为了之后的拓展
      Query queryAll = parser.getSpecificQuery(resultAll);
      queryAll.setStartAbsolute(1L);
      queryAll.setEndAbsolute(TOP_KEY);
      queryAll.setTimePrecision(TimePrecision.NS);

      // 空查询判断
      if (queryAll.getQueryMetrics().isEmpty()) {
        return setHeaders(Response.status(Status.OK).entity("\n")).build();
      }

      QueryExecutor executorData = new QueryExecutor(queryAll);
      // 执行删除操作
      executorData.deleteMetric();

      String entity = parser.parseResultToJson(null, true);
      return setHeaders(Response.status(Status.OK).entity(entity + "\n")).build();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      return setHeaders(Response.status(Status.BAD_REQUEST).entity(e.toString().trim())).build();
    }
  }

  private Response postDelete(String jsonStr) {
    try {
      if (jsonStr == null) {
        throw new Exception("query json must not be null or empty");
      }
      QueryParser parser = new QueryParser();
      Query query = parser.parseQueryMetric(jsonStr);
      QueryExecutor executor = new QueryExecutor(query);
      QueryResult result = executor.execute(true);
      String entity = parser.parseResultToJson(result, true);
      return setHeaders(Response.status(Status.OK).entity(entity + "\n")).build();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      return setHeaders(
              Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n"))
          .build();
    }
  }

  private void deleteMetric(String metricName) throws Exception {
    RestSession restSession = new RestSession();
    restSession.openSession();
    List<String> ins = new ArrayList<>();
    ins.add(metricName);
    restSession.deleteColumns(ins, new ArrayList<>());
    restSession.closeSession();
  }

  private void appendAnno(String jsonStr, HttpHeaders httpheaders, AsyncResponse asyncResponse)
      throws Exception {
    // 查找出所有符合tagkv的序列路径
    QueryParser parser = new QueryParser();
    // 包含时间范围的查询
    Query queryBase = parser.parseAnnotationQueryMetric(jsonStr, false);
    QueryExecutor executorPath = new QueryExecutor(queryBase);
    QueryResult result = executorPath.execute(false);

    // 修改路径，得到所有的单条路径信息，便于之后操作
    Query queryAll = parser.splitPath(result);
    queryAll.setStartAbsolute(queryBase.getStartAbsolute());
    queryAll.setEndAbsolute(queryBase.getEndAbsolute());

    // 执行删除操作,还是要先执行删除，因为之后执行删除，可能会删除已经插入的序列值
    //        QueryExecutor executorData = new QueryExecutor(queryAll);
    //        executorData.execute(true);

    // 修改路径，并重新查询数据，并插入数据
    threadPool.execute(new InsertWorker(asyncResponse, httpheaders, result, queryBase, true));
  }

  private void updateAnno(String jsonStr, HttpHeaders httpheaders, AsyncResponse asyncResponse)
      throws Exception {
    // 查找出所有符合tagkv的序列路径
    QueryParser parser = new QueryParser();
    Query queryBase = parser.parseAnnotationQueryMetric(jsonStr, false);

    // 加入category路径信息
    Query querySp = parser.addAnnoTags(queryBase);
    querySp.setStartAbsolute(1L);
    querySp.setEndAbsolute(TOP_KEY);
    querySp.setTimePrecision(TimePrecision.NS);
    QueryExecutor executorPath = new QueryExecutor(querySp);
    QueryResult resultAll = executorPath.execute(false);

    // 路径筛选，这里代码的意义是为了之后的拓展
    Query queryAll = parser.getSpecificQuery(resultAll);
    queryAll.setStartAbsolute(1L);
    queryAll.setEndAbsolute(TOP_KEY);
    queryAll.setTimePrecision(TimePrecision.NS);

    QueryExecutor executorData = new QueryExecutor(queryAll);
    // 执行删除操作
    Exception exception = null;
    boolean cautionDuringDelete = false;
    try {
      executorData.deleteMetric();
    } catch (StatementExecutionException e) {
      if (e.toString().trim().contains(CLEAR_DUMMY_DATA_CAUTION)) {
        exception = e;
        cautionDuringDelete = true;
        LOGGER.warn("cant delete the READ_ONLY data and go on.");
      } else {
        LOGGER.error("Error occurred during executing", e);
        throw e;
      }
    }

    // 修改路径，并重新查询数据，并插入数据
    threadPool.execute(new InsertWorker(asyncResponse, httpheaders, resultAll, querySp, false));

    if (cautionDuringDelete) {
      throw exception;
    }
  }
}
