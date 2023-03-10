/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.rest.bean.*;
import cn.edu.tsinghua.iginx.rest.insert.InsertWorker;
import cn.edu.tsinghua.iginx.rest.query.QueryExecutor;
import cn.edu.tsinghua.iginx.rest.query.QueryParser;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static cn.edu.tsinghua.iginx.rest.RestUtils.*;

@Path("/")
public class MetricsResource {

    private static final String INSERT_URL = "api/v1/datapoints";
    private static final String INSERT_ANNOTATION_URL = "api/v1/datapoints/annotations";
    private static final String ADD_ANNOTATION_URL = "api/v1/datapoints/annotations/add";
    private static final String UPDATE_ANNOTATION_URL = "api/v1/datapoints/annotations/update";
    private static final String QUERY_URL = "api/v1/datapoints/query";
    private static final String QUERY_ANNOTATION_URL = "api/v1/datapoints/query/annotations";
    private static final String QUERY_ANNOTATION_DATA_URL = "api/v1/datapoints/query/annotations/data";
    private static final String DELETE_URL = "api/v1/datapoints/delete";
    private static final String DELETE_ANNOTATION_URL = "api/v1/datapoints/annotations/delete";
    private static final String DELETE_METRIC_URL = "api/v1/metric/{metricName}";
    private static final String GRAFANA_OK = "";
    private static final String GRAFANA_QUERY = "query";
    private static final String GRAFANA_STRING = "annotations";
    private static final String ERROR_PATH = "{string : .+}";

    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsResource.class);
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(config.getAsyncRestThreadPool());

    @Inject
    public MetricsResource() {
    }

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
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
        }
    }

    @POST
    @Path(INSERT_ANNOTATION_URL)
    public void insertAnnotation(@Context HttpHeaders httpheaders, final InputStream stream, @Suspended final AsyncResponse asyncResponse) {
        threadPool.execute(new InsertWorker(asyncResponse, httpheaders, stream, false));
    }

    @POST
    @Path(ADD_ANNOTATION_URL)
    public Response addAnnotation(@Context HttpHeaders httpheaders, final InputStream stream, @Suspended final AsyncResponse asyncResponse) {
        try {
            String str = inputStreamToString(stream);
            appendAnno(str, httpheaders, asyncResponse);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
        }
        return setHeaders(Response.status(Status.OK).entity("add annotation OK")).build();
    }

    @POST
    @Path(UPDATE_ANNOTATION_URL)
    public Response updateAnnotation(@Context HttpHeaders httpheaders, final InputStream stream, @Suspended final AsyncResponse asyncResponse) {
        try {
            String str = inputStreamToString(stream);
            updateAnno(str, httpheaders, asyncResponse);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
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
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
        }
    }

    @POST
    @Path(QUERY_ANNOTATION_URL)
    public Response queryAnnotation(String jsonStr) {
        try {
            return postQuery(jsonStr, true, false, false);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
        }
    }

    @POST
    @Path(QUERY_ANNOTATION_DATA_URL)
    public Response queryAnnotationData(String jsonStr) {
        try {
            return postQuery(jsonStr, true, true, false);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
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
    public void add(@Context HttpHeaders httpheaders, final InputStream stream, @Suspended final AsyncResponse asyncResponse) {
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
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
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
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
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
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
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
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
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
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
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
        //??????show time series
        QueryResult timeSeries = executor.executeShowTimeSeries();
        //??????????????????????????????????????????????????????query
        Query queryAnnoData = getAnnoDataQueryFromTimeSeries(query, timeSeries);
        //?????????title??????
        //??????anno???title??????dsp??????
        queryAnnoData.setTimePrecision(TimePrecision.NS);
        QueryResult resultAnno = getAnno(queryAnnoData,1L,MAXTIEM);

        //??????cat??????
        parser.getAnnoCategory(resultAnno);
        //???????????????title???????????????
        return getAnnoDataQueryFromTitle(query, resultAnno);
    }

    private QueryResult annoQuery(Query query, QueryParser parser, String jsonStr) throws Exception {
        //?????????????????????tagkv???????????????
        Query queryBase = parser.parseAnnotationQueryMetric(jsonStr, false);
        Query queryAnno = new Query();

        //??????first?????????????????????????????????????????????
        queryBase.addFirstAggregator();
        queryBase.setStartAbsolute(1L);
        queryBase.setEndAbsolute(TOPTIEM);
        queryBase.setTimePrecision(TimePrecision.NS);
        QueryExecutor executorPath = new QueryExecutor(queryBase);
        QueryResult resultPath = executorPath.execute(false);

        //????????????????????????????????????????????????
        parser.removeAggPath(resultPath);
        //??????cat????????????
        queryAnno = parser.splitAnnoPathToQuery(resultPath);

        //??????anno???title??????dsp??????
        queryAnno.setTimePrecision(TimePrecision.NS);
        return getAnno(queryAnno,DESCRIPTIONTIEM,MAXTIEM);
    }

    public Response postQuery(String jsonStr, boolean isAnnotation, boolean isAnnoData, boolean isGrafana) {
        try {
            if (jsonStr == null) {
                throw new Exception("query json must not be null or empty");
            }
            QueryParser parser = new QueryParser();
            String entity = new String();
            Query query = isAnnotation ? parser.parseAnnotationQueryMetric(jsonStr, isGrafana) : parser.parseQueryMetric(jsonStr);
            if (!isAnnotation) {
                QueryResult result = normalQuery(query);
                entity = parser.parseResultToJson(result, false);
            } else if (isAnnoData) {
                QueryResult result = annoDataQuery(query, parser);
                entity = parser.parseAnnoDataResultToJson(result);
            } else {//?????????anno??????
                QueryResult result = annoQuery(query, parser, jsonStr);
                parser.getAnnoCategory(result);
                entity = parser.parseAnnoResultToJson(result);
            }
            return setHeaders(Response.status(Status.OK).entity(entity + "\n")).build();
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
        }
    }

    //??????queryAnno?????????????????????????????????????????????anno??????
    private QueryResult getAnno(Query queryAnno, Long startTime, Long endTime) throws Exception  {
        //??????title??????description??????
        queryAnno.setStartAbsolute(startTime);//LHZ???????????????????????????????????????????????????
        queryAnno.setEndAbsolute(endTime);
        QueryExecutor executorAnno = new QueryExecutor(queryAnno);//??????????????????annotation?????????????????????????????????
        QueryResult resultAnno = executorAnno.execute(false);

        try {
            executorAnno.queryAnno(resultAnno);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            throw e;
        }
        return resultAnno;
    }

    public QueryResult getAnnoDataQueryFromTitle(Query query, QueryResult result) {
        QueryResult ret = new QueryResult();
        int len = result.getQueryResultDatasets().size();
        for(int i=0; i<len; i++) {
            QueryResultDataset dataRet = new QueryResultDataset();
            QueryResultDataset data = result.getQueryResultDatasets().get(i);
            String title = result.getQueryMetrics().get(i).getAnnotationLimit().getTitle();
            for(int j=0; j<data.getPaths().size(); j++) {
                if(title.equals(".*") || title.isEmpty() || (data.getTitles().get(j)!=null && data.getTitles().get(j).equals(title)) ) {
                    if(!data.getPaths().isEmpty()) dataRet.addPath(data.getPaths().get(j));
                    if(!data.getDescriptions().isEmpty()) dataRet.addDescription(data.getDescriptions().get(j));
                    if(!data.getCategorys().isEmpty()) dataRet.addCategory(data.getCategorys().get(j));
                    if(!data.getTitles().isEmpty()) dataRet.addTitle(data.getTitles().get(j));
                    if(!data.getValueLists().isEmpty()) dataRet.addValueLists(data.getValueLists().get(j));
                    if(!data.getTimeLists().isEmpty()) dataRet.addTimeLists(data.getTimeLists().get(j));
                }
            }
            ret.addqueryResultDataset(dataRet);
            ret.addQueryMetric(result.getQueryMetrics().get(i));
            ret.addQueryAggregator(result.getQueryAggregators().get(i));
        }
        return ret;
    }

    public Query getAnnoDataQueryFromTimeSeries(Query query, QueryResult result){
        Query ret = new Query();
        QueryParser parser = new QueryParser();
        //?????????????????????anno???????????????
        for(int i=0;i<query.getQueryMetrics().size();i++){
            List<String> paths = new ArrayList<>();
            for(int j=0; j<result.getQueryResultDatasets().size(); j++){
                QueryResultDataset data = result.getQueryResultDatasets().get(j);
                paths = parser.getPrefixPaths(query.getQueryMetrics().get(i).getAnnotationLimit().getTag(),data.getPaths());
            }
            for(String pathStr : paths){
                QueryMetric metric = new QueryMetric();
                metric = parser.parseQueryResultAnnoDataPaths(pathStr);
                metric.setQueryOriPath(pathStr);
                metric.setAnnotationLimit(query.getQueryMetrics().get(i).getAnnotationLimit());
                ret.addQueryMetrics(metric);
            }
        }
        return ret;
    }

    public Response postAnnoDelete(String jsonStr) {
        try {
            //?????????????????????tagkv???????????????
            QueryParser parser = new QueryParser();
            Query queryBase = parser.parseAnnotationQueryMetric(jsonStr, false);
            //??????category????????????
            Query querySp = parser.addAnnoTags(queryBase);
            querySp.setStartAbsolute(1L);
            querySp.setEndAbsolute(TOPTIEM);
            querySp.setTimePrecision(TimePrecision.NS);
            QueryExecutor executorPath = new QueryExecutor(querySp);
            QueryResult resultALL = executorPath.execute(false);

            //??????????????????????????????????????????????????????
//            DataPointsParser parserInsert = new DataPointsParser();
//            querySp.setNullNewAnno();
//            parserInsert.handleAnnotationUpdate(querySp, resultALL);

            //??????????????????
            Query queryAll = parser.getSpecificQuery(resultALL, queryBase);
            queryAll.setStartAbsolute(1L);
            queryAll.setEndAbsolute(TOPTIEM);
            queryAll.setTimePrecision(TimePrecision.NS);

            //???????????????
            if(queryAll.getQueryMetrics().isEmpty())
                return setHeaders(Response.status(Status.OK).entity("\n")).build();

            QueryExecutor executorData = new QueryExecutor(queryAll);
            //??????????????????
            executorData.deleteMetric();

            String entity = parser.parseResultToJson(null, true);
            return setHeaders(Response.status(Status.OK).entity(entity + "\n")).build();
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
        }
    }

    public Response postDelete(String jsonStr) {
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
            return setHeaders(Response.status(Status.BAD_REQUEST).entity("Error occurred during execution\n")).build();
        }
    }

    void deleteMetric(String metricName) throws Exception {
        RestSession restSession = new RestSession();
        restSession.openSession();
        List<String> ins = new ArrayList<>();
        ins.add(metricName);
        restSession.deleteColumns(ins);
        restSession.closeSession();
    }

    public void appendAnno(String jsonStr, HttpHeaders httpheaders, AsyncResponse asyncResponse) throws Exception {
        //?????????????????????tagkv???????????????
        QueryParser parser = new QueryParser();
        //???????????????????????????
        Query queryBase = parser.parseAnnotationQueryMetric(jsonStr, false);
        QueryExecutor executorPath = new QueryExecutor(queryBase);
        QueryResult result = executorPath.execute(false);

        //?????????????????????????????????????????????????????????????????????
        Query queryAll = parser.splitPath(result, queryBase);
        queryAll.setStartAbsolute(queryBase.getStartAbsolute());
        queryAll.setEndAbsolute(queryBase.getEndAbsolute());

        //??????????????????,?????????????????????????????????????????????????????????????????????????????????????????????
//        QueryExecutor executorData = new QueryExecutor(queryAll);
//        executorData.execute(true);

        //??????????????????????????????????????????????????????
        threadPool.execute(new InsertWorker(asyncResponse, httpheaders, result, queryBase, true));
    }

    public void updateAnno(String jsonStr, HttpHeaders httpheaders, AsyncResponse asyncResponse) throws Exception {
        //?????????????????????tagkv???????????????
        QueryParser parser = new QueryParser();
        Query queryBase = parser.parseAnnotationQueryMetric(jsonStr, false);
        //??????category????????????
        Query querySp = parser.addAnnoTags(queryBase);
        querySp.setStartAbsolute(1L);
        querySp.setEndAbsolute(TOPTIEM);
        querySp.setTimePrecision(TimePrecision.NS);
        QueryExecutor executorPath = new QueryExecutor(querySp);
        QueryResult resultALL = executorPath.execute(false);

        //????????????????????????????????????????????????????????????
        Query queryAll = parser.getSpecificQuery(resultALL, queryBase);
        queryAll.setStartAbsolute(1L);
        queryAll.setEndAbsolute(TOPTIEM);
        queryAll.setTimePrecision(TimePrecision.NS);
        QueryExecutor executorData = new QueryExecutor(queryAll);
        //??????????????????
        executorData.deleteMetric();

        //??????????????????????????????????????????????????????
        threadPool.execute(new InsertWorker(asyncResponse, httpheaders, resultALL, querySp, false));
    }
}