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
package cn.edu.tsinghua.iginx.rest.insert;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.bean.*;
import cn.edu.tsinghua.iginx.rest.query.QueryExecutor;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.rest.RestUtils;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.util.*;

import static cn.edu.tsinghua.iginx.rest.RestUtils.*;

public class DataPointsParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataPointsParser.class);
    private Reader inputStream = null;
    private final ObjectMapper mapper = new ObjectMapper();
    private List<Metric> metricList = new ArrayList<>();
    private final RestSession session = new RestSession();

    public DataPointsParser() {

    }

    public DataPointsParser(Reader stream) {
        this.inputStream = stream;
    }

    public void parse(boolean isAnnotation) throws Exception {
        try {
            session.openSession();
        } catch (SessionException e) {
            LOGGER.error("Error occurred during opening session", e);
            throw e;
        }
        try {
            JsonNode node = mapper.readTree(inputStream);
            if (node.isArray()) {
                for (JsonNode objNode : node) {
                    metricList.add(getMetricObject(objNode, isAnnotation));
                }
            } else {
                metricList.add(getMetricObject(node, isAnnotation));
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred during parsing data ", e);
            throw e;
        }
        try {
            sendMetricsData();
        } catch (Exception e) {
            LOGGER.debug("Exception occur for create and send ", e);
            throw e;
        } finally {
            session.closeSession();
        }
    }

    private boolean ifInputDataVaild(JsonNode node) {
        String name = node.get("name").toString();
        if (!name.contains(".")) {
            LOGGER.error("The input path should contains at least second order path");
            return false;
        }
        return true;
    }
    //?????????anno???????????????????????????????????????
    private Metric getMetricObject(JsonNode node, boolean isAnnotation) throws Exception {
        try {
            Metric ret = new Metric();
            if (!ifInputDataVaild(node)) {
                throw new Exception("The input correctness check is abnormal");
            }
            ret.setName(node.get("name").asText());
            Iterator<String> fieldNames = node.get("tags").fieldNames();
            Iterator<JsonNode> elements = node.get("tags").elements();
            //insert?????????tag???????????????val
            while (elements.hasNext() && fieldNames.hasNext()) {
                ret.addTag(fieldNames.next(), elements.next().textValue());
            }

            JsonNode tim = node.get("timestamp"), val = node.get("value");
            if (tim != null && val != null) {
                ret.addKey(tim.asLong());
                ret.addValue(val.asText());
            }
            JsonNode dp = node.get("datapoints");
            if (dp != null) {
                if (dp.isArray()) {
                    for (JsonNode dpnode : dp) {
                        if (dpnode.isArray()) {
                            ret.addKey(dpnode.get(0).asLong());
                            ret.addValue(dpnode.get(1).asText());
                        }
                    }
                }
            }
            JsonNode anno = node.get("annotation");
            if (anno != null) {
                String title=null,description=null;
                JsonNode titleNode = anno.get("title");
                if(titleNode!=null)
                    title = titleNode.asText();
                JsonNode dspNode = anno.get("description");
                if(dspNode!=null)
                    description = dspNode.asText();
                List<String> category = new ArrayList<>();
                JsonNode categoryNode = anno.get("category");
                if (categoryNode.isArray()) {
                    for (JsonNode objNode : categoryNode) {
                        category.add(objNode.asText());
                    }
                }

                //???cat???key???val???????????????tag????????????
                for(String cat : category){
                    ret.addTag(cat, RestUtils.CATEGORY);
                }
                if(title!=null)
                    ret.addAnno("title",title);
                if(description!=null)
                    ret.addAnno("description",description);
            }
            return ret;
        } catch (Exception e) {
            LOGGER.error("Error occurred during parsing data ", e);
            throw e;
        }
    }

    public void sendData() {
        try {
            session.openSession();
            sendMetricsData();
        } catch (Exception e) {
            LOGGER.error("Error occurred during sending data ", e);
        }
        session.closeSession();
    }

    public List<Metric> getMetricList() {
        return metricList;
    }

    public void setMetricList(List<Metric> metricList) {
        this.metricList = metricList;
    }

    private Long ifHasAnnoSequence() {
        try {
            //????????????
            QueryMetric metric = new QueryMetric();
            metric.setName(ANNOTAIONSEQUENCE);
//            metric.addLastAggregator();
            Query query = new Query();
            query.addQueryMetrics(metric);
            query.setStartAbsolute(1L);
            query.setEndAbsolute(2L);
            query.setTimePrecision(TimePrecision.NS);

            //????????????
            QueryExecutor executor = new QueryExecutor(query);
            QueryResult result = executor.execute(false);

            //??????????????????
            if(result.getQueryResultDatasets().get(0).getPaths().isEmpty()) {
                return new Long(-1L);
            } else {
                if(result.getQueryResultDatasets().get(0).getTimestamps().isEmpty())
                    return new Long(ANNOSTARTTIME+1L);
                else {
                    Object val = result.getQueryResultDatasets().get(0).getValueLists().get(0).get(0);
                    String valStr = new String();
                    if (val instanceof byte[]) {
                        valStr = new String((byte[]) val);
                    } else {
                        valStr = String.valueOf(val.toString());
                    }
                    return Long.parseLong(valStr);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return -1L;
        }
    }

    private void createAnnoSequence(boolean ifUpdate, Long val) throws Exception {
        List<String> paths = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        Object[] valuesList = new Object[1];
        Object[] value = new Object[2];
        List<DataType> type = new ArrayList<>();
        List<Map<String, String>> tagsList = new ArrayList<>();


        paths.add(ANNOTAIONSEQUENCE);
        timestamps.add(1L);
        if(ifUpdate) {
            value[0] = getType(String.valueOf(val),DataType.BINARY);
        } else {
            value[0] = getType(String.valueOf(ANNOSTARTTIME+1L),DataType.BINARY);
        }

        type.add(DataType.BINARY);
        valuesList[0] = value;
        try {
            session.insertNonAlignedColumnRecords(paths, timestamps.stream().mapToLong(Long::longValue).toArray(), valuesList, type, null, TimePrecision.NS);
        } catch (ExecutionException e) {
            LOGGER.error("Error occurred during insert ", e);
            throw e;
        }
    }

    private void insertAnnoSquence(Map<Long,String> annoSequence) throws Exception {
        List<Long> timestamps = new ArrayList<>();
        List<String> ANNOPATHS = new ArrayList<>();
        Object[] valuesList = new Object[1];
        Object[] valuesAnno = new Object[2];
        List<DataType> type = new ArrayList<>();

        int pos = 0;
        for(Map.Entry<Long,String> entry : annoSequence.entrySet()) {
            timestamps.add(entry.getKey());
            valuesAnno[pos++] = getType(String.valueOf(entry.getValue()),DataType.BINARY);
        }
        valuesList[0] = valuesAnno;
        type.add(DataType.BINARY);
        ANNOPATHS.add(ANNOTAIONSEQUENCE);
        try {
            session.insertNonAlignedColumnRecords(ANNOPATHS, timestamps.stream().mapToLong(Long::longValue).toArray(), valuesList, type, null, TimePrecision.NS);
        } catch (ExecutionException e) {
            LOGGER.error("Error occurred during insert ", e);
            throw e;
        }
    }
    private void insertAnno(List<String> paths, List<Map<String, String>> tagsList, Map<String,String> anno, DataType typeAb) throws Exception {
        //????????????????????????TitleDsp???????????????????????????????????????
        Long time = ifHasAnnoSequence();
        if(!time.equals(-1L)) {
            Object[] valuesList = new Object[1];
            Object[] valuesAnno = new Object[2];
            List<DataType> type = new ArrayList<>();
            List<Long> timestamps = new ArrayList<>();
            Map<Long,String> annoSequence = new TreeMap<>();
            Long num = 0L;

            int pos = 0;
            if(anno.get("title")!=null){
                valuesAnno[pos++] = getType(String.valueOf(time+num),typeAb);
                annoSequence.put(time+num,anno.get("title"));
                timestamps.add(TITLETIEM);
                num++;
            }
            if(anno.get("description")!=null){
                valuesAnno[pos++] = getType(String.valueOf(time+num),typeAb);
                annoSequence.put(time+num,anno.get("description"));
                timestamps.add(DESCRIPTIONTIEM);
                num++;
            }
            //????????????anno?????????????????????
            createAnnoSequence(true, time+num);
            //???anno???????????????title??????dsp??????
            insertAnnoSquence(annoSequence);

            //??????????????????????????????????????????
            valuesList[0] = valuesAnno;
            type.add(typeAb);
            try {
                session.insertNonAlignedColumnRecords(paths, timestamps.stream().mapToLong(Long::longValue).toArray(), valuesList, type, tagsList, TimePrecision.NS);
            } catch (ExecutionException e) {
                LOGGER.error("Error occurred during insert ", e);
                throw e;
            }
        } else {
            createAnnoSequence(false, null);
            insertAnno(paths,tagsList,anno,typeAb);
        }
    }

    private void sendMetricsData() throws Exception {
        for (Metric metric : metricList) {
            List<Map<String, String>> tagsList = new ArrayList<>();
            tagsList.add(metric.getTags());

            StringBuilder path = new StringBuilder();
            path.append(metric.getName());
            List<String> paths = new ArrayList<>();
            paths.add(path.toString());

            int size = metric.getKeys().size();
            List<DataType> type = new ArrayList<>();
            type.add(findType(metric.getValues()));

            Object[] valuesList = new Object[1];
            Object[] values = new Object[size];
            for (int i = 0; i < size; i++) {
                values[i] = getType(metric.getValues().get(i), type.get(0));
            }
            valuesList[0] = values;
            try {
                session.insertNonAlignedColumnRecords(paths, metric.getKeys().stream().mapToLong(Long::longValue).toArray(), valuesList, type, tagsList);
                if (!metric.getAnno().isEmpty()) {
                    insertAnno(paths,tagsList,metric.getAnno(),type.get(0));
                }
            } catch (ExecutionException e) {
                LOGGER.error("Error occurred during insert ", e);
                throw e;
            }
        }
    }

    private Map<String, String> getTagsFromPaths(String path, StringBuilder name) {//LHZ??????????????????????????????
        Map<String, String> ret = new TreeMap<>();//LHZ????????????????????????tag????????????????????????????????????
        int firstBrace = path.indexOf("{");
        int lastBrace = path.indexOf("}");
        if(firstBrace==-1 || lastBrace==-1){
            name.append(path);
            return ret;
        }
        name.append(path.substring(0, firstBrace));
        String tagLists = path.substring(firstBrace+1, lastBrace);
        String[] splitpaths = tagLists.split(",");
        for(String tag : splitpaths){
            int equalPos = tag.indexOf("=");
            String tagKey = tag.substring(0, equalPos);
            String tagVal = tag.substring(equalPos+1);
            ret.put(tagKey,tagVal);
        }

        return ret;
    }

    //LHZ?????????????????????????????????path?????????????????????????????????????????????????????????
    private String pathAppendAnno(Metric metric, String path, AnnotationLimit annotationLimit){
        StringBuilder name = new StringBuilder();
        Map<String, String> tags = getTagsFromPaths(path, name);
        for(String tag : annotationLimit.getTag()){
            tags.putIfAbsent(tag, RestUtils.CATEGORY);
        }
        metric.setTags(tags);
        return name.toString();
    }

    private void insertExe(Metric metric) throws Exception {
        insertExe(metric, TimeUtils.DEFAULT_TIMESTAMP_PRECISION);
    }

    private void insertExe(Metric metric, TimePrecision timePrecision) throws Exception {
        //LHZ????????????????????????????????????????????????????????????
        //????????????
        StringBuilder path = new StringBuilder();
        path.append(metric.getName());
        List<String> paths = new ArrayList<>();
        paths.add(path.toString());
        List<Map<String,String>> taglist = new ArrayList<>();
        taglist.add(metric.getTags());
        int size = metric.getKeys().size();
        List<DataType> type = new ArrayList<>();
        type.add(findType(metric.getValues()));
        Object[] valuesList = new Object[1];
        Object[] values = new Object[size];
        for (int i = 0; i < size; i++) {
            values[i] = getType(metric.getValues().get(i), type.get(0));
        }
        valuesList[0] = values;
        try {
            //LHZ ????????????????????????????????????@?????????????????????????????????????????????????????????tag
            session.insertNonAlignedColumnRecords(paths, metric.getKeys().stream().mapToLong(Long::longValue).toArray(), valuesList, type, taglist, timePrecision);
            if (!metric.getAnno().isEmpty()) {
                insertAnno(paths,taglist,metric.getAnno(),type.get(0));
            }
        } catch (ExecutionException e) {
            LOGGER.error("Error occurred during insert ", e);
            throw e;
        }
    }

    //??????????????????????????????
    public void handleAnnotationAppend(Query preQuery, QueryResult preQueryResult) throws Exception {
        //??????session
        try {
            session.openSession();
        } catch (SessionException e) {
            LOGGER.error("Error occurred during opening session", e);
            throw e;
        }
        try{
            for (int pos = 0; pos < preQueryResult.getSiz(); pos++) {//LHZ????????????????????????????????????resultDataSet???????????????

                QueryResultDataset queryResultDataset = preQueryResult.getQueryResultDatasets().get(pos);
                QueryMetric queryBase = preQueryResult.getQueryMetrics().get(pos);
                for(int pl = 0; pl < queryResultDataset.getPaths().size(); pl++) {
                    Metric metric = new Metric();
                    //?????????tag?????????metric???
                    String name = pathAppendAnno(metric, queryResultDataset.getPaths().get(pl), queryBase.getAnnotationLimit());
                    metric.setName(name);

                    //???metric????????????anno?????????????????????
                    metricGetData(metric,queryBase,queryResultDataset,queryBase.getAnnotationLimit(),pl);

                    //????????????
                    insertExe(metric, TimePrecision.NS);
                }
            }
        } catch (Exception e) {
            LOGGER.debug("Exception occur for create and send ", e);
            throw e;
        } finally {
            session.closeSession();
        }
    }

    private Metric updateAnnoPath(String path, AnnotationLimit annoLimit) {
        Metric metric = new Metric();
        StringBuilder name = new StringBuilder();
        Map<String,String> tags = getTagsFromPaths(path, name);
        Map<String,String> newTags = new TreeMap<>();
        for(Map.Entry<String,String> entry : tags.entrySet()) {
            if(!entry.getValue().equals(RestUtils.CATEGORY))
                newTags.put(entry.getKey(),entry.getValue());
        }
        if(!annoLimit.getTag().isEmpty())
            for(String tag : annoLimit.getTag()) {
                newTags.putIfAbsent(tag, RestUtils.CATEGORY);
            }

        metric.setTags(newTags);
        metric.setName(name.toString());
        return metric;
    }

    private boolean specificAnnoCategoryPath(Map<String, String> tags, AnnotationLimit annoLimit) {
        int num = 0;

        //???????????????ok
        for(Map.Entry<String,String> entry : tags.entrySet()) {
            if(entry.getValue().equals(RestUtils.CATEGORY)) num++;
        }
        if(num==annoLimit.getTag().size()) return true;
        return false;
    }

    public void handleAnnotationUpdate(Query preQuery, QueryResult preQueryResult) throws Exception {
        //??????session
        try {
            session.openSession();
        } catch (SessionException e) {
            LOGGER.error("Error occurred during opening session", e);
            throw e;
        }
        try{
            for (int pos = 0; pos < preQueryResult.getSiz(); pos++) {//LHZ????????????????????????????????????resultDataSet???????????????
                QueryResultDataset queryResultDataset = preQueryResult.getQueryResultDatasets().get(pos);
                QueryMetric queryBase = preQueryResult.getQueryMetrics().get(pos);
                for(int pl = 0; pl < queryResultDataset.getPaths().size(); pl++){
                    Metric metric = new Metric();
                    StringBuilder name = new StringBuilder();
                    //????????????@?????????
                    Map<String, String> tags = getTagsFromPaths(queryResultDataset.getPaths().get(pl), name);
                    /*???????????????????????????2022.8.12.23.24???????????????????????????????????????if????????????*/
                    //???????????????anno???????????????????????????cat????????????
                    AnnotationLimit newAnnoLimit = preQuery.getQueryMetrics().get(pos).getNewAnnotationLimit();
                    metric = updateAnnoPath(queryResultDataset.getPaths().get(pl), newAnnoLimit);

                    //??????anno???title?????????????????????????????????
                    metricGetData(metric,queryBase,queryResultDataset,queryBase.getNewAnnotationLimit(),pl);

                    insertExe(metric, TimePrecision.NS);
                }
            }
        } catch (Exception e) {
            LOGGER.debug("Exception occur for create and send ", e);
            throw e;
        } finally {
            session.closeSession();
        }
    }

    public DataType judgeObjectType(Object obj){
        if (obj instanceof Boolean){
            return DataType.BOOLEAN;
        }else if (obj instanceof Byte || obj instanceof String || obj instanceof Character){
            return DataType.BINARY;
        }else if (obj instanceof Long || obj instanceof Integer){
            return DataType.LONG;
        }else if (obj instanceof Double || obj instanceof Float){
            return DataType.DOUBLE;
        }
        //???????????????????????????
        return DataType.BINARY;
    }

    public String getStringVal(Object val) {
        switch (judgeObjectType(val)) {
            case BINARY:
                return new String((byte[]) val);
            case LONG:
            case DOUBLE:
                return String.valueOf(val);
            default:
                return new String();//???????????????null
        }
    }

    Object getType(String str, DataType tp) {
        switch (tp) {
            case BINARY:
                return str.getBytes();
            case DOUBLE:
                return Double.parseDouble(str);
            case LONG:
                return Long.parseLong(str);
            default:
                return null;
        }
    }

    DataType findType(List<String> values) {
        //????????? LONG
        DataType ret = DataType.LONG;
        for (String value : values) {
            try {
                Long.parseLong(value,10);
            } catch (NumberFormatException e) {
                try{
                    Double.parseDouble(value);
                } catch (NumberFormatException e2) {
                    return DataType.BINARY;
                }
                ret =  DataType.DOUBLE;
            }
        }
        return ret;
    }

    void metricGetData(Metric metric, QueryMetric queryBase, QueryResultDataset queryResultDataset, AnnotationLimit anno, int pl) {
        //??????anno???title?????????
        if(!anno.getTitle().equals(".*"))
            metric.addAnno("title", anno.getTitle());
        if(!anno.getText().equals(".*"))
            metric.addAnno("description", anno.getText());
        //?????????????????????
        for(int tl = 0;tl<queryResultDataset.getTimeLists().get(pl).size();tl++) {
            metric.addKey(queryResultDataset.getTimeLists().get(pl).get(tl));
            Object val = queryResultDataset.getValueLists().get(pl).get(tl);
            metric.addValue(getStringVal(val));
        }
    }
}