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
package cn.edu.tsinghua.iginx.rest.insert;

import static cn.edu.tsinghua.iginx.rest.RestUtils.*;

import cn.edu.tsinghua.iginx.engine.shared.exception.StatementExecutionException;
import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.RestUtils;
import cn.edu.tsinghua.iginx.rest.bean.*;
import cn.edu.tsinghua.iginx.rest.query.QueryExecutor;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import cn.edu.tsinghua.iginx.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Reader;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataPointsParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataPointsParser.class);
  private Reader inputStream = null;
  private final ObjectMapper mapper = new ObjectMapper();
  private List<Metric> metricList = new ArrayList<>();
  private final RestSession session = new RestSession();

  public DataPointsParser() {}

  public DataPointsParser(Reader stream) {
    this.inputStream = stream;
  }

  public void parse() throws Exception {
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
          metricList.add(getMetricObject(objNode));
        }
      } else {
        metricList.add(getMetricObject(node));
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

  private boolean ifInputDataValid(JsonNode node) {
    String name = node.get("name").toString();
    if (!name.contains(".")) {
      LOGGER.error("The input path should contains at least second order path");
      return false;
    }
    return true;
  }
  // 如果有anno信息会直接放入到插入路径中
  private Metric getMetricObject(JsonNode node) throws Exception {
    try {
      Metric ret = new Metric();
      if (!ifInputDataValid(node)) {
        throw new Exception("The input correctness check is abnormal");
      }
      ret.setName(node.get("name").asText());
      Iterator<String> fieldNames = node.get("tags").fieldNames();
      Iterator<JsonNode> elements = node.get("tags").elements();
      // insert语句的tag只能有一个val
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
          for (JsonNode dpNode : dp) {
            if (dpNode.isArray()) {
              ret.addKey(dpNode.get(0).asLong());
              ret.addValue(dpNode.get(1).asText());
            }
          }
        }
      }
      JsonNode anno = node.get("annotation");
      if (anno != null) {
        String title = null, description = null;
        JsonNode titleNode = anno.get("title");
        if (titleNode != null) title = titleNode.asText();
        JsonNode dspNode = anno.get("description");
        if (dspNode != null) description = dspNode.asText();
        List<String> category = new ArrayList<>();
        JsonNode categoryNode = anno.get("category");
        if (categoryNode.isArray()) {
          for (JsonNode objNode : categoryNode) {
            category.add(objNode.asText());
          }
        }

        // 将cat的key与val颠倒后作为tag进行插入
        for (String cat : category) {
          ret.addTag(cat, RestUtils.CATEGORY);
        }
        if (title != null) ret.addAnno("title", title);
        if (description != null) ret.addAnno("description", description);
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
      // 构造查询
      QueryMetric metric = new QueryMetric();
      metric.setName(ANNOTATION_SEQUENCE);
      //            metric.addLastAggregator();
      Query query = new Query();
      query.addQueryMetrics(metric);
      query.setStartAbsolute(1L);
      query.setEndAbsolute(2L);
      query.setTimePrecision(TimePrecision.NS);

      // 执行查询
      QueryExecutor executor = new QueryExecutor(query);
      QueryResult result = executor.execute(false);

      // 判断是否存在
      if (result.getQueryResultDatasets().get(0).getPaths().isEmpty()) {
        return -1L;
      } else {
        if (result.getQueryResultDatasets().get(0).getKeys().isEmpty())
          return ANNOTATION_START_KEY + 1L;
        else {
          Object val = result.getQueryResultDatasets().get(0).getValueLists().get(0).get(0);
          String valStr;
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

    paths.add(ANNOTATION_SEQUENCE);
    timestamps.add(1L);
    if (ifUpdate) {
      value[0] = getType(String.valueOf(val), DataType.BINARY);
    } else {
      value[0] = getType(String.valueOf(ANNOTATION_START_KEY + 1L), DataType.BINARY);
    }

    type.add(DataType.BINARY);
    valuesList[0] = value;
    try {
      session.insertNonAlignedColumnRecords(
          paths,
          timestamps.stream().mapToLong(Long::longValue).toArray(),
          valuesList,
          type,
          null,
          TimePrecision.NS);
    } catch (SessionException e) {
      LOGGER.error("Error occurred during insert ", e);
      throw e;
    }
  }

  private void insertAnnoSequence(Map<Long, String> annoSequence) throws Exception {
    List<Long> timestamps = new ArrayList<>();
    List<String> annoPaths = new ArrayList<>();
    Object[] valuesList = new Object[1];
    Object[] valuesAnno = new Object[2];
    List<DataType> type = new ArrayList<>();

    int pos = 0;
    for (Map.Entry<Long, String> entry : annoSequence.entrySet()) {
      timestamps.add(entry.getKey());
      valuesAnno[pos++] = getType(String.valueOf(entry.getValue()), DataType.BINARY);
    }
    valuesList[0] = valuesAnno;
    type.add(DataType.BINARY);
    annoPaths.add(ANNOTATION_SEQUENCE);
    try {
      session.insertNonAlignedColumnRecords(
          annoPaths,
          timestamps.stream().mapToLong(Long::longValue).toArray(),
          valuesList,
          type,
          null,
          TimePrecision.NS);
    } catch (SessionException e) {
      LOGGER.error("Error occurred during insert ", e);
      throw e;
    }
  }

  private void insertAnno(
      List<String> paths,
      List<Map<String, String>> tagsList,
      Map<String, String> anno,
      DataType typeAb)
      throws Exception {
    // 首先判断是否存在TitleDsp序列，并获取要插入的时间戳
    Long time = ifHasAnnoSequence();
    if (!time.equals(-1L)) {
      Object[] valuesList = new Object[1];
      Object[] valuesAnno = new Object[2];
      List<DataType> type = new ArrayList<>();
      List<Long> timestamps = new ArrayList<>();
      Map<Long, String> annoSequence = new TreeMap<>();
      Long num = 0L;

      int pos = 0;
      if (anno.get("title") != null) {
        valuesAnno[pos++] = getType(String.valueOf(time + num), typeAb);
        annoSequence.put(time + num, anno.get("title"));
        timestamps.add(TITLE_KEY);
        num++;
      }
      if (anno.get("description") != null) {
        valuesAnno[pos++] = getType(String.valueOf(time + num), typeAb);
        annoSequence.put(time + num, anno.get("description"));
        timestamps.add(DESCRIPTION_KEY);
        num++;
      }
      // 首先更新anno列表可用最小值
      createAnnoSequence(true, time + num);
      // 在anno列表中插入title以及dsp信息
      insertAnnoSequence(annoSequence);

      // 在原序列中插入相应的时间戳值
      valuesList[0] = valuesAnno;
      type.add(typeAb);
      try {
        session.insertNonAlignedColumnRecords(
            paths,
            timestamps.stream().mapToLong(Long::longValue).toArray(),
            valuesList,
            type,
            tagsList,
            TimePrecision.NS);
      } catch (SessionException e) {
        LOGGER.error("Error occurred during insert ", e);
        throw e;
      }
    } else {
      createAnnoSequence(false, null);
      insertAnno(paths, tagsList, anno, typeAb);
    }
  }

  private void sendMetricsData() throws Exception {
    for (Metric metric : metricList) {
      try {
        insertExe(metric, TimeUtils.DEFAULT_TIMESTAMP_PRECISION);
      } catch (StatementExecutionException e) {
        LOGGER.error("Error occurred during insert ", e);
        throw e;
      }
    }
  }

  private Map<String, String> getTagsFromPaths(String path, StringBuilder name) { // LHZ确认下是否传入了引用
    Map<String, String> ret = new TreeMap<>(); // LHZ这里要再次确认下tag的顺序是否和底层存储一样
    TagKVUtils.fillNameAndTagMap(path, name, ret);
    return ret;
  }

  // LHZ注意了！！给路径中添加path，这个是允许的，但是一定要保证顺序！！
  private String pathAppendAnno(Metric metric, String path, AnnotationLimit annotationLimit) {
    StringBuilder name = new StringBuilder();
    Map<String, String> tags = getTagsFromPaths(path, name);
    for (String tag : annotationLimit.getTag()) {
      tags.putIfAbsent(tag, RestUtils.CATEGORY);
    }
    metric.setTags(tags);
    return name.toString();
  }

  private void insertExe(Metric metric) throws Exception {
    insertExe(metric, TimeUtils.DEFAULT_TIMESTAMP_PRECISION);
  }

  private void insertExe(Metric metric, TimePrecision timePrecision) throws Exception {
    // LHZ以下代码重复了，能否合并到一个函数？？？
    // 执行插入
    List<String> paths = new ArrayList<>();
    paths.add(metric.getName());
    List<Map<String, String>> taglist = new ArrayList<>();
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
      // LHZ 因为我们默认是可以通过加@的路径访问实现确切的插入，所以无需添加tag
      session.insertNonAlignedColumnRecords(
          paths,
          metric.getKeys().stream().mapToLong(Long::longValue).toArray(),
          valuesList,
          type,
          taglist,
          timePrecision);
      if (!metric.getAnno().isEmpty()) {
        insertAnno(paths, taglist, metric.getAnno(), type.get(0));
      }
    } catch (StatementExecutionException e) {
      LOGGER.error("Error occurred during insert ", e);
      throw e;
    }
  }

  // 修改路径，并插入数据
  public void handleAnnotationAppend(QueryResult preQueryResult) throws Exception {
    // 创建session
    try {
      session.openSession();
    } catch (SessionException e) {
      LOGGER.error("Error occurred during opening session", e);
      throw e;
    }
    try {
      for (int pos = 0;
          pos < preQueryResult.getSize();
          pos++) { // LHZ这里在测试时确认是否每个resultDataSet只有一个值

        QueryResultDataset queryResultDataset = preQueryResult.getQueryResultDatasets().get(pos);
        QueryMetric queryBase = preQueryResult.getQueryMetrics().get(pos);
        for (int pl = 0; pl < queryResultDataset.getPaths().size(); pl++) {
          Metric metric = new Metric();
          // 分析出tag加入到metric中
          String name =
              pathAppendAnno(
                  metric, queryResultDataset.getPaths().get(pl), queryBase.getAnnotationLimit());
          metric.setName(name);

          // 向metric中插入，anno以及数据点信息
          metricGetData(metric, queryResultDataset, queryBase.getAnnotationLimit(), pl);

          // 执行插入
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
    Map<String, String> tags = getTagsFromPaths(path, name);
    Map<String, String> newTags = new TreeMap<>();
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      if (!entry.getValue().equals(RestUtils.CATEGORY))
        newTags.put(entry.getKey(), entry.getValue());
    }
    if (!annoLimit.getTag().isEmpty())
      for (String tag : annoLimit.getTag()) {
        newTags.putIfAbsent(tag, RestUtils.CATEGORY);
      }

    metric.setTags(newTags);
    metric.setName(name.toString());
    return metric;
  }

  public void handleAnnotationUpdate(Query preQuery, QueryResult preQueryResult) throws Exception {
    // 创建session
    try {
      session.openSession();
    } catch (SessionException e) {
      LOGGER.error("Error occurred during opening session", e);
      throw e;
    }
    try {
      for (int pos = 0;
          pos < preQueryResult.getSize();
          pos++) { // LHZ这里在测试时确认是否每个resultDataSet只有一个值
        QueryResultDataset queryResultDataset = preQueryResult.getQueryResultDatasets().get(pos);
        QueryMetric queryBase = preQueryResult.getQueryMetrics().get(pos);
        for (int pl = 0; pl < queryResultDataset.getPaths().size(); pl++) {
          // 添加包含@的路径
          /*这里更新为包含关系2022.8.12.23.24，如果之后修改，在此处加入if限制条件*/
          // 更改为新的anno信息，即将路径中的cat信息更新
          AnnotationLimit newAnnoLimit =
              preQuery.getQueryMetrics().get(pos).getNewAnnotationLimit();
          Metric metric = updateAnnoPath(queryResultDataset.getPaths().get(pl), newAnnoLimit);

          // 添加anno的title等信息，以及数据点信息
          metricGetData(metric, queryResultDataset, queryBase.getNewAnnotationLimit(), pl);

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

  public String getStringVal(Object val) {
    switch (judgeObjectType(val)) {
      case BINARY:
        return new String((byte[]) val);
      case LONG:
      case DOUBLE:
        return String.valueOf(val);
      default:
        return ""; // 尽量不要传null
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
    // 默认为 LONG
    DataType ret = DataType.LONG;
    for (String value : values) {
      try {
        Long.parseLong(value, 10);
      } catch (NumberFormatException e) {
        try {
          Double.parseDouble(value);
        } catch (NumberFormatException e2) {
          return DataType.BINARY;
        }
        ret = DataType.DOUBLE;
      }
    }
    return ret;
  }

  void metricGetData(
      Metric metric, QueryResultDataset queryResultDataset, AnnotationLimit anno, int pl) {
    // 添加anno的title等信息
    if (!anno.getTitle().equals(".*")) metric.addAnno("title", anno.getTitle());
    if (!anno.getText().equals(".*")) metric.addAnno("description", anno.getText());
    // 添加数据点信息
    for (int tl = 0; tl < queryResultDataset.getKeyLists().get(pl).size(); tl++) {
      metric.addKey(queryResultDataset.getKeyLists().get(pl).get(tl));
      Object val = queryResultDataset.getValueLists().get(pl).get(tl);
      metric.addValue(getStringVal(val));
    }
  }
}
