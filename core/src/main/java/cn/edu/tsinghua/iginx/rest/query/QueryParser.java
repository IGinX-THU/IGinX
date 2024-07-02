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
package cn.edu.tsinghua.iginx.rest.query;

import static cn.edu.tsinghua.iginx.utils.TagKVUtils.*;

import cn.edu.tsinghua.iginx.rest.RestUtils;
import cn.edu.tsinghua.iginx.rest.bean.*;
import cn.edu.tsinghua.iginx.rest.query.aggregator.*;
import cn.edu.tsinghua.iginx.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryParser.class);
  private final ObjectMapper mapper = new ObjectMapper();

  public QueryParser() {}

  public static Long dealDateFormat(String oldDateStr) {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    try {
      Date date = df.parse(oldDateStr);
      return date.getTime() + 28800000L;
    } catch (ParseException e) {
      LOGGER.error("unexpected error: ", e);
    }
    return null;
  }

  public static Long transTimeFromString(String str) {
    switch (str) {
        //            case "nanos":
        //                return 1L;
        //            case "micros":
        //                return 1000L;
      case "millis":
        return 1L;
      case "seconds":
        return 1000L;
      case "minutes":
        return 60000L;
      case "hours":
        return 3600000L;
      case "days":
        return 86400000L;
      case "weeks":
        return 604800000L;
      case "months":
        return 2419200000L;
      case "years":
        return 29030400000L;
      default:
        return 0L;
    }
  }

  public Query parseGrafanaQueryMetric(String json) throws Exception {
    Query ret;
    try {
      JsonNode node = mapper.readTree(json);
      ret = getGrafanaQuery(node);
    } catch (Exception e) {
      LOGGER.error("Error occurred during parsing query ", e);
      throw e;
    }
    return ret;
  }

  public Query parseQueryMetric(String json) throws Exception {
    Query ret;
    try {
      JsonNode node = mapper.readTree(json);
      ret = getQuery(node);
    } catch (Exception e) {
      LOGGER.error("Error occurred during parsing query ", e);
      throw e;
    }
    return ret;
  }

  public Query parseAnnotationQueryMetric(String json, boolean isGrafana) throws Exception {
    Query ret;
    try {
      JsonNode node = mapper.readTree(json);
      ret = getAnnotationQuery(node, isGrafana);
    } catch (Exception e) {
      LOGGER.error("Error occurred during parsing query ", e);
      throw e;
    }
    return ret;
  }

  private Query getGrafanaQuery(JsonNode node) {
    Query ret = new Query();
    JsonNode timeRange = node.get("range");
    if (timeRange == null) {
      return null;
    }
    JsonNode startAbsolute = timeRange.get("from");
    JsonNode end_absolute = timeRange.get("to");

    if (startAbsolute == null || end_absolute == null) {
      return null;
    }

    Long start = dealDateFormat(startAbsolute.asText());
    Long end = dealDateFormat(end_absolute.asText());
    ret.setStartAbsolute(start);
    ret.setEndAbsolute(end);

    JsonNode array = node.get("targets");
    if (!array.isArray()) {
      return null;
    }
    for (JsonNode jsonNode : array) {
      QueryMetric queryMetric = new QueryMetric();
      JsonNode type = jsonNode.get("type");
      if (type == null) {
        return null;
      }
      JsonNode target = jsonNode.get("target");
      if (target == null) {
        return null;
      }
      queryMetric.setName(target.asText());
      ret.addQueryMetrics(queryMetric);
    }
    return ret;
  }

  public Query getQuery(JsonNode node) {
    Query ret = new Query();
    JsonNode start_absolute = node.get("start_absolute");
    JsonNode end_absolute = node.get("end_absolute");
    long now = System.currentTimeMillis();
    if (start_absolute == null && end_absolute == null) {
      return null;
    } else if (start_absolute != null && end_absolute != null) {
      ret.setStartAbsolute(start_absolute.asLong());
      ret.setEndAbsolute(end_absolute.asLong());
    } else if (start_absolute != null) {
      if (setEndAbsolute(node, ret, start_absolute, now)) {
        return null;
      }
    } else {
      ret.setEndAbsolute(end_absolute.asLong());
      JsonNode start_relative = node.get("start_relative");
      if (start_relative == null) {
        ret.setStartAbsolute(now);
      } else {
        JsonNode value = start_relative.get("value");
        if (value == null) {
          return null;
        }
        long v = value.asLong();
        JsonNode unit = start_relative.get("unit");
        if (unit == null) {
          return null;
        }
        Long time = transTimeFromString(unit.asText());
        ret.setStartAbsolute(now - v * time);
      }
    }
    JsonNode cacheTime = node.get("cacheTime");
    if (cacheTime != null) {
      ret.setCacheTime(cacheTime.asLong());
    }
    JsonNode timeZone = node.get("time_zone");
    if (cacheTime != null) {
      ret.setTimeZone(timeZone.asText());
    }

    JsonNode metrics = node.get("metrics");
    if (metrics != null && metrics.isArray()) {
      for (JsonNode dpNode : metrics) {
        QueryMetric ins = setQueryMetric(dpNode);
        addAggregators(ins, dpNode);
        ret.addQueryMetrics(ins);
      }
    }
    return ret;
  }

  private QueryMetric setQueryMetric(JsonNode dpnode) {
    QueryMetric ret = new QueryMetric();
    JsonNode name = dpnode.get("name");
    if (name != null) {
      ret.setName(name.asText());
    }
    JsonNode tags = dpnode.get("tags");
    if (tags != null) {
      Iterator<String> fieldNames = tags.fieldNames();
      while (fieldNames.hasNext()) {
        String fieldName = fieldNames.next();
        JsonNode value = tags.get(fieldName);
        List<String> values = new ArrayList<>();
        if (value.isArray()) {
          for (JsonNode jsonNode : value) {
            values.add(jsonNode.asText());
          }
        } else {
          values.add(value.asText());
        }
        ret.addTag(
            new HashMap<String, List<String>>() {
              {
                put(fieldName, values);
              }
            });
      }
    }
    return ret;
  }

  private boolean setEndAbsolute(JsonNode node, Query ret, JsonNode start_absolute, long now) {
    ret.setStartAbsolute(start_absolute.asLong());
    JsonNode end_relative = node.get("end_relative");
    if (end_relative == null) {
      ret.setEndAbsolute(now);
    } else {
      JsonNode value = end_relative.get("value");
      if (value == null) {
        return true;
      }
      long v = value.asLong();
      JsonNode unit = end_relative.get("unit");
      if (unit == null) {
        return true;
      }
      Long time = transTimeFromString(unit.asText());
      ret.setEndAbsolute(now - v * time);
    }
    return false;
  }

  private Query getAnnotationQuery(JsonNode node, boolean isGrafana)
      throws JsonProcessingException {
    Query ret = new Query();
    if (isGrafana) {
      JsonNode range = node.get("range");
      if (range == null) {
        return null;
      }
      JsonNode start_absolute = range.get("from");
      JsonNode end_absolute = range.get("to");
      if (start_absolute == null || end_absolute == null) {
        return null;
      } else {
        Long start = dealDateFormat(start_absolute.asText());
        Long end = dealDateFormat(end_absolute.asText());
        ret.setStartAbsolute(start);
        ret.setEndAbsolute(end);
      }

      JsonNode metric = node.get("annotation");
      if (metric == null) {
        return null;
      }
      QueryMetric ins = new QueryMetric();
      JsonNode name = metric.get("name");
      if (name != null) {
        ins.setName(name.asText());
      }
      JsonNode query = metric.get("query");
      if (query.get("tags") == null) {
        query = mapper.readTree(query.asText());
      }
      JsonNode tags = query.get("tags");
      if (tags != null) {
        tags = tags.get("tags");
        if (tags != null) {
          Iterator<String> fieldNames = tags.fieldNames();
          Map<String, List<String>> tagkv = new HashMap<>();
          while (fieldNames.hasNext()) {
            String key = fieldNames.next();
            JsonNode valueNode = tags.get(key);
            tagkv.put(key, Collections.singletonList(valueNode.asText()));
          }
          ins.addTag(tagkv);
        }
      }
      setAnnotationLimit(ret, ins, query);
    } else {
      JsonNode start_absolute = node.get("start_absolute");
      JsonNode end_absolute = node.get("end_absolute");
      long now = System.currentTimeMillis();
      if (start_absolute == null && end_absolute == null) {
        ret.setStartAbsolute(0L);
        ret.setEndAbsolute(now);
      } else if (start_absolute != null && end_absolute != null) {
        ret.setStartAbsolute(start_absolute.asLong());
        ret.setEndAbsolute(end_absolute.asLong());
      } else if (start_absolute != null) {
        if (setEndAbsolute(node, ret, start_absolute, now)) {
          //                   return null;
        }
      } else {
        ret.setEndAbsolute(end_absolute.asLong());
        JsonNode start_relative = node.get("start_relative");
        if (start_relative == null) {
          ret.setStartAbsolute(0L);
        } else {
          JsonNode value = start_relative.get("value");
          if (value == null) {
            //                       return null;
          }
          long v = value.asLong();
          JsonNode unit = start_relative.get("unit");
          if (unit == null) {
            //                       return null;
          }
          Long time = transTimeFromString(unit.asText());
          ret.setEndAbsolute(now - v * time);
        }
      }

      JsonNode metrics = node.get("metrics");
      if (metrics != null && metrics.isArray()) {
        for (JsonNode dpNode : metrics) {
          QueryMetric ins = setQueryMetric(dpNode);
          setAnnotationLimit(ret, ins, dpNode);
        }
      }
    }
    return ret;
  }

  private AnnotationLimit parserAnno(JsonNode anno) {
    AnnotationLimit annotationLimit = new AnnotationLimit();
    List<String> category = new ArrayList<>();
    JsonNode categoryNode = anno.get("category");
    if (categoryNode != null) {
      if (categoryNode.isArray()) {
        for (JsonNode objNode : categoryNode) {
          category.add(objNode.asText());
        }
      }

      annotationLimit.setTag(category);
    }

    JsonNode text = anno.get("description");
    if (text != null) {
      annotationLimit.setText(text.asText());
    }

    JsonNode description = anno.get("title");
    if (description != null) {
      annotationLimit.setTitle(description.asText());
    }
    return annotationLimit;
  }

  private void setAnnotationLimit(Query ret, QueryMetric ins, JsonNode query) {
    AnnotationLimit annotationLimit;
    JsonNode anno = query.get("annotation");
    if (anno != null) {
      annotationLimit = parserAnno(anno);
      ins.setAnnotationLimit(annotationLimit);
      ins.setAnnotation(true);
    }
    // 设置annotation-new属性
    anno = query.get("annotation-new");
    if (anno != null) {
      annotationLimit = parserAnno(anno);
      ins.setNewAnnotationLimit(annotationLimit);
      ins.setAnnotation(true);
    }
    ret.addQueryMetrics(ins);
  }

  public void addAggregators(QueryMetric q, JsonNode node) {
    JsonNode aggregators = node.get("aggregators");
    if (aggregators == null || !aggregators.isArray()) {
      return;
    }
    for (JsonNode aggregator : aggregators) {
      JsonNode name = aggregator.get("name");
      if (name == null) {
        continue;
      }
      QueryAggregator qa;
      switch (name.asText()) {
        case "max":
          qa = new QueryAggregatorMax();
          break;
        case "min":
          qa = new QueryAggregatorMin();
          break;
        case "sum":
          qa = new QueryAggregatorSum();
          break;
        case "count":
          qa = new QueryAggregatorCount();
          break;
        case "avg":
          qa = new QueryAggregatorAvg();
          break;
        case "first":
          qa = new QueryAggregatorFirst();
          break;
        case "last":
          qa = new QueryAggregatorLast();
          break;
        case "dev":
          qa = new QueryAggregatorDev();
          break;
        case "diff":
          qa = new QueryAggregatorDiff();
          break;
        case "div":
          qa = new QueryAggregatorDiv();
          break;
        case "filter":
          qa = new QueryAggregatorFilter();
          break;
        case "save_as":
          qa = new QueryAggregatorSaveAs();
          break;
        case "rate":
          qa = new QueryAggregatorRate();
          break;
        case "sampler":
          qa = new QueryAggregatorSampler();
          break;
        case "percentile":
          qa = new QueryAggregatorPercentile();
          break;
        default:
          continue;
      }
      switch (name.asText()) {
        case "max":
        case "min":
        case "sum":
        case "count":
        case "avg":
        case "first":
        case "last":
        case "dev":
        case "percentile":
          JsonNode sampling = aggregator.get("sampling");
          if (sampling == null) {
            continue;
          }
          JsonNode value = sampling.get("value");
          if (value == null) {
            continue;
          }
          JsonNode unit = sampling.get("unit");
          if (unit == null) {
            continue;
          }
          long time = transTimeFromString(unit.asText());
          qa.setDur(value.asLong() * time);
          break;
        case "div":
          JsonNode divisor = aggregator.get("divisor");
          if (divisor == null) {
            continue;
          }
          qa.setDivisor(Double.parseDouble(divisor.asText()));
          break;
        case "filter":
          JsonNode filter_op = aggregator.get("filter_op");
          if (filter_op == null) {
            continue;
          }
          JsonNode threshold = aggregator.get("threshold");
          if (threshold == null) {
            continue;
          }
          qa.setFilter(new Filter(filter_op.asText(), threshold.asDouble()));
          break;
        case "save_as":
          JsonNode metric_name = aggregator.get("metric_name");
          if (metric_name == null) {
            continue;
          }
          qa.setMetric_name(metric_name.asText());
          break;
        case "rate":
          sampling = aggregator.get("sampling");
          if (sampling == null) {
            continue;
          }
          unit = sampling.get("unit");
          if (unit == null) {
            continue;
          }
          time = transTimeFromString(unit.asText());
          qa.setUnit(time);
          break;
        case "sampler":
          unit = aggregator.get("unit");
          if (unit == null) {
            continue;
          }
          time = transTimeFromString(unit.asText());
          qa.setUnit(time);
          break;
        case "diff":
        default:
          break;
      }
      if ("percentile".equals(name.asText())) {
        JsonNode percentile = aggregator.get("percentile");
        if (percentile == null) {
          continue;
        }
        qa.setPercentile(Double.parseDouble(percentile.asText()));
      }
      q.addAggregator(qa);
    }
  }

  public String parseAnnoResultToJson(QueryResult anno) {
    return parseAnnoResultToJsonBase(anno, anno::toResultStringAnno);
  }

  public String parseAnnoDataResultToJson(QueryResult data) {
    return parseAnnoResultToJsonBase(data, data::toResultString);
  }

  private String parseAnnoResultToJsonBase(
      QueryResult result, BiFunction<Integer, Integer, String> resultGenerator) {
    StringBuilder ret = new StringBuilder("{\"queries\":[");
    Set<String> paths = new HashSet<>();
    for (int i = 0; i < result.getQueryResultDatasets().size(); i++) {
      QueryResultDataset dataSet = result.getQueryResultDatasets().get(i);
      QueryMetric metric = result.getQueryMetrics().get(i);
      for (int j = 0; j < dataSet.getPaths().size(); j++) {
        if (!dataSet.getPaths().get(j).equals(metric.getQueryOriPath())) {
          continue;
        }
        String tmpPath = metric.getQueryOriPath() + dataSet.getTitles().get(j);
        if (!paths.contains(tmpPath)) {
          paths.add(tmpPath);
        } else {
          continue;
        }

        ret.append(resultGenerator.apply(i, j));
        ret.append(",");
      }
    }
    if (ret.charAt(ret.length() - 1) == ',') {
      ret.deleteCharAt(ret.length() - 1);
    }
    ret.append("]}");
    return ret.toString();
  }

  public String parseResultToJson(QueryResult result, boolean isDelete) {
    if (isDelete) {
      return "";
    }
    StringBuilder ret = new StringBuilder("{\"queries\":[");
    for (int i = 0; i < result.getSize(); i++) {
      ret.append(result.toResultString(i));
      ret.append(",");
    }
    if (ret.charAt(ret.length() - 1) == ',') {
      ret.deleteCharAt(ret.length() - 1);
    }
    ret.append("]}");
    return ret.toString();
  }

  public String parseResultToGrafanaJson(QueryResult result) {
    StringBuilder ret = new StringBuilder("[");
    for (int i = 0; i < result.getSize(); i++) {
      ret.append("{");
      ret.append(String.format("\"target\":\"%s\",", result.getQueryMetrics().get(i).getName()));
      ret.append("\"datapoints\":[");
      int n = result.getQueryResultDatasets().get(i).getSize();
      for (int j = 0; j < n; j++) {
        ret.append("[");
        if (result.getQueryResultDatasets().get(i).getValues().get(j) instanceof byte[]) {
          ret.append("\"");
          ret.append(result.getQueryResultDatasets().get(i).getValues().get(j));
          ret.append("\"");
        } else {
          ret.append(result.getQueryResultDatasets().get(i).getValues().get(j).toString());
        }

        long timeInPrecision =
            TimeUtils.getTimeFromNsToSpecPrecision(
                result.getQueryResultDatasets().get(i).getKeys().get(j),
                TimeUtils.DEFAULT_TIMESTAMP_PRECISION);
        ret.append(String.format(",%d", timeInPrecision));
        ret.append("],");
      }
      if (ret.charAt(ret.length() - 1) == ',') {
        ret.deleteCharAt(ret.length() - 1);
      }
      ret.append("]},");
    }
    if (ret.charAt(ret.length() - 1) == ',') {
      ret.deleteCharAt(ret.length() - 1);
    }
    ret.append("]");
    return ret.toString();
  }

  public Map<String, String> getTagsFromPaths(String path, StringBuilder name) {
    Map<String, String> ret = new LinkedHashMap<>();
    TagKVUtils.fillNameAndTagMap(path, name, ret);
    return ret;
  }

  // 将传入的path（格式为name{tagkey=tagval}）转换为正常的QueryMetric
  public QueryMetric parseQueryResultAnnoDataPaths(String path) {
    StringBuilder name = new StringBuilder();
    QueryMetric queryMetric = new QueryMetric();
    Map<String, String> result = getTagsFromPaths(path, name);

    Map<String, List<String>> tags = new HashMap<>();
    result.forEach((key, val) -> tags.put(key, Collections.singletonList(val)));
    queryMetric.addTag(tags);
    queryMetric.setName(name.toString());
    return queryMetric;
  }

  // 筛选出全部包含prefix集合信息的路径集合
  public List<String> getPrefixPaths(List<String> Prefix, List<String> paths) {
    List<String> ret = new ArrayList<>();
    boolean ifok;
    for (String path : paths) {
      ifok = true;
      for (String prefix : Prefix) {
        if (!path.contains(prefix)) {
          ifok = false;
          break;
        }
      }
      if (ifok) ret.add(path);
    }
    return ret;
  }

  // （title应用）筛选出全部包含prefix集合信息的路径集合
  public List<String> getPathsFromAnnoTitle(
      String prefix, List<String> paths, List<Object> titles) {
    List<String> ret = new ArrayList<>();
    for (int i = 0; i < titles.size(); i++) {
      String path = paths.get(i);
      if (String.valueOf(titles.get(i)).contains(prefix)
          || prefix.equals(".*")) { // LHZ这里要支持正则！！！！！一定要改
        ret.add(path);
      }
    }
    return ret;
  }

  // 获取到确切的路径信息，如何设置这个查询还是一个问题
  public Query splitPath(QueryResult result) {
    Query ret = new Query();
    int pos = 0;
    for (QueryResultDataset queryResultDataset : result.getQueryResultDatasets()) {
      for (String path : queryResultDataset.getPaths()) {
        QueryMetric metric = parseResultAnnoDataPaths(path);
        // 设置anno信息
        metric.setAnnotationLimit(result.getQueryMetrics().get(pos).getAnnotationLimit());
        ret.addQueryMetrics(metric);
      }
      pos++;
    }
    return ret;
  }

  private boolean specificAnnoCategoryPath(Map<String, String> tags, AnnotationLimit annoLimit) {
    int num = 0;

    // 数量相同就欧克克
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      if (entry.getValue().equals(RestUtils.CATEGORY)) {
        num++;
      }
    }
    return num == annoLimit.getTag().size();
  }

  // 获取完全匹配路径信息的query，包含@路径
  public Query getSpecificQuery(QueryResult result) {
    Query ret = new Query();
    int pos = 0;
    for (QueryResultDataset queryResultDataset : result.getQueryResultDatasets()) {
      for (String path : queryResultDataset.getPaths()) {
        /*如果要获取完全匹配的路径，在这里对每个path路径修改*/
        QueryMetric metric = parseResultAnnoDataPaths(path);
        metric.setAnnotationLimit(result.getQueryMetrics().get(pos).getAnnotationLimit());
        ret.addQueryMetrics(metric);
      }
      pos++;
    }
    return ret;
  }

  // 将传入的path（格式为name{tagkey=tagval}）转换为正常的QueryMetric，这里加入了@@@@@@
  private QueryMetric parseResultAnnoDataPaths(String path) {
    StringBuilder name = new StringBuilder();
    QueryMetric metric = new QueryMetric();
    Map<String, String> tags = getTagsFromPaths(path, name);
    metric.setName(name.toString());

    name.append("." + tagPrefix);
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      name.append("." + tagNameAnnotation)
          .append(entry.getKey())
          .append(".")
          .append(entry.getValue());
    }
    name.append("." + tagSuffix);

    Map<String, List<String>> kv = new HashMap<>();
    tags.forEach((key, val) -> kv.put(key, Collections.singletonList(val)));
    metric.addTag(kv);
    metric.setPathName(name.toString());
    return metric;
  }

  public Query addAnnoTags(Query query) {
    Query ret = new Query();
    ret.setQueryMetrics(query.getQueryMetrics());
    for (int i = 0; i < ret.getQueryMetrics().size(); i++) {
      List<String> tags = ret.getQueryMetrics().get(i).getAnnotationLimit().getTag();
      Map<String, List<String>> tagkv = new HashMap<>();
      for (String tag : tags) {
        tagkv.put(tag, Collections.singletonList(RestUtils.CATEGORY));
      }
      ret.getQueryMetrics().get(i).addTag(tagkv);
    }
    return ret;
  }

  public void getAnnoCategory(QueryResult path) {
    for (int i = 0; i < path.getQueryResultDatasets().size(); i++) {
      StringBuilder name = new StringBuilder();
      for (int j = 0; j < path.getQueryResultDatasets().get(i).getPaths().size(); j++) {
        Map<String, String> tags =
            getTagsFromPaths(path.getQueryResultDatasets().get(i).getPaths().get(j), name);
        List<String> categoryList = new ArrayList<>();
        for (Map.Entry<String, String> entry : tags.entrySet()) {
          if (entry.getValue().equals(RestUtils.CATEGORY)) categoryList.add(entry.getKey());
        }
        path.getQueryResultDatasets().get(i).addCategory(categoryList);
      }
    }
  }

  public void removeAggPath(QueryResult result) {
    for (QueryResultDataset data : result.getQueryResultDatasets()) {
      List<String> paths = new ArrayList<>();
      for (String path : data.getPaths()) {
        if (path.contains("(") && path.contains(")")) {
          int first = path.indexOf("("), last = path.indexOf(")");
          path = path.substring(first + 1, last);
        }
        paths.add(path);
      }
      data.setPaths(paths);
    }
  }

  public Query splitAnnoPathToQuery(QueryResult result) {
    Query ret = new Query();
    for (QueryResultDataset data : result.getQueryResultDatasets()) {
      for (String path : data.getPaths()) {
        boolean ifhasAnno = false;
        QueryMetric metric;
        metric = parseQueryResultAnnoDataPaths(path);
        metric.setQueryOriPath(path);
        Map<String, List<String>> tagkv = metric.getTags().get(0);
        for (Map.Entry<String, List<String>> entry : tagkv.entrySet()) {
          if (entry.getValue().get(0).equals(RestUtils.CATEGORY)) {
            ifhasAnno = true;
            break;
          }
        }
        if (ifhasAnno) {
          ret.addQueryMetrics(metric);
        }
      }
    }
    return ret;
  }
}
