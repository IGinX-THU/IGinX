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
package cn.edu.tsinghua.iginx.rest.bean;

import static cn.edu.tsinghua.iginx.rest.RestUtils.TOP_KEY;

import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.rest.RestUtils;
import cn.edu.tsinghua.iginx.rest.query.QueryParser;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregator;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorType;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import java.util.*;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class QueryResult {
  public static final Logger logger = LoggerFactory.getLogger(QueryResult.class);
  private static final IMetaManager META_MANAGER = DefaultMetaManager.getInstance();
  private List<QueryMetric> queryMetrics = new ArrayList<>();
  private List<QueryResultDataset> queryResultDatasets = new ArrayList<>();
  private List<QueryAggregator> queryAggregators = new ArrayList<>();
  private int size = 0;

  public void addQueryMetric(QueryMetric queryMetric) {
    queryMetrics.add(queryMetric);
  }

  public void addQueryResultDataset(QueryResultDataset queryResultDataset) {
    queryResultDatasets.add(queryResultDataset);
  }

  public void addQueryAggregator(QueryAggregator queryAggregator) {
    queryAggregators.add(queryAggregator);
  }

  public void addResultSet(
      QueryResultDataset queryDataSet, QueryMetric queryMetric, QueryAggregator queryAggregator) {
    addQueryResultDataset(queryDataSet);
    addQueryMetric(queryMetric);
    addQueryAggregator(queryAggregator);
    size += 1;
  }

  public void addResultSet(QueryResultDataset queryDataSet) {
    addQueryResultDataset(queryDataSet);
  }

  public String toResultString(int pos) {
    return "{"
        + sampleSizeToString(pos)
        + ","
        + "\"results\": [{ "
        + nameToString(pos)
        + ","
        + groupbyToString()
        + ","
        + tagsToString(pos)
        + ","
        + valueToString(pos)
        + "}]}";
  }

  public String toResultStringAnno(int pos, int now) {
    return "{"
        + nameToString(pos)
        + ","
        + tagsToStringAnno(queryResultDatasets.get(pos).getPaths().get(now))
        + ","
        + annoToString(pos, now)
        + "}";
  }

  public String toResultString(int pos, int now) {
    return "{"
        + nameToString(pos)
        + ","
        + tagsToStringAnno(queryResultDatasets.get(pos).getPaths().get(now))
        + ","
        + annoToString(pos, now)
        + ","
        + valueToStringAnno(pos, now)
        + "}";
  }

  // 从包含cat的完整路径中获取tags{}
  private String tagsToStringAnno(String path) {
    StringBuilder ret = new StringBuilder(" \"tags\": {");
    QueryParser parser = new QueryParser();
    Map<String, String> tags = parser.getTagsFromPaths(path, new StringBuilder());
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      if (!entry.getValue().equals(RestUtils.CATEGORY)) {
        ret.append("\"")
            .append(entry.getKey())
            .append("\" : [\"")
            .append(entry.getValue())
            .append("\"],");
      }
    }
    if (ret.charAt(ret.length() - 1) == ',') {
      ret.deleteCharAt(ret.length() - 1);
    }
    ret.append("}");
    return ret.toString();
  }

  // 获取anno信息{}
  private String annoToString(int pos, int now) {
    StringBuilder ret = new StringBuilder("\"annotation\": {");
    ret.append(
        String.format("\"title\": \"%s\",", queryResultDatasets.get(pos).getTitles().get(now)));
    ret.append(
        String.format(
            "\"description\": \"%s\",", queryResultDatasets.get(pos).getDescriptions().get(now)));
    ret.append("\"category\": [");
    for (String tag : queryResultDatasets.get(pos).getCategoryLists().get(now)) {
      ret.append(String.format("\"%s\",", tag));
    }
    if (ret.charAt(ret.length() - 1) == ',') {
      ret.deleteCharAt(ret.length() - 1);
    }
    ret.append("]}");
    return ret.toString();
  }

  private String nameToString(int pos) {
    if (queryAggregators.get(pos).getType() == QueryAggregatorType.SAVE_AS) {
      return String.format("\"name\": \"%s\"", queryAggregators.get(pos).getMetric_name());
    } else {
      return String.format("\"name\": \"%s\"", queryMetrics.get(pos).getName());
    }
  }

  private String groupbyToString() {
    return "\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}]";
  }

  private String tagsToString(int pos) {
    StringBuilder ret = new StringBuilder(" \"tags\": {");
    Map<String, Set<String>> tags = getTagsFromPaths(queryResultDatasets.get(pos).getPaths());
    for (Map.Entry<String, Set<String>> entry : tags.entrySet()) {
      ret.append(String.format("\"%s\": [", entry.getKey()));
      for (String v : entry.getValue()) {
        ret.append(String.format("\"%s\",", v));
      }
      ret.deleteCharAt(ret.length() - 1);
      ret.append("],");
    }
    if (ret.charAt(ret.length() - 1) == ',') {
      ret.deleteCharAt(ret.length() - 1);
    }
    ret.append("}");
    return ret.toString();
  }

  private String valueToString(int pos) {
    StringBuilder ret = new StringBuilder(" \"values\": [");
    int n = queryResultDatasets.get(pos).getSize();
    for (int i = 0; i < n; i++) {
      long timeRes =
          TimeUtils.getTimeFromNsToSpecPrecision(
              queryResultDatasets.get(pos).getKeys().get(i), TimeUtils.DEFAULT_TIMESTAMP_PRECISION);
      ret.append(String.format("[%d,", timeRes));
      if (queryResultDatasets.get(pos).getValues().get(i) instanceof byte[]) {
        ret.append("\"");
        ret.append(new String((byte[]) queryResultDatasets.get(pos).getValues().get(i)));
        ret.append("\"");
      } else {
        ret.append(queryResultDatasets.get(pos).getValues().get(i).toString());
      }
      ret.append("],");
    }
    if (ret.charAt(ret.length() - 1) == ',') {
      ret.deleteCharAt(ret.length() - 1);
    }
    ret.append("]");
    return ret.toString();
  }

  private String valueToStringAnno(int pos, int now) {
    StringBuilder ret = new StringBuilder(" \"values\": [");
    List<Long> keyLists = queryResultDatasets.get(pos).getKeyLists().get(now);
    List<Object> valueLists = queryResultDatasets.get(pos).getValueLists().get(now);

    for (int j = 0; j < keyLists.size(); j++) {
      if (keyLists.get(j) > TOP_KEY) {
        continue;
      }
      long timeInPrecision =
          TimeUtils.getTimeFromNsToSpecPrecision(
              keyLists.get(j), TimeUtils.DEFAULT_TIMESTAMP_PRECISION);
      ret.append(String.format("[%d,", timeInPrecision));
      if (valueLists.get(j) instanceof byte[]) {
        ret.append("\"");
        ret.append(new String((byte[]) valueLists.get(j)));
        ret.append("\"");
      } else {
        ret.append(valueLists.get(j).toString());
      }
      ret.append("],");
    }
    if (ret.charAt(ret.length() - 1) == ',') {
      ret.deleteCharAt(ret.length() - 1);
    }
    ret.append("]");
    return ret.toString();
  }

  private String sampleSizeToString(int pos) {
    return "\"sample_size\": " + queryResultDatasets.get(pos).getSampleSize();
  }

  private Map<String, Set<String>> getTagsFromPaths(List<String> paths) {
    Map<String, Set<String>> ret = new TreeMap<>();
    for (String path : paths) {
      int firstBrace = path.indexOf("{");
      int lastBrace = path.indexOf("}");
      if (firstBrace == -1 || lastBrace == -1) {
        break;
      }
      String tagLists = path.substring(firstBrace + 1, lastBrace);
      String[] splitPaths = tagLists.split(",");
      for (String tag : splitPaths) {
        int equalPos = tag.indexOf("=");
        String tagKey = tag.substring(0, equalPos);
        String tagVal = tag.substring(equalPos + 1);
        ret.computeIfAbsent(tagKey, k -> new HashSet<>());
        ret.get(tagKey).add(tagVal);
      }
    }
    return ret;
  }
}
