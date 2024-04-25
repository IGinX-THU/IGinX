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
package cn.edu.tsinghua.iginx.session;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.*;

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.FormatUtils;
import java.util.*;

public class SessionExecuteSqlResult {

  private SqlType sqlType;
  private long[] keys;
  private List<String> paths;
  private List<List<Object>> values;
  private List<DataType> dataTypeList;
  private int replicaNum;
  private long pointsNum;
  private String parseErrorMsg;
  private List<IginxInfo> iginxInfos;
  private List<StorageEngineInfo> storageEngineInfos;
  private List<MetaStorageInfo> metaStorageInfos;
  private LocalMetaStorageInfo localMetaStorageInfo;
  private List<RegisterTaskInfo> registerTaskInfos;
  private long jobId;
  private JobState jobState;
  private List<Long> jobIdList;
  private String configValue;
  private String loadCsvPath;
  private List<Long> sessionIDs;

  private Map<String, Boolean> rules;

  private long totalCostTime;

  // Only for mock test
  public SessionExecuteSqlResult() {}

  // Only for mock test
  public SessionExecuteSqlResult(
      SqlType sqlType,
      long[] keys,
      List<String> paths,
      List<List<Object>> values,
      List<DataType> dataTypeList) {
    this.sqlType = sqlType;
    this.keys = keys;
    this.paths = paths;
    this.values = values;
    this.dataTypeList = dataTypeList;
  }

  public SessionExecuteSqlResult(ExecuteSqlResp resp) {
    this.sqlType = resp.getType();
    this.parseErrorMsg = resp.getParseErrorMsg();
    switch (resp.getType()) {
      case GetReplicaNum:
        this.replicaNum = resp.getReplicaNum();
        break;
      case CountPoints: // TODO 需要在底层屏蔽系统级时间序列以及注释索引数据点
        this.pointsNum = resp.getPointsNum();
        break;
      case Query:
        constructQueryResult(resp);
        break;
      case ShowColumns:
        this.paths = resp.getPaths();
        this.dataTypeList = resp.getDataTypeList();
        break;
      case ShowClusterInfo:
        this.iginxInfos = resp.getIginxInfos();
        this.storageEngineInfos = resp.getStorageEngineInfos();
        this.metaStorageInfos = resp.getMetaStorageInfos();
        this.localMetaStorageInfo = resp.getLocalMetaStorageInfo();
        break;
      case ShowRegisterTask:
        this.registerTaskInfos = resp.getRegisterTaskInfos();
        break;
      case CommitTransformJob:
        this.jobId = resp.getJobId();
        break;
      case ShowJobStatus:
        this.jobState = resp.getJobState();
        break;
      case ShowEligibleJob:
        this.jobIdList = resp.getJobIdList();
        break;
      case ShowConfig:
        this.configValue = resp.getConfigValue();
        break;
      case LoadCsv:
        this.loadCsvPath = resp.getLoadCsvPath();
      case ShowSessionID:
        this.sessionIDs = resp.getSessionIDList();
      case ShowRules:
        this.rules = resp.getRules();
      default:
        break;
    }
  }

  private void constructQueryResult(ExecuteSqlResp resp) {
    this.paths = resp.getPaths();
    this.dataTypeList = resp.getDataTypeList();

    if (resp.keys != null) {
      this.keys = getLongArrayFromByteBuffer(resp.keys);
    }

    // parse values
    if (resp.getQueryDataSet() != null) {
      this.values =
          getValuesFromBufferAndBitmaps(
              resp.dataTypeList, resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList);
    } else {
      this.values = new ArrayList<>();
    }
    this.totalCostTime = resp.getCostTime();
  }

  public List<List<String>> getResultInList(
      boolean needFormatTime, String timeFormat, String timePrecision) {
    List<List<String>> result = new ArrayList<>();
    if (sqlType == SqlType.Query) {
      result = cacheResult(needFormatTime, timeFormat, timePrecision);
    } else if (sqlType == SqlType.ShowColumns) {
      result.add(new ArrayList<>(Arrays.asList("Path", "DataType")));
      if (paths != null) {
        for (int i = 0; i < paths.size(); i++) {
          result.add(Arrays.asList(paths.get(i) + "", dataTypeList.get(i) + ""));
        }
      }
    } else if (sqlType == SqlType.GetReplicaNum) {
      result.add(new ArrayList<>(Collections.singletonList("Replica num")));
      result.add(new ArrayList<>(Collections.singletonList(replicaNum + "")));
    } else if (sqlType == SqlType.CountPoints) {
      result.add(new ArrayList<>(Collections.singletonList("Points num")));
      result.add(new ArrayList<>(Collections.singletonList(pointsNum + "")));
    } else if (sqlType == SqlType.ShowClusterInfo) {
      result = buildShowClusterInfoListResult();
    } else if (sqlType == SqlType.ShowRegisterTask) {
      result = buildShowRegisterTaskListResult();
    } else {
      result.add(new ArrayList<>(Collections.singletonList("Empty set")));
    }
    return result;
  }

  public void print(boolean needFormatTime, String timePrecision) {
    System.out.print(getResultInString(needFormatTime, timePrecision));
    if (parseErrorMsg != null) {
      System.out.println(parseErrorMsg);
    }
  }

  public String getResultInString(boolean needFormatTime, String timePrecision) {
    switch (sqlType) {
      case Query:
        return buildQueryResult(needFormatTime, timePrecision);
      case ShowColumns:
        return buildShowColumnsResult();
      case ShowClusterInfo:
        return buildShowClusterInfoResult();
      case ShowRegisterTask:
        return buildShowRegisterTaskResult();
      case ShowEligibleJob:
        return buildShowEligibleJobResult();
      case ShowSessionID:
        return buildShowSessionIDResult();
      case ShowRules:
        return buildShowRulesResult();
      case GetReplicaNum:
        return "Replica num: " + replicaNum + "\n";
      case CountPoints:
        return "Points num: " + pointsNum + "\n";
      case CommitTransformJob:
        return "job id: " + jobId;
      case ShowJobStatus:
        return "Job status: " + jobState;
      case ShowConfig:
        return "config value: " + configValue + "\n";
      default:
        return "No data to print." + "\n";
    }
  }

  private String buildQueryResult(boolean needFormatTime, String timePrecision) {
    StringBuilder builder = new StringBuilder();
    builder.append("ResultSets:").append("\n");

    List<List<String>> cache =
        cacheResult(needFormatTime, FormatUtils.DEFAULT_TIME_FORMAT, timePrecision);
    builder.append(FormatUtils.formatResult(cache));

    builder.append(FormatUtils.formatCount(cache.size() - 1));

    if (paths != null && !paths.isEmpty() && paths.get(0).equals("Physical Tree")) {
      builder.append(cacheMoreThings());
    }
    return builder.toString();
  }

  private String cacheMoreThings() {
    Map<String, OpStats> map = new HashMap<>();
    for (List<Object> row : values) {
      String timeStr = FormatUtils.valueToString(row.get(1));
      String name = FormatUtils.valueToString(row.get(3)).split(":")[0];
      int costTime = Integer.parseInt(timeStr.substring(0, timeStr.length() - 2));
      if (map.containsKey(name)) {
        map.get(name).addUp(costTime);
      } else {
        map.put(name, new OpStats(name, costTime));
      }
    }

    List<OpStats> list = new ArrayList<>(map.values());
    list.sort(Comparator.comparingInt(a -> a.costTime));

    StringBuilder builder = new StringBuilder();
    list.forEach(
        opStats ->
            builder
                .append(
                    String.format(
                        "[%s] cost time: %s, num: %s", opStats.name, opStats.costTime, opStats.num))
                .append("\n"));
    builder.append("total cost: ").append(totalCostTime).append("ms\n");
    return builder.toString();
  }

  static class OpStats {
    String name;
    int num;
    int costTime;

    public OpStats(String name, int costTime) {
      this.num = 1;
      this.name = name;
      this.costTime = costTime;
    }

    public void addUp(int costTime) {
      this.num++;
      this.costTime += costTime;
    }
  }

  private List<List<String>> cacheResult(
      boolean needFormatTime, String timeFormat, String timePrecision) {
    List<List<String>> cache = new ArrayList<>();
    List<String> label = new ArrayList<>();
    int annotationPathIndex = -1;
    if (keys != null) {
      label.add(GlobalConstant.KEY_NAME);
    }
    for (int i = 0; i < paths.size(); i++) {
      String path = paths.get(i);
      if (!path.equals("TITLE.DESCRIPTION")) { // TODO 不展示系统级时间序列
        label.add(path);
      } else {
        annotationPathIndex = i;
      }
    }

    for (int i = 0; i < values.size(); i++) {
      List<String> rowCache = new ArrayList<>();
      if (keys != null) {
        if (keys[i] == Long.MAX_VALUE - 1 || keys[i] == Long.MAX_VALUE - 2) {
          continue;
        }
        String timeValue;
        if (needFormatTime) {
          timeValue = FormatUtils.formatTime(keys[i], timeFormat, timePrecision);
        } else {
          timeValue = String.valueOf(keys[i]);
        }
        rowCache.add(timeValue);
      }

      List<Object> rowData = values.get(i);
      boolean isNull = true; // TODO 该行除系统级时间序列之外全部为空
      for (int j = 0; j < rowData.size(); j++) {
        if (j == annotationPathIndex) {
          continue;
        }
        String rowValue = FormatUtils.valueToString(rowData.get(j));
        rowCache.add(rowValue);
        if (!rowValue.equalsIgnoreCase("null")) {
          isNull = false;
        }
      }
      if (!isNull) {
        cache.add(rowCache);
      }
    }

    cache.add(0, label);

    return cache;
  }

  private String buildShowColumnsResult() {
    StringBuilder builder = new StringBuilder();
    builder.append("Columns:").append("\n");
    int num = 0;
    if (paths != null) {
      List<List<String>> cache = new ArrayList<>();
      cache.add(new ArrayList<>(Arrays.asList("Path", "DataType")));
      for (int i = 0; i < paths.size(); i++) {
        if (!paths.get(i).equals("TITLE.DESCRIPTION")) { // TODO 不展示系统级时间序列
          cache.add(new ArrayList<>(Arrays.asList(paths.get(i), dataTypeList.get(i).toString())));
          num++;
        }
      }
      builder.append(FormatUtils.formatResult(cache));
    }
    builder.append(FormatUtils.formatCount(num));
    return builder.toString();
  }

  private String buildShowClusterInfoResult() {
    StringBuilder builder = new StringBuilder();

    if (iginxInfos != null && !iginxInfos.isEmpty()) {
      builder.append("IginX infos:").append("\n");
      List<List<String>> cache = new ArrayList<>();
      cache.add(new ArrayList<>(Arrays.asList("ID", "IP", "PORT")));
      for (IginxInfo info : iginxInfos) {
        cache.add(
            new ArrayList<>(
                Arrays.asList(
                    String.valueOf(info.getId()), info.getIp(), String.valueOf(info.getPort()))));
      }
      builder.append(FormatUtils.formatResult(cache));
    }

    if (storageEngineInfos != null && !storageEngineInfos.isEmpty()) {
      builder.append("Storage engine infos:").append("\n");
      List<List<String>> cache = new ArrayList<>();
      cache.add(
          new ArrayList<>(
              Arrays.asList("ID", "IP", "PORT", "TYPE", "SCHEMA_PREFIX", "DATA_PREFIX")));
      for (StorageEngineInfo info : storageEngineInfos) {
        cache.add(
            new ArrayList<>(
                Arrays.asList(
                    String.valueOf(info.getId()),
                    info.getIp(),
                    String.valueOf(info.getPort()),
                    info.getType().toString(),
                    info.getSchemaPrefix(),
                    info.getDataPrefix())));
      }
      builder.append(FormatUtils.formatResult(cache));
    }

    if (metaStorageInfos != null && !metaStorageInfos.isEmpty()) {
      builder.append("Meta Storage infos:").append("\n");
      List<List<String>> cache = new ArrayList<>();
      cache.add(new ArrayList<>(Arrays.asList("IP", "PORT", "TYPE")));
      for (MetaStorageInfo info : metaStorageInfos) {
        cache.add(
            new ArrayList<>(
                Arrays.asList(info.getIp(), String.valueOf(info.getPort()), info.getType())));
      }
      builder.append(FormatUtils.formatResult(cache));
    }

    if (localMetaStorageInfo != null) {
      builder.append("Meta Storage path:").append("\n");
      List<List<String>> cache = new ArrayList<>();
      cache.add(new ArrayList<>(Collections.singletonList("PATH")));
      cache.add(new ArrayList<>(Collections.singletonList(localMetaStorageInfo.getPath())));
      builder.append(FormatUtils.formatResult(cache));
    }

    return builder.toString();
  }

  private String buildShowRegisterTaskResult() {
    StringBuilder builder = new StringBuilder();

    if (registerTaskInfos != null && !registerTaskInfos.isEmpty()) {
      builder.append("Register task infos:").append("\n");
      List<List<String>> cache = new ArrayList<>();
      cache.add(
          new ArrayList<>(Arrays.asList("NAME", "CLASS_NAME", "FILE_NAME", "IP", "UDF_TYPE")));
      for (RegisterTaskInfo info : registerTaskInfos) {
        cache.add(
            new ArrayList<>(
                Arrays.asList(
                    info.getName(),
                    info.getClassName(),
                    info.getFileName(),
                    info.getIp(),
                    info.getType().toString())));
      }
      builder.append(FormatUtils.formatResult(cache));
    }

    return builder.toString();
  }

  private String buildShowSessionIDResult() {
    StringBuilder builder = new StringBuilder();
    if (sessionIDs != null) {
      builder.append("Session ID List:").append("\n");
      List<List<String>> cache = new ArrayList<>();
      cache.add(new ArrayList<>(Collections.singletonList("SessionID")));
      for (long sessionID : sessionIDs) {
        cache.add(new ArrayList<>(Collections.singletonList(String.valueOf(sessionID))));
      }
      builder.append(FormatUtils.formatResult(cache));
    }
    return builder.toString();
  }

  private String buildShowRulesResult() {
    StringBuilder builder = new StringBuilder();
    if (rules != null) {
      builder.append("Current Rules Info:").append("\n");
      List<List<String>> cache = new ArrayList<>();
      cache.add(new ArrayList<>(Arrays.asList("RuleName", "Status")));
      for (Map.Entry<String, Boolean> entry : rules.entrySet()) {
        cache.add(new ArrayList<>(Arrays.asList(entry.getKey(), entry.getValue() ? "ON" : "OFF")));
      }
      builder.append(FormatUtils.formatResult(cache));
    }
    return builder.toString();
  }

  private String buildShowEligibleJobResult() {
    StringBuilder builder = new StringBuilder();

    if (jobIdList != null) {
      builder.append("Transform Id List:").append("\n");
      List<List<String>> cache = new ArrayList<>();
      cache.add(new ArrayList<>(Collections.singletonList("JobIdList")));
      for (long jobId : jobIdList) {
        cache.add(new ArrayList<>(Collections.singletonList(String.valueOf(jobId))));
      }
      builder.append(FormatUtils.formatResult(cache));
    }

    return builder.toString();
  }

  private List<List<String>> buildShowRegisterTaskListResult() {
    List<List<String>> resList = new ArrayList<>();

    StringBuilder builder = new StringBuilder();

    if (registerTaskInfos != null && !registerTaskInfos.isEmpty()) {
      resList.add(
          new ArrayList<>(Arrays.asList("NAME", "CLASS_NAME", "FILE_NAME", "IP", "UDF_TYPE")));
      for (RegisterTaskInfo info : registerTaskInfos) {
        resList.add(
            new ArrayList<>(
                Arrays.asList(
                    info.getName(),
                    info.getClassName(),
                    info.getFileName(),
                    info.getIp(),
                    info.getType().toString())));
      }
    }
    return resList;
  }

  private List<List<String>> buildShowClusterInfoListResult() {
    List<List<String>> resList = new ArrayList<>();

    if (iginxInfos != null && !iginxInfos.isEmpty()) {
      resList.add(new ArrayList<>(Arrays.asList("IginX infos:")));
      resList.add(new ArrayList<>(Arrays.asList("ID", "IP", "PORT")));
      for (IginxInfo info : iginxInfos) {
        resList.add(
            new ArrayList<>(
                Arrays.asList(
                    String.valueOf(info.getId()), info.getIp(), String.valueOf(info.getPort()))));
      }
    }

    if (storageEngineInfos != null && !storageEngineInfos.isEmpty()) {
      resList.add(new ArrayList<>(Arrays.asList("Storage engine infos:")));
      resList.add(
          new ArrayList<>(
              Arrays.asList("ID", "IP", "PORT", "TYPE", "SCHEMA_PREFIX", "DATA_PREFIX")));
      for (StorageEngineInfo info : storageEngineInfos) {
        resList.add(
            new ArrayList<>(
                Arrays.asList(
                    String.valueOf(info.getId()),
                    info.getIp(),
                    String.valueOf(info.getPort()),
                    info.getType().toString(),
                    info.getSchemaPrefix(),
                    info.getDataPrefix())));
      }
    }

    if (metaStorageInfos != null && !metaStorageInfos.isEmpty()) {
      resList.add(new ArrayList<>(Arrays.asList("Meta Storage infos:")));
      resList.add(new ArrayList<>(Arrays.asList("IP", "PORT", "TYPE")));
      for (MetaStorageInfo info : metaStorageInfos) {
        resList.add(
            new ArrayList<>(
                Arrays.asList(info.getIp(), String.valueOf(info.getPort()), info.getType())));
      }
    }

    if (localMetaStorageInfo != null) {
      resList.add(new ArrayList<>(Arrays.asList("Meta Storage path:")));
      resList.add(new ArrayList<>(Collections.singletonList("PATH")));
      resList.add(new ArrayList<>(Collections.singletonList(localMetaStorageInfo.getPath())));
    }

    return resList;
  }

  public SqlType getSqlType() {
    return sqlType;
  }

  public void setSqlType(SqlType sqlType) {
    this.sqlType = sqlType;
  }

  public long[] getKeys() {
    return keys;
  }

  public void setKeys(long[] keys) {
    this.keys = keys;
  }

  public List<String> getPaths() {
    return paths;
  }

  public void setPaths(List<String> paths) {
    this.paths = paths;
  }

  public List<List<Object>> getValues() {
    return values;
  }

  public void setValues(List<List<Object>> values) {
    this.values = values;
  }

  public List<DataType> getDataTypeList() {
    return dataTypeList;
  }

  public void setDataTypeList(List<DataType> dataTypeList) {
    this.dataTypeList = dataTypeList;
  }

  public int getReplicaNum() {
    return replicaNum;
  }

  public long getPointsNum() {
    return pointsNum;
  }

  public String getParseErrorMsg() {
    return parseErrorMsg;
  }

  public long getJobId() {
    return jobId;
  }

  public JobState getJobState() {
    return jobState;
  }

  public List<RegisterTaskInfo> getRegisterTaskInfos() {
    return registerTaskInfos;
  }

  public String getLoadCsvPath() {
    return loadCsvPath;
  }

  public List<Long> getSessionIDs() {
    return sessionIDs;
  }
}
