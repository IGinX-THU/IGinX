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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.thrift.*;
import java.io.File;
import java.util.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformCompare {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransformCompare.class);

  private static Session session;

  private static final long START_TIMESTAMP = 0L;

  private static final long END_TIMESTAMP = 150000L;

  private static final int RETRY_TIMES = 5;

  private static final List<String> FUNC_LIST = Arrays.asList("min", "max", "sum", "avg", "count");

  private static final String SHOW_FUNCTION_SQL = "SHOW FUNCTIONS;";
  private static final String CREATE_SQL_FORMATTER = "CREATE FUNCTION TRANSFORM %s FROM %s IN %s";
  private static final String DROP_SQL_FORMATTER = "DROP FUNCTION %s";

  private static final String OUTPUT_DIR_PREFIX =
      System.getProperty("user.dir")
          + File.separator
          + "example"
          + File.separator
          + "src"
          + File.separator
          + "main"
          + File.separator
          + "resources";

  private static final Map<String, String> TASK_MAP = new HashMap<>();

  static {
    TASK_MAP.put(
        "\"MaxTransformer\"", "\"" + OUTPUT_DIR_PREFIX + File.separator + "transformer_max.py\"");
    TASK_MAP.put(
        "\"MinTransformer\"", "\"" + OUTPUT_DIR_PREFIX + File.separator + "transformer_min.py\"");
    TASK_MAP.put(
        "\"SumTransformer\"", "\"" + OUTPUT_DIR_PREFIX + File.separator + "transformer_sum.py\"");
    TASK_MAP.put(
        "\"AvgTransformer\"", "\"" + OUTPUT_DIR_PREFIX + File.separator + "transformer_avg.py\"");
    TASK_MAP.put(
        "\"CountTransformer\"",
        "\"" + OUTPUT_DIR_PREFIX + File.separator + "transformer_count.py\"");
  }

  public static void main(String[] args) throws SessionException, InterruptedException {
    before();

    String multiPathWholeRange = "SELECT s1, s2 FROM test.compare;";
    //        commitStdJob(multiPathWholeRange, "MaxTransformer");
    //        commitStdJob(multiPathWholeRange, "MinTransformer");
    //        commitStdJob(multiPathWholeRange, "SumTransformer");
    //        commitStdJob(multiPathWholeRange, "AvgTransformer");
    //        commitStdJob(multiPathWholeRange, "CountTransformer");

    String singlePathWholeRange = "SELECT s1 FROM test.compare;";
    //        commitStdJob(singlePathWholeRange, "MaxTransformer");
    //        commitStdJob(singlePathWholeRange, "MinTransformer");
    //        commitStdJob(singlePathWholeRange, "SumTransformer");
    //        commitStdJob(singlePathWholeRange, "AvgTransformer");
    //        commitStdJob(singlePathWholeRange, "CountTransformer");

    String multiPathPartialRange = "SELECT s1, s2 FROM test.compare WHERE time < 200;";
    //        commitStdJob(multiPathPartialRange, "MaxTransformer");
    //        commitStdJob(multiPathPartialRange, "MinTransformer");
    //        commitStdJob(multiPathPartialRange, "SumTransformer");
    //        commitStdJob(multiPathPartialRange, "AvgTransformer");
    //        commitStdJob(multiPathPartialRange, "CountTransformer");

    String singlePathPartialRange = "SELECT s1 FROM test.compare WHERE time < 200;";
    commitStdJob(singlePathPartialRange, "MaxTransformer");
    commitStdJob(singlePathPartialRange, "MinTransformer");
    commitStdJob(singlePathPartialRange, "SumTransformer");
    commitStdJob(singlePathPartialRange, "AvgTransformer");
    commitStdJob(singlePathPartialRange, "CountTransformer");

    after();
  }

  private static void commitStdJob(String sql, String pyTaskName)
      throws SessionException, InterruptedException {
    List<TaskInfo> taskInfoList = new ArrayList<>();

    TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
    iginxTask.setSqlList(Collections.singletonList(sql));
    taskInfoList.add(iginxTask);

    TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
    pyTask.setPyTaskName(pyTaskName);
    taskInfoList.add(pyTask);

    // 提交任务
    long jobId = session.commitTransformJob(taskInfoList, ExportType.Log, "");
    // 轮询查看任务情况
    JobState jobState = JobState.JOB_CREATED;
    while (!jobState.equals(JobState.JOB_CLOSED)
        && !jobState.equals(JobState.JOB_FAILED)
        && !jobState.equals(JobState.JOB_FINISHED)) {
      Thread.sleep(50);
      jobState = session.queryTransformJobStatus(jobId);
    }
    System.out.println("job " + jobId + " state is " + jobState.toString());
  }

  private static void before() throws SessionException {
    setUp();
    insertData();

    registerTask();
    // 查询已注册的任务
    SessionExecuteSqlResult result = session.executeSql(SHOW_FUNCTION_SQL);
    result.print(false, "ms");
  }

  private static void after() throws SessionException {
    dropTask();
    // 查询已注册的任务
    SessionExecuteSqlResult result = session.executeSql(SHOW_FUNCTION_SQL);
    result.print(false, "ms");

    clearData();
    tearDown();
  }

  private static void setUp() {
    session = new Session("127.0.0.1", 6888, "root", "root");
    try {
      session.openSession();
    } catch (SessionException e) {
      System.out.println(e.getMessage());
    }
  }

  private static void tearDown() {
    try {
      session.closeSession();
    } catch (SessionException e) {
      System.out.println(e.getMessage());
    }
  }

  private static void registerTask() {
    TASK_MAP.forEach(
        (k, v) -> {
          String registerSQL = String.format(CREATE_SQL_FORMATTER, k, k, v);
          try {
            session.executeSql(registerSQL);
          } catch (Exception e) {
            LOGGER.error("unexpected error: ", e);
          }
        });
  }

  private static void dropTask() {
    TASK_MAP.forEach(
        (k, v) -> {
          String registerSQL = String.format(DROP_SQL_FORMATTER, k);
          try {
            session.executeSql(registerSQL);
          } catch (Exception e) {
            LOGGER.error("unexpected error: ", e);
          }
        });
  }

  private static void insertData() throws SessionException {
    String insertStrPrefix = "INSERT INTO test.compare (key, s1, s2, s3, s4) values ";

    StringBuilder builder = new StringBuilder(insertStrPrefix);

    int size = (int) (END_TIMESTAMP - START_TIMESTAMP);
    for (int i = 0; i < size; i++) {
      builder.append(", ");
      builder.append("(");
      builder.append(START_TIMESTAMP + i).append(", ");
      builder.append(i).append(", ");
      builder.append(i + 1).append(", ");
      builder
          .append("\"")
          .append(new String(RandomStringUtils.randomAlphanumeric(10).getBytes()))
          .append("\", ");
      builder.append((i + 0.1));
      builder.append(")");
    }
    builder.append(";");

    String insertStatement = builder.toString();

    SessionExecuteSqlResult res = session.executeSql(insertStatement);
    if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      System.out.println("Insert date execute fail. Caused by: " + res.getParseErrorMsg());
    }
  }

  private static void clearData() throws SessionException {
    String clearData = "CLEAR DATA;";

    SessionExecuteSqlResult res = session.executeSql(clearData);
    if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      System.out.println("Clear date execute fail. Caused by: " + res.getParseErrorMsg());
    }
  }
}
