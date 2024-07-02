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
package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session_v2.IginXClient;
import cn.edu.tsinghua.iginx.session_v2.IginXClientFactory;
import cn.edu.tsinghua.iginx.session_v2.TransformClient;
import cn.edu.tsinghua.iginx.session_v2.domain.Task;
import cn.edu.tsinghua.iginx.session_v2.domain.Transform;
import cn.edu.tsinghua.iginx.thrift.*;
import java.io.File;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformExample {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransformExample.class);

  private static Session session;
  private static IginXClient client;

  private static final String S1 = "transform.value1";
  private static final String S2 = "transform.value2";
  private static final String S3 = "transform.value3";
  private static final String S4 = "transform.value4";

  private static final String QUERY_SQL = "select value1, value2, value3, value4 from transform;";
  private static final String SHOW_TIME_SERIES_SQL = "SHOW COLUMNS;";
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

  private static final long START_TIMESTAMP = 0L;
  private static final long END_TIMESTAMP = 1000L;

  private static final long TIMEOUT = 10000L;

  private static final Map<String, String> TASK_MAP = new HashMap<>();

  static {
    TASK_MAP.put(
        "\"RowSumTransformer\"",
        "\"" + OUTPUT_DIR_PREFIX + File.separator + "transformer_row_sum.py\"");
    TASK_MAP.put(
        "\"AddOneTransformer\"",
        "\"" + OUTPUT_DIR_PREFIX + File.separator + "transformer_add_one.py\"");
    TASK_MAP.put(
        "\"SumTransformer\"", "\"" + OUTPUT_DIR_PREFIX + File.separator + "transformer_sum.py\"");
  }

  public static void main(String[] args) throws SessionException, InterruptedException {
    before();

    // session
    runWithSession();
    // session v2
    runWithSessionV2();

    after();
  }

  private static void before() throws SessionException {
    session = new Session("127.0.0.1", 6888, "root", "root");
    // 打开 Session
    session.openSession();

    client = IginXClientFactory.create();

    // 准备数据
    session.deleteColumns(Collections.singletonList("*"));
    prepareData();

    // 查询序列
    SessionExecuteSqlResult result = session.executeSql("SHOW COLUMNS");
    result.print(false, "ms");

    // 注册任务
    registerTask();

    // 查询已注册的任务
    result = session.executeSql(SHOW_FUNCTION_SQL);
    result.print(false, "ms");
  }

  private static void after() throws SessionException {
    // 注销任务
    dropTask();

    // 查询已注册的任务
    SessionExecuteSqlResult result = session.executeSql(SHOW_FUNCTION_SQL);
    result.print(false, "ms");

    // 清除数据
    session.deleteColumns(Collections.singletonList("*"));
    // 关闭 Session
    session.closeSession();
  }

  private static void runWithSession() throws SessionException, InterruptedException {
    // 直接输出到文件
    commitWithoutPyTask();

    // 导出到日志
    commitStdJob();

    // 导出到file
    commitFileJob();

    // 综合任务
    commitCombineJob();

    // 混合任务
    commitMixedJob();

    // 导出到IginX
    commitIginXJob();

    // SQL提交
    commitBySQL();
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

  private static void commitWithoutPyTask() throws SessionException, InterruptedException {
    // 构造任务
    List<TaskInfo> taskInfoList = new ArrayList<>();

    TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
    iginxTask.setSqlList(Collections.singletonList(SHOW_TIME_SERIES_SQL));
    taskInfoList.add(iginxTask);

    // 提交任务
    long jobId =
        session.commitTransformJob(
            taskInfoList,
            ExportType.File,
            OUTPUT_DIR_PREFIX + File.separator + "export_file_show_ts.txt");
    System.out.println("job id is " + jobId);

    // 轮询查看任务情况
    JobState jobState = JobState.JOB_CREATED;
    while (!jobState.equals(JobState.JOB_CLOSED)
        && !jobState.equals(JobState.JOB_FAILED)
        && !jobState.equals(JobState.JOB_FINISHED)) {
      Thread.sleep(500);
      jobState = session.queryTransformJobStatus(jobId);
    }
    System.out.println("job state is " + jobState.toString());
  }

  private static void commitStdJob() throws SessionException, InterruptedException {
    // 构造任务
    List<TaskInfo> taskInfoList = new ArrayList<>();

    TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
    iginxTask.setSqlList(Collections.singletonList(QUERY_SQL));
    taskInfoList.add(iginxTask);

    TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
    pyTask.setPyTaskName("RowSumTransformer");
    taskInfoList.add(pyTask);

    // 提交任务
    long jobId = session.commitTransformJob(taskInfoList, ExportType.Log, "");
    System.out.println("job id is " + jobId);

    // 轮询查看任务情况
    JobState jobState = JobState.JOB_CREATED;
    while (!jobState.equals(JobState.JOB_CLOSED)
        && !jobState.equals(JobState.JOB_FAILED)
        && !jobState.equals(JobState.JOB_FINISHED)) {
      Thread.sleep(500);
      jobState = session.queryTransformJobStatus(jobId);
    }
    System.out.println("job state is " + jobState.toString());
  }

  private static void commitFileJob() throws SessionException, InterruptedException {
    // 构造任务
    List<TaskInfo> taskInfoList = new ArrayList<>();

    TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
    iginxTask.setSqlList(Collections.singletonList(QUERY_SQL));
    taskInfoList.add(iginxTask);

    TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
    pyTask.setPyTaskName("RowSumTransformer");
    taskInfoList.add(pyTask);

    // 提交任务
    long jobId =
        session.commitTransformJob(
            taskInfoList, ExportType.File, OUTPUT_DIR_PREFIX + File.separator + "export_file.txt");
    System.out.println("job id is " + jobId);

    // 轮询查看任务情况
    JobState jobState = JobState.JOB_CREATED;
    while (!jobState.equals(JobState.JOB_CLOSED)
        && !jobState.equals(JobState.JOB_FAILED)
        && !jobState.equals(JobState.JOB_FINISHED)) {
      Thread.sleep(500);
      jobState = session.queryTransformJobStatus(jobId);
    }
    System.out.println("job state is " + jobState.toString());
  }

  private static void commitCombineJob() throws SessionException, InterruptedException {
    // 构造任务
    List<TaskInfo> taskInfoList = new ArrayList<>();

    TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
    iginxTask.setSqlList(Collections.singletonList(QUERY_SQL));
    taskInfoList.add(iginxTask);

    TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
    pyTask.setPyTaskName("AddOneTransformer");
    taskInfoList.add(pyTask);

    pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
    pyTask.setPyTaskName("RowSumTransformer");
    taskInfoList.add(pyTask);

    // 提交任务
    long jobId =
        session.commitTransformJob(
            taskInfoList,
            ExportType.File,
            OUTPUT_DIR_PREFIX + File.separator + "export_file_combine.txt");
    System.out.println("job id is " + jobId);

    // 轮询查看任务情况
    JobState jobState = JobState.JOB_CREATED;
    while (!jobState.equals(JobState.JOB_CLOSED)
        && !jobState.equals(JobState.JOB_FAILED)
        && !jobState.equals(JobState.JOB_FINISHED)) {
      Thread.sleep(500);
      jobState = session.queryTransformJobStatus(jobId);
    }
    System.out.println("job state is " + jobState.toString());
  }

  private static void commitMixedJob() throws SessionException, InterruptedException {
    // 构造任务
    List<TaskInfo> taskInfoList = new ArrayList<>();

    TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
    iginxTask.setSqlList(Collections.singletonList(QUERY_SQL));
    taskInfoList.add(iginxTask);

    TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
    pyTask.setPyTaskName("AddOneTransformer");
    taskInfoList.add(pyTask);

    pyTask = new TaskInfo(TaskType.Python, DataFlowType.Batch);
    pyTask.setPyTaskName("SumTransformer");
    taskInfoList.add(pyTask);

    pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
    pyTask.setPyTaskName("RowSumTransformer");
    taskInfoList.add(pyTask);

    // 提交任务
    long jobId =
        session.commitTransformJob(
            taskInfoList,
            ExportType.File,
            OUTPUT_DIR_PREFIX + File.separator + "export_file_sum.txt");
    System.out.println("job id is " + jobId);

    // 轮询查看任务情况
    JobState jobState = JobState.JOB_CREATED;
    while (!jobState.equals(JobState.JOB_CLOSED)
        && !jobState.equals(JobState.JOB_FAILED)
        && !jobState.equals(JobState.JOB_FINISHED)) {
      Thread.sleep(500);
      jobState = session.queryTransformJobStatus(jobId);
    }
    System.out.println("job state is " + jobState.toString());
  }

  private static void commitIginXJob() throws SessionException, InterruptedException {
    // 构造任务
    List<TaskInfo> taskInfoList = new ArrayList<>();

    TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
    iginxTask.setSqlList(Collections.singletonList(QUERY_SQL));
    taskInfoList.add(iginxTask);

    TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
    pyTask.setPyTaskName("RowSumTransformer");
    taskInfoList.add(pyTask);

    // 提交任务
    long jobId = session.commitTransformJob(taskInfoList, ExportType.IginX, "");
    System.out.println("job id is " + jobId);

    // 轮询查看任务情况
    JobState jobState = JobState.JOB_CREATED;
    while (!jobState.equals(JobState.JOB_CLOSED)
        && !jobState.equals(JobState.JOB_FAILED)
        && !jobState.equals(JobState.JOB_FINISHED)) {
      Thread.sleep(500);
      jobState = session.queryTransformJobStatus(jobId);
    }
    System.out.println("job state is " + jobState.toString());

    // 查询序列
    SessionExecuteSqlResult result = session.executeSql("SHOW COLUMNS");
    result.print(false, "ms");
  }

  private static void commitBySQL() throws SessionException, InterruptedException {
    String yamlPath = "\"" + OUTPUT_DIR_PREFIX + File.separator + "TransformJobExample.yaml\"";
    SessionExecuteSqlResult result = session.executeSql("commit transform job " + yamlPath);

    long jobId = result.getJobId();
    // 轮询查看任务情况
    JobState jobState = JobState.JOB_CREATED;
    while (!jobState.equals(JobState.JOB_CLOSED)
        && !jobState.equals(JobState.JOB_FAILED)
        && !jobState.equals(JobState.JOB_FINISHED)) {
      Thread.sleep(500);
      jobState = session.queryTransformJobStatus(jobId);
    }
    System.out.println("job state is " + jobState.toString());
  }

  private static void prepareData() throws SessionException {
    List<String> paths = new ArrayList<>();
    paths.add(S1);
    paths.add(S2);
    paths.add(S3);
    paths.add(S4);

    int size = (int) (END_TIMESTAMP - START_TIMESTAMP);
    long[] timestamps = new long[size];
    Object[] valuesList = new Object[size];
    for (long i = 0; i < size; i++) {
      timestamps[(int) i] = START_TIMESTAMP + i;
      Object[] values = new Object[4];
      for (long j = 0; j < 4; j++) {
        values[(int) j] = i + j;
      }
      valuesList[(int) i] = values;
    }

    List<DataType> dataTypeList = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      dataTypeList.add(DataType.LONG);
    }

    System.out.println("insertRowRecords...");
    session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null);
  }

  private static void runWithSessionV2() throws InterruptedException {

    TransformClient transformClient = client.getTransformClient();
    long jobId =
        transformClient.commitTransformJob(
            Transform.builder()
                .addTask(
                    Task.builder()
                        .dataFlowType(DataFlowType.Stream)
                        .timeout(TIMEOUT)
                        .sql(QUERY_SQL)
                        .build())
                .addTask(
                    Task.builder()
                        .dataFlowType(DataFlowType.Stream)
                        .timeout(TIMEOUT)
                        .pyTaskName("RowSumTransformer")
                        .build())
                .exportToFile(OUTPUT_DIR_PREFIX + File.separator + "export_file_v2.txt")
                .build());

    // 轮询查看任务情况
    JobState jobState = JobState.JOB_CREATED;
    while (!jobState.equals(JobState.JOB_CLOSED)
        && !jobState.equals(JobState.JOB_FAILED)
        && !jobState.equals(JobState.JOB_FINISHED)) {
      Thread.sleep(500);
      jobState = transformClient.queryTransformJobStatus(jobId);
    }
    System.out.println("job state is " + jobState.toString());
  }
}
