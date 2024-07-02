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
package cn.edu.tsinghua.iginx.integration.func.udf;

import static cn.edu.tsinghua.iginx.integration.controller.Controller.SUPPORT_KEY;
import static cn.edu.tsinghua.iginx.integration.controller.Controller.clearAllData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.func.session.InsertAPIType;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.JobFromYAML;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import cn.edu.tsinghua.iginx.utils.YAMLReader;
import cn.edu.tsinghua.iginx.utils.YAMLWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransformIT.class);

  private static Session session;

  private static final String OUTPUT_DIR_PREFIX =
      System.getProperty("user.dir")
          + File.separator
          + "src"
          + File.separator
          + "test"
          + File.separator
          + "resources"
          + File.separator
          + "transform";

  private static final long START_TIMESTAMP = 0L;

  private static final long END_TIMESTAMP = 15000L;

  private static final String SHOW_REGISTER_TASK_SQL = "SHOW FUNCTIONS;";

  private static final String DROP_SQL_FORMATTER = "DROP FUNCTION \"%s\";";

  private static final String CREATE_SQL_FORMATTER =
      "CREATE FUNCTION TRANSFORM \"%s\" FROM \"%s\" IN \"%s\";";

  private static final String COMMIT_SQL_FORMATTER = "COMMIT TRANSFORM JOB \"%s\";";

  private static final String SHOW_TIME_SERIES_SQL = "SHOW COLUMNS;";

  private static final String QUERY_SQL_1 = "SELECT s2 FROM us.d1 WHERE key >= 14800;";

  private static final String QUERY_SQL_2 = "SELECT s1, s2 FROM us.d1 WHERE key < 200;";

  private static final Map<String, String> TASK_MAP = new HashMap<>();

  private static boolean dummyNoData = true;

  private static boolean needCompareResult = true;

  static {
    TASK_MAP.put(
        "RowSumTransformer", OUTPUT_DIR_PREFIX + File.separator + "transformer_row_sum.py");
    TASK_MAP.put(
        "AddOneTransformer", OUTPUT_DIR_PREFIX + File.separator + "transformer_add_one.py");
    TASK_MAP.put("SumTransformer", OUTPUT_DIR_PREFIX + File.separator + "transformer_sum.py");
    TASK_MAP.put("SleepTransformer", OUTPUT_DIR_PREFIX + File.separator + "transformer_sleep.py");
  }

  @BeforeClass
  public static void setUp() throws SessionException {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    if (!SUPPORT_KEY.get(conf.getStorageType()) && conf.isScaling()) {
      needCompareResult = false;
    }
    session = new Session("127.0.0.1", 6888, "root", "root");
    session.openSession();
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    dropAllTask();
    clearAllData(session);
    session.closeSession();
  }

  @Before
  public void insertData() {
    List<String> pathList =
        new ArrayList<String>() {
          {
            add("us.d1.s1");
            add("us.d1.s2");
            add("us.d1.s3");
            add("us.d1.s4");
          }
        };
    List<DataType> dataTypeList =
        new ArrayList<DataType>() {
          {
            add(DataType.LONG);
            add(DataType.LONG);
            add(DataType.BINARY);
            add(DataType.DOUBLE);
          }
        };
    List<Long> keyList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    int size = (int) (END_TIMESTAMP - START_TIMESTAMP);
    for (int i = 0; i < size; i++) {
      keyList.add(START_TIMESTAMP + i);
      valuesList.add(
          Arrays.asList(
              (long) i,
              (long) i + 1,
              ("\"" + RandomStringUtils.randomAlphanumeric(10) + "\"").getBytes(),
              (i + 0.1d)));
    }
    Controller.writeRowsData(
        session,
        pathList,
        keyList,
        dataTypeList,
        valuesList,
        new ArrayList<>(),
        InsertAPIType.Row,
        dummyNoData);
    dummyNoData = false;
    Controller.after(session);
  }

  @After
  public void clearData() {
    Controller.clearData(session);
  }

  private static void dropAllTask() throws SessionException {
    String[] taskList = {
      "RowSumTransformer", "AddOneTransformer", "SumTransformer", "SleepTransformer"
    };
    for (String task : taskList) {
      dropTask(task);
    }
  }

  private static void dropTask(String task) throws SessionException {
    SessionExecuteSqlResult result = session.executeSql(SHOW_REGISTER_TASK_SQL);
    for (RegisterTaskInfo info : result.getRegisterTaskInfos()) {
      if (info.getClassName().equals(task)) {
        session.executeSql(String.format(DROP_SQL_FORMATTER, task));
      }
    }
  }

  private void registerTask(String task) throws SessionException {
    dropTask(task);
    session.executeSql(String.format(CREATE_SQL_FORMATTER, task, task, TASK_MAP.get(task)));
  }

  private void verifyJobState(long jobId) throws SessionException, InterruptedException {
    LOGGER.info("job is {}", jobId);
    JobState jobState = JobState.JOB_CREATED;
    while (!jobState.equals(JobState.JOB_CLOSED)
        && !jobState.equals(JobState.JOB_FAILED)
        && !jobState.equals(JobState.JOB_FINISHED)) {
      Thread.sleep(500);
      jobState = session.queryTransformJobStatus(jobId);
    }
    LOGGER.info("job {} state is {}", jobId, jobState.toString());

    if (!needCompareResult) {
      return;
    }
    assertEquals(JobState.JOB_FINISHED, jobState);

    List<Long> finishedJobIds = session.showEligibleJob(JobState.JOB_FINISHED);
    assertTrue(finishedJobIds.contains(jobId));
  }

  @Test
  public void exportFileWithoutPermissionTest() {
    LOGGER.info("exportFileWithoutPermissionTest");

    List<String> sqlList = new ArrayList<>();
    sqlList.add(QUERY_SQL_2);

    TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
    iginxTask.setSqlList(sqlList);

    List<TaskInfo> taskInfoList = new ArrayList<>();
    taskInfoList.add(iginxTask);

    try {
      String outputFileName = OUTPUT_DIR_PREFIX + File.separator + "output.denied";
      session.commitTransformJob(taskInfoList, ExportType.File, outputFileName);
      fail("Export file without permission should fail.");
    } catch (SessionException e) {
      assertEquals(RpcUtils.ACCESS_DENY.message, e.getMessage());
    }
  }

  @Test
  public void commitSingleSqlStatementTest() {
    LOGGER.info("commitSingleSqlStatementTest");
    List<TaskInfo> taskInfoList = new ArrayList<>();

    TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
    iginxTask.setSqlList(Collections.singletonList(SHOW_TIME_SERIES_SQL));
    taskInfoList.add(iginxTask);

    try {
      long jobId = session.commitTransformJob(taskInfoList, ExportType.Log, "");

      verifyJobState(jobId);
    } catch (SessionException | InterruptedException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  @Test
  public void commitSingleSqlStatementByYamlTest() {
    LOGGER.info("commitSingleSqlStatementByYamlTest");
    try {
      String yamlFileName = OUTPUT_DIR_PREFIX + File.separator + "TransformSingleSqlStatement.yaml";
      SessionExecuteSqlResult result =
          session.executeSql(String.format(COMMIT_SQL_FORMATTER, yamlFileName));

      long jobId = result.getJobId();
      verifyJobState(jobId);
    } catch (SessionException | InterruptedException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  @Test
  public void commitMultipleSqlStatementsTest() {
    LOGGER.info("commitMultipleSqlStatementsTest");
    List<TaskInfo> taskInfoList = new ArrayList<>();

    TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
    List<String> sqlList = new ArrayList<>();
    String insertStrPrefix = "INSERT INTO us.d1 (key, s2) values ";
    StringBuilder builder = new StringBuilder(insertStrPrefix);
    for (int i = 0; i < 100; i++) {
      builder.append("(");
      builder.append(END_TIMESTAMP + i).append(", ");
      builder.append(END_TIMESTAMP + i + 1);
      builder.append(")");
    }
    builder.append(";");
    sqlList.add(builder.toString());
    sqlList.add(QUERY_SQL_1);
    iginxTask.setSqlList(sqlList);
    taskInfoList.add(iginxTask);

    try {
      String outputFileName =
          OUTPUT_DIR_PREFIX + File.separator + "export_file_multiple_sql_statements.txt";
      long jobId = session.commitTransformJob(taskInfoList, ExportType.File, outputFileName);

      verifyJobState(jobId);
      verifyMultipleSqlStatements(outputFileName);
    } catch (SessionException | InterruptedException | IOException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  @Test
  public void commitMultipleSqlStatementsByYamlTest() {
    LOGGER.info("commitMultipleSqlStatementsByYamlTest");
    try {
      String yamlFileName =
          OUTPUT_DIR_PREFIX + File.separator + "TransformMultipleSqlStatements.yaml";
      String outputFileName =
          OUTPUT_DIR_PREFIX + File.separator + "export_file_multiple_sql_statements_by_yaml.txt";
      SessionExecuteSqlResult result =
          session.executeSql(String.format(COMMIT_SQL_FORMATTER, yamlFileName));
      long jobId = result.getJobId();

      verifyJobState(jobId);
      verifyMultipleSqlStatements(outputFileName);
    } catch (SessionException | InterruptedException | IOException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  private void verifyMultipleSqlStatements(String outputFileName) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(outputFileName));
    String line = reader.readLine();
    String[] parts = line.split(",");

    if (!needCompareResult) {
      return;
    }
    assertEquals(GlobalConstant.KEY_NAME, parts[0]);
    assertEquals("us.d1.s2", parts[1]);

    int index = 0;
    while ((line = reader.readLine()) != null) {
      parts = line.split(",");
      assertEquals(14800 + index, Long.parseLong(parts[0]));
      assertEquals(14800 + index + 1, Long.parseLong(parts[1]));
      index++;
    }
    reader.close();

    assertEquals(300, index);
    assertTrue(Files.deleteIfExists(Paths.get(outputFileName)));
  }

  @Test
  public void commitSinglePythonJobTest() {
    LOGGER.info("commitSinglePythonJobTest");
    try {
      String task = "RowSumTransformer";
      registerTask(task);

      List<TaskInfo> taskInfoList = new ArrayList<>();

      TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
      iginxTask.setSqlList(Collections.singletonList(QUERY_SQL_2));

      TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
      pyTask.setPyTaskName("RowSumTransformer");

      taskInfoList.add(iginxTask);
      taskInfoList.add(pyTask);

      String outputFileName =
          OUTPUT_DIR_PREFIX + File.separator + "export_file_single_python_job.txt";
      long jobId = session.commitTransformJob(taskInfoList, ExportType.File, outputFileName);

      verifyJobState(jobId);
      verifySinglePythonJob(outputFileName);
    } catch (SessionException | InterruptedException | IOException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  @Test
  public void commitSinglePythonJobByYamlTest() {
    LOGGER.info("commitSinglePythonJobByYamlTest");
    try {
      String task = "RowSumTransformer";
      registerTask(task);

      String yamlFileName = OUTPUT_DIR_PREFIX + File.separator + "TransformSinglePythonJob.yaml";
      String outputFileName =
          OUTPUT_DIR_PREFIX + File.separator + "export_file_single_python_job_by_yaml.txt";
      SessionExecuteSqlResult result =
          session.executeSql(String.format(COMMIT_SQL_FORMATTER, yamlFileName));
      long jobId = result.getJobId();

      verifyJobState(jobId);
      verifySinglePythonJob(outputFileName);
    } catch (SessionException | InterruptedException | IOException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  private void verifySinglePythonJob(String outputFileName) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(outputFileName));
    String line = reader.readLine();
    String[] parts = line.split(",");

    if (!needCompareResult) {
      return;
    }
    assertEquals(GlobalConstant.KEY_NAME, parts[0]);
    assertEquals("sum", parts[1]);

    int index = 0;
    while ((line = reader.readLine()) != null) {
      parts = line.split(",");
      assertEquals(index, Long.parseLong(parts[0]));
      assertEquals(index + index + 1, Long.parseLong(parts[1]));
      index++;
    }
    reader.close();

    assertEquals(200, index);
    assertTrue(Files.deleteIfExists(Paths.get(outputFileName)));
  }

  @Test
  public void commitMultiplePythonJobsTest() {
    LOGGER.info("commitMultiplePythonJobsTest");
    try {
      String[] taskList = {"RowSumTransformer", "AddOneTransformer"};
      for (String task : taskList) {
        registerTask(task);
      }

      List<TaskInfo> taskInfoList = new ArrayList<>();

      TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
      iginxTask.setSqlList(Collections.singletonList(QUERY_SQL_2));

      TaskInfo addOnePyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
      addOnePyTask.setPyTaskName("AddOneTransformer");

      TaskInfo rowSumPyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
      rowSumPyTask.setPyTaskName("RowSumTransformer");

      taskInfoList.add(iginxTask);
      taskInfoList.add(addOnePyTask);
      taskInfoList.add(rowSumPyTask);

      String outputFileName =
          OUTPUT_DIR_PREFIX + File.separator + "export_file_multiple_python_jobs.txt";
      long jobId = session.commitTransformJob(taskInfoList, ExportType.File, outputFileName);

      verifyJobState(jobId);
      verifyMultiplePythonJobs(outputFileName);
    } catch (SessionException | InterruptedException | IOException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  @Test
  public void commitMultiplePythonJobsByYamlTest() {
    LOGGER.info("commitMultiplePythonJobsByYamlTest");
    try {
      String[] taskList = {"RowSumTransformer", "AddOneTransformer"};
      for (String task : taskList) {
        registerTask(task);
      }

      String yamlFileName = OUTPUT_DIR_PREFIX + File.separator + "TransformMultiplePythonJobs.yaml";
      String outputFileName =
          OUTPUT_DIR_PREFIX + File.separator + "export_file_multiple_python_jobs_by_yaml.txt";
      SessionExecuteSqlResult result =
          session.executeSql(String.format(COMMIT_SQL_FORMATTER, yamlFileName));
      long jobId = result.getJobId();

      verifyJobState(jobId);
      verifyMultiplePythonJobs(outputFileName);
    } catch (SessionException | InterruptedException | IOException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  @Test
  public void commitMultiplePythonJobsByYamlWithExportToIginxTest() {
    LOGGER.info("commitMultiplePythonJobsByYamlWithExportToIginxTest");
    try {
      String[] taskList = {"RowSumTransformer", "AddOneTransformer"};
      for (String task : taskList) {
        registerTask(task);
      }

      String yamlFileName =
          OUTPUT_DIR_PREFIX + File.separator + "TransformMultiplePythonJobsWithExportToIginx.yaml";
      SessionExecuteSqlResult result =
          session.executeSql(String.format(COMMIT_SQL_FORMATTER, yamlFileName));
      long jobId = result.getJobId();

      verifyJobState(jobId);

      SessionExecuteSqlResult queryResult = session.executeSql("SELECT * FROM transform;");
      int timeIndex = queryResult.getPaths().indexOf("transform.key");
      int sumIndex = queryResult.getPaths().indexOf("transform.sum");
      if (needCompareResult) {
        assertNotEquals(-1, timeIndex);
        assertNotEquals(-1, sumIndex);
      }

      verifyMultiplePythonJobs(queryResult, timeIndex, sumIndex, 200);
    } catch (SessionException | InterruptedException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  // no need to write query result into txt file then read file and compare.
  private void verifyMultiplePythonJobs(
      SessionExecuteSqlResult queryResult, int timeIndex, int sumIndex, int lineCount) {
    long index = 0;
    for (List<Object> row : queryResult.getValues()) {
      assertEquals(index + 1, row.get(timeIndex));
      assertEquals(index + 1 + index + 1 + 1, row.get(sumIndex));
      index++;
    }
    assertEquals(lineCount, index);
  }

  private void verifyMultiplePythonJobs(String outputFileName) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(outputFileName));
    String line = reader.readLine();
    String[] parts = line.split(",");

    if (!needCompareResult) {
      return;
    }
    assertEquals(GlobalConstant.KEY_NAME, parts[0]);
    assertEquals("sum", parts[1]);

    int index = 0;
    while ((line = reader.readLine()) != null) {
      parts = line.split(",");
      assertEquals(index + 1, Long.parseLong(parts[0]));
      assertEquals(index + 1 + index + 1 + 1, Long.parseLong(parts[1]));
      index++;
    }
    reader.close();

    assertEquals(200, index);
    assertTrue(Files.deleteIfExists(Paths.get(outputFileName)));
  }

  @Test
  public void commitMixedPythonJobsTest() {
    LOGGER.info("commitMixedPythonJobsTest");
    try {
      String[] taskList = {"RowSumTransformer", "AddOneTransformer", "SumTransformer"};
      for (String task : taskList) {
        registerTask(task);
      }

      List<TaskInfo> taskInfoList = new ArrayList<>();

      TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
      iginxTask.setSqlList(Collections.singletonList(QUERY_SQL_2));

      TaskInfo addOnePyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
      addOnePyTask.setPyTaskName("AddOneTransformer");

      TaskInfo sumPyTask = new TaskInfo(TaskType.Python, DataFlowType.Batch);
      sumPyTask.setPyTaskName("SumTransformer");

      TaskInfo rowSumPyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
      rowSumPyTask.setPyTaskName("RowSumTransformer");

      taskInfoList.add(iginxTask);
      taskInfoList.add(addOnePyTask);
      taskInfoList.add(sumPyTask);
      taskInfoList.add(rowSumPyTask);

      String outputFileName =
          OUTPUT_DIR_PREFIX + File.separator + "export_file_mixed_python_jobs.txt";
      long jobId = session.commitTransformJob(taskInfoList, ExportType.File, outputFileName);

      verifyJobState(jobId);
      verifyMixedPythonJobs(outputFileName);
    } catch (SessionException | InterruptedException | IOException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  @Test
  public void commitMixedPythonJobsByYamlTest() {
    LOGGER.info("commitMixedPythonJobsByYamlTest");
    try {
      String[] taskList = {"RowSumTransformer", "AddOneTransformer", "SumTransformer"};
      for (String task : taskList) {
        registerTask(task);
      }

      String yamlFileName = OUTPUT_DIR_PREFIX + File.separator + "TransformMixedPythonJobs.yaml";
      String outputFileName =
          OUTPUT_DIR_PREFIX + File.separator + "export_file_mixed_python_jobs_by_yaml.txt";
      SessionExecuteSqlResult result =
          session.executeSql(String.format(COMMIT_SQL_FORMATTER, yamlFileName));
      long jobId = result.getJobId();

      verifyJobState(jobId);
      verifyMixedPythonJobs(outputFileName);
    } catch (SessionException | InterruptedException | IOException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  @Test
  public void commitMixedPythonJobsByYamlWithRegisterTest() {
    LOGGER.info("commitMixedPythonJobsByYamlWithRegisterTest");
    try {
      String[] taskList = {"RowSumTransformer", "AddOneTransformer", "SumTransformer"};
      for (String task : taskList) {
        dropTask(task);
      }

      String yamlFileName =
          OUTPUT_DIR_PREFIX + File.separator + "TransformMixedPythonJobsWithRegister.yaml";
      String outputFileName =
          OUTPUT_DIR_PREFIX
              + File.separator
              + "export_file_mixed_python_jobs_with_register_by_yaml.txt";
      YAMLReader reader = new YAMLReader(yamlFileName);
      JobFromYAML job = reader.getJobFromYAML();
      replaceRelativePythonPathToAbsolute(job);
      YAMLWriter writer = new YAMLWriter();
      writer.writeJobIntoYAML(new File(yamlFileName), job);

      SessionExecuteSqlResult result =
          session.executeSql(String.format(COMMIT_SQL_FORMATTER, yamlFileName));
      long jobId = result.getJobId();

      verifyJobState(jobId);
      verifyMixedPythonJobs(outputFileName);
    } catch (SessionException | InterruptedException | IOException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  // replace relative python filepath in sql with absolute paths for UDF registration.
  private void replaceRelativePythonPathToAbsolute(JobFromYAML job) {
    // match string between two double quotes
    Pattern pattern = Pattern.compile("\"([^\"]*)\"");
    job.getTaskList()
        .forEach(
            task -> {
              if (task.getSqlList() == null) {
                return;
              }
              List<String> newSqlList = new ArrayList<>();
              task.getSqlList()
                  .forEach(
                      sql -> {
                        String oriPath;
                        Matcher matcher = pattern.matcher(sql);
                        while (matcher.find()) {
                          oriPath = matcher.group(1);
                          if (!oriPath.endsWith(".py")) {
                            continue;
                          }
                          File filePath = new File(oriPath);
                          if (!filePath.isAbsolute()) {
                            sql = sql.replace(oriPath, filePath.getAbsolutePath());
                          }
                        }
                        newSqlList.add(sql);
                      });
              task.setSqlList(newSqlList);
            });
  }

  private void verifyMixedPythonJobs(String outputFileName) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(outputFileName));
    String line = reader.readLine();
    String[] parts = line.split(",");

    if (!needCompareResult) {
      return;
    }
    assertEquals(GlobalConstant.KEY_NAME, parts[0]);
    assertEquals("sum", parts[1]);

    line = reader.readLine();
    parts = line.split(",");
    reader.close();

    assertEquals(20100, Long.parseLong(parts[0]));
    assertEquals(40400, Long.parseLong(parts[1]));
    assertTrue(Files.deleteIfExists(Paths.get(outputFileName)));
  }

  //    @Test
  public void cancelJobTest() {
    LOGGER.info("cancelJobTest");
    try {
      String task = "SleepTransformer";
      registerTask(task);

      List<TaskInfo> taskInfoList = new ArrayList<>();

      TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
      iginxTask.setSqlList(Collections.singletonList(QUERY_SQL_2));

      TaskInfo sleepPyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
      sleepPyTask.setPyTaskName("SleepTransformer");

      taskInfoList.add(iginxTask);
      taskInfoList.add(sleepPyTask);

      long jobId = session.commitTransformJob(taskInfoList, ExportType.Log, "");
      LOGGER.info("job is {}", jobId);
      JobState jobState = session.queryTransformJobStatus(jobId);
      LOGGER.info("job {} state is {}", jobId, jobState.toString());

      session.cancelTransformJob(jobId);
      jobState = session.queryTransformJobStatus(jobId);
      LOGGER.info("After cancellation, job {} state is {}", jobId, jobState.toString());
      assertEquals(JobState.JOB_CLOSED, jobState);

      List<Long> closedJobIds = session.showEligibleJob(JobState.JOB_CLOSED);
      assertTrue(closedJobIds.contains(jobId));
    } catch (SessionException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }
}
