/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.integration.func.udf;

import static cn.edu.tsinghua.iginx.integration.controller.Controller.SUPPORT_KEY;
import static cn.edu.tsinghua.iginx.integration.controller.Controller.clearAllData;
import static cn.edu.tsinghua.iginx.integration.tool.TestUtils.downloadFile;
import static cn.edu.tsinghua.iginx.utils.FileUtils.appendFile;
import static cn.edu.tsinghua.iginx.utils.FileUtils.deleteFileOrDir;
import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.func.session.InsertAPIType;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.*;
import cn.hutool.core.collection.CollectionUtil;
import com.icegreen.greenmail.junit4.GreenMailRule;
import com.icegreen.greenmail.util.ServerSetupTest;
import java.io.*;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.mail.MessagingException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.*;
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

  private static final String DOWNLOAD_DIR =
      System.getProperty("user.dir")
          + File.separator
          + "src"
          + File.separator
          + "test"
          + File.separator
          + "resources"
          + File.separator
          + "downloads";

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

  private static final String QUERY_SQL_3 = "SELECT s1, s2 FROM us.d1 WHERE key < 10;";

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
    TASK_MAP.put(
        "ToBytesTransformer", OUTPUT_DIR_PREFIX + File.separator + "transformer_to_bytes.py");
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
    for (String task : TASK_MAP.keySet()) {
      dropTask(task);
    }
  }

  private static void dropTask(String task) throws SessionException {
    SessionExecuteSqlResult result = session.executeSql(SHOW_REGISTER_TASK_SQL);
    for (RegisterTaskInfo info : result.getRegisterTaskInfos()) {
      if (info.getName().equals(task)) {
        session.executeSql(String.format(DROP_SQL_FORMATTER, task));
      }
    }
  }

  private void registerTask(String task) throws SessionException {
    dropTask(task);
    session.executeSql(String.format(CREATE_SQL_FORMATTER, task, task, TASK_MAP.get(task)));
  }

  private void registerTask(String task, String className, String filename)
      throws SessionException {
    dropTask(task);
    session.executeSql(String.format(CREATE_SQL_FORMATTER, task, className, filename));
  }

  /**
   * verity job state instantly without waiting
   *
   * @param expectedState specified: check whether job is in this state; null: check all status
   */
  private void verifyJobState(long jobId, JobState expectedState) throws SessionException {
    LOGGER.info(
        "Querying job({})'s state(expected:{} )...",
        jobId,
        expectedState == null ? "Any" : expectedState);
    Map<JobState, List<Long>> jobStateListMap = session.showEligibleJob(null);
    for (Map.Entry<JobState, List<Long>> entry : jobStateListMap.entrySet()) {
      if (expectedState != null && entry.getKey() != expectedState) {
        // skip unwanted states
        continue;
      }
      for (Long jobId2 : entry.getValue()) {
        if (jobId == jobId2) {
          LOGGER.info("Query succeeded.");
          return;
        }
      }
    }
    JobState state = session.queryTransformJobStatus(jobId);
    // not found
    LOGGER.error(
        "Job({}) is not found in list of \"{}\" state. Current state: {}",
        jobId,
        expectedState == null ? "Any" : expectedState,
        state);
    fail();
  }

  /** will wait until job finishes/failed/closed and check whether finished */
  private void verifyJobFinishedBlocked(long jobId) throws SessionException, InterruptedException {
    LOGGER.info("job is {}", jobId);
    JobState jobState = JobState.JOB_CREATED;
    int timeout = 60000; // 60s
    while (!jobState.equals(JobState.JOB_CLOSED)
        && !jobState.equals(JobState.JOB_FAILED)
        && !jobState.equals(JobState.JOB_FINISHED)
        && timeout > 0) {
      Thread.sleep(500);
      timeout -= 500;
      jobState = session.queryTransformJobStatus(jobId);
    }
    LOGGER.info("job {} state is {}", jobId, jobState.toString());
    if (timeout <= 0) {
      LOGGER.error("job is stuck, check IGinX server log...");
      fail();
    }

    if (!needCompareResult) {
      return;
    }
    assertEquals(JobState.JOB_FINISHED, jobState);

    List<Long> finishedJobIds =
        session.showEligibleJob(JobState.JOB_FINISHED).get(JobState.JOB_FINISHED);
    assertTrue(finishedJobIds.contains(jobId));
  }

  private void cancelJob(long jobID) {
    try {
      JobState jobState;
      session.cancelTransformJob(jobID);
      jobState = session.queryTransformJobStatus(jobID);
      LOGGER.info("After cancellation, job {} state is {}", jobID, jobState.toString());
      assertEquals(JobState.JOB_CLOSED, jobState);

      List<Long> closedJobIds =
          session.showEligibleJob(JobState.JOB_CLOSED).get(JobState.JOB_CLOSED);
      assertTrue(closedJobIds.contains(jobID));
    } catch (SessionException e) {
      LOGGER.error("Failed to cancel job: {}", jobID, e);
      fail();
    }
  }

  @Test
  public void exportFileWithoutPermissionTest() {
    LOGGER.info("exportFileWithoutPermissionTest");

    List<String> sqlList = new ArrayList<>();
    sqlList.add(QUERY_SQL_2);

    TaskInfo iginxTask = new TaskInfo(TaskType.IGINX, DataFlowType.STREAM);
    iginxTask.setSqlList(sqlList);

    List<TaskInfo> taskInfoList = new ArrayList<>();
    taskInfoList.add(iginxTask);

    try {
      String outputFileName = OUTPUT_DIR_PREFIX + File.separator + "output.denied";
      session.commitTransformJob(taskInfoList, ExportType.FILE, outputFileName);
      fail("Export file without permission should fail.");
    } catch (SessionException e) {
      assertEquals(RpcUtils.ACCESS_DENY.message, e.getMessage());
    }
  }

  @Test
  public void commitSingleSqlStatementTest() {
    LOGGER.info("commitSingleSqlStatementTest");
    List<TaskInfo> taskInfoList = new ArrayList<>();

    TaskInfo iginxTask = new TaskInfo(TaskType.IGINX, DataFlowType.STREAM);
    iginxTask.setSqlList(Collections.singletonList(SHOW_TIME_SERIES_SQL));
    taskInfoList.add(iginxTask);

    try {
      long jobId = session.commitTransformJob(taskInfoList, ExportType.LOG, "");

      verifyJobFinishedBlocked(jobId);

      // additional test: cancel finished job
      redundantCancellationTest(jobId);
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
      long jobId = session.commitTransformJob(String.format(COMMIT_SQL_FORMATTER, yamlFileName));
      verifyJobFinishedBlocked(jobId);
    } catch (SessionException | InterruptedException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  @Test
  public void commitScheduledYamlTestAfter10s() {
    LOGGER.info("commitScheduledYamlTest(after 10s)");
    String outputFileName = OUTPUT_DIR_PREFIX + File.separator + "export_file_after_10_s.txt";
    try {
      String yamlFileName = OUTPUT_DIR_PREFIX + File.separator + "TransformScheduledAfter10s.yaml";
      long jobId = session.commitTransformJob(String.format(COMMIT_SQL_FORMATTER, yamlFileName));

      Thread.sleep(3000L); // sleep 3s to delay insertion
      String insertSQL = "insert into scheduleData(key, %s) values(1, 2);";
      // add col0
      session.executeSql(String.format(insertSQL, "col0"));

      // job can be queried in all/idle jobs
      verifyJobState(jobId, null);
      verifyJobState(jobId, JobState.JOB_IDLE);

      verifyJobFinishedBlocked(jobId);
      // job will finish after 10 seconds

      // check whether new column is in job result
      fileResultContains(outputFileName, "col0");
    } catch (SessionException | InterruptedException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    } finally {
      try {
        assertTrue(Files.deleteIfExists(Paths.get(outputFileName)));
      } catch (IOException e) {
        LOGGER.error("Fail to delete result file: {}", outputFileName, e);
        fail();
      }
    }
  }

  @Test
  public void commitScheduledYamlTestEvery10sAndCancel() {
    LOGGER.info("commitScheduledYamlTest(every 10s) and cancel");
    String outputFileName = OUTPUT_DIR_PREFIX + File.separator + "export_file_every_10_s.txt";
    try {
      String insertSQL = "insert into scheduleData(key, %s) values(1, 2);";
      String yamlFileName = OUTPUT_DIR_PREFIX + File.separator + "TransformScheduledEvery10s.yaml";
      // add col0
      session.executeSql(String.format(insertSQL, "col0"));
      long jobId = session.commitTransformJob(String.format(COMMIT_SQL_FORMATTER, yamlFileName));
      try {

        Thread.sleep(3000L); // sleep 3s to make sure first try is triggered.
        LOGGER.info("Verifying 0th try...");
        fileResultContains(outputFileName, "col0");

        // add col1, col2 and verify res are changed
        for (int i = 1; i < 3; i++) {
          session.executeSql(String.format(insertSQL, "col" + i));
          // job can be queried in all/idle jobs
          verifyJobState(jobId, null);
          verifyJobState(jobId, JobState.JOB_IDLE);
          Thread.sleep(10000L); // sleep 10s to make sure next try is triggered.
          LOGGER.info("Verifying " + i + "th try...");
          fileResultContains(outputFileName, "col" + i);
        }
      } finally {
        cancelJob(jobId);
        assertTrue(Files.deleteIfExists(Paths.get(outputFileName)));

        // additional test: cancel closed job
        redundantCancellationTest(jobId);
      }
    } catch (SessionException | InterruptedException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    } catch (IOException e) {
      LOGGER.error("Fail to delete result file: {}", outputFileName, e);
      fail();
    }
  }

  @Test
  public void commitStopOnFailureTest() {
    LOGGER.info("commitStopOnFailureTest");
    try {
      dropAllTask();
      List<TaskInfo> taskInfoList = new ArrayList<>();

      TaskInfo iginxTask = new TaskInfo(TaskType.IGINX, DataFlowType.STREAM);
      iginxTask.setSqlList(Collections.singletonList("SELECT s1, s2 FROM us.d1 WHERE key < 10;"));

      TaskInfo pyTask = new TaskInfo(TaskType.PYTHON, DataFlowType.STREAM);
      pyTask.setPyTaskName("RowSumTransformer"); // not registered, job should fail

      taskInfoList.add(iginxTask);
      taskInfoList.add(pyTask);

      String schedule = "every 10 second";
      long jobId = session.commitTransformJob(taskInfoList, ExportType.LOG, "", schedule);

      Thread.sleep(5000); // triggered
      verifyJobState(jobId, JobState.JOB_FAILED); // verify failed
    } catch (SessionException | InterruptedException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
    }
  }

  @Test
  public void commitContinueOnFailureTest() {
    LOGGER.info("commitContinueOnFailureTest");
    try {
      dropAllTask();
      List<TaskInfo> taskInfoList = new ArrayList<>();

      TaskInfo iginxTask = new TaskInfo(TaskType.IGINX, DataFlowType.STREAM);
      iginxTask.setSqlList(Collections.singletonList("SELECT s1, s2 FROM us.d1 WHERE key < 10;"));

      TaskInfo pyTask = new TaskInfo(TaskType.PYTHON, DataFlowType.STREAM);
      pyTask.setPyTaskName("RowSumTransformer"); // at 1st try, udf is not registered.

      taskInfoList.add(iginxTask);
      taskInfoList.add(pyTask);

      String schedule = "every 20 second";

      String outputFileName =
          OUTPUT_DIR_PREFIX + File.separator + "export_file_continue_on_failure.txt";
      long jobId =
          session.commitTransformJob(
              taskInfoList, ExportType.FILE, outputFileName, schedule, false);

      try {
        testContinueOnFailure(jobId, outputFileName);
      } finally {
        cancelJob(jobId);
      }
    } catch (SessionException | InterruptedException | IOException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  @Test
  public void commitContinueOnFailureByYamlTest() {
    // same step as commitContinueOnFailureTest but by yaml
    LOGGER.info("commitContinueOnFailureByYamlTest");
    try {
      dropAllTask();
      String yamlFileName =
          OUTPUT_DIR_PREFIX + File.separator + "TransformScheduledEvery10sWrong.yaml";
      long jobId = session.commitTransformJob(String.format(COMMIT_SQL_FORMATTER, yamlFileName));
      String outputFileName =
          OUTPUT_DIR_PREFIX + File.separator + "export_file_continue_on_failure.txt";
      try {
        testContinueOnFailure(jobId, outputFileName);
      } finally {
        cancelJob(jobId);
      }
    } catch (SessionException | InterruptedException | IOException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  private void testContinueOnFailure(long jobId, String outputFileName)
      throws InterruptedException, SessionException, IOException {
    Thread.sleep(13000L); // wait for 1st try to fail
    verifyJobState(jobId, null);
    verifyJobState(jobId, JobState.JOB_PARTIALLY_FAILED);

    // register udf before 2nd try
    String task = "RowSumTransformer";
    registerTask(task);

    Thread.sleep(20000L);
    verifySinglePythonJob(outputFileName, 10); // verify 2nd try result
  }

  @Ignore // this test takes too much time(> 1 minute) because the smallest time unit in cron is
  // minute. This test has been tested locally before committed
  @Test
  public void commitScheduledYamlTestByCronAndCancel() {
    LOGGER.info("commitScheduledYamlTest(every 1 minute by cron) and cancel");
    String outputFileName =
        OUTPUT_DIR_PREFIX + File.separator + "export_file_every_1_minute_cron.txt";
    try {
      String insertSQL = "insert into scheduleData(key, %s) values(1, 2);";
      String yamlFileName = OUTPUT_DIR_PREFIX + File.separator + "TransformScheduledCron.yaml";
      long jobId = session.commitTransformJob(String.format(COMMIT_SQL_FORMATTER, yamlFileName));
      try {
        // add col0
        session.executeSql(String.format(insertSQL, "col0"));
        Thread.sleep(62000L); // sleep 62s to make sure next try is triggered.
        LOGGER.info("Verifying 0th try...");
        fileResultContains(outputFileName, "col0");
      } finally {
        cancelJob(jobId);
        assertTrue(Files.deleteIfExists(Paths.get(outputFileName)));
      }
    } catch (SessionException | InterruptedException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    } catch (IOException e) {
      LOGGER.error("Fail to delete result file: {}", outputFileName, e);
      fail();
    }
  }

  @Ignore // the time system on github action is somehow bugged, thus it cannot be tested in action
  // It has passed local test
  @Test
  public void commitScheduledYamlTestAt10sFromNow() {
    LOGGER.info("commitScheduledYamlTest(at 10s from now)");
    String outputFileName = OUTPUT_DIR_PREFIX + File.separator + "export_file_at_10_s.txt";
    try {
      String insertSQL = "insert into scheduleData(key, %s) values(1, 2);";
      String yamlFileName =
          OUTPUT_DIR_PREFIX + File.separator + "TransformScheduledAt10sFromNow.yaml";

      LocalDateTime tenSecondsLater = LocalDateTime.now().plusSeconds(10);
      String formattedDateTime =
          tenSecondsLater.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
      appendFile(new File(yamlFileName), "\nschedule: \"at '" + formattedDateTime + "'\"");

      long jobId = session.commitTransformJob(String.format(COMMIT_SQL_FORMATTER, yamlFileName));
      Thread.sleep(2000L); // add new data after 2s, before job is triggered.
      // add col0
      session.executeSql(String.format(insertSQL, "col0"));

      Thread.sleep(8000L); // verify result after 10s
      fileResultContains(outputFileName, "col0");

      verifyJobFinishedBlocked(jobId);
    } catch (SessionException | InterruptedException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    } catch (IOException e) {
      LOGGER.error("Fail to add schedule line in yaml", e);
      fail();
    }
  }

  @Test
  public void commitMultipleSqlStatementsTest() {
    LOGGER.info("commitMultipleSqlStatementsTest");
    List<TaskInfo> taskInfoList = new ArrayList<>();

    TaskInfo iginxTask = new TaskInfo(TaskType.IGINX, DataFlowType.STREAM);
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
      long jobId = session.commitTransformJob(taskInfoList, ExportType.FILE, outputFileName);

      verifyJobFinishedBlocked(jobId);
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
      long jobId = session.commitTransformJob(String.format(COMMIT_SQL_FORMATTER, yamlFileName));

      verifyJobFinishedBlocked(jobId);
      verifyMultipleSqlStatements(outputFileName);
    } catch (SessionException | InterruptedException | IOException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  private void verifyMultipleSqlStatements(String outputFileName) throws IOException {
    try {
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
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      final Path path = Paths.get(outputFileName);
      String content = new String(Files.readAllBytes(path));
      LOGGER.error("verifyMultipleSqlStatements failed, file content: \n{}\n", content, e);
      throw e;
    }
  }

  @Test
  public void commitSinglePythonJobTest() {
    LOGGER.info("commitSinglePythonJobTest");
    try {
      String task = "RowSumTransformer";
      registerTask(task);

      List<TaskInfo> taskInfoList = new ArrayList<>();

      TaskInfo iginxTask = new TaskInfo(TaskType.IGINX, DataFlowType.STREAM);
      iginxTask.setSqlList(Collections.singletonList(QUERY_SQL_2));

      TaskInfo pyTask = new TaskInfo(TaskType.PYTHON, DataFlowType.STREAM);
      pyTask.setPyTaskName("RowSumTransformer");

      taskInfoList.add(iginxTask);
      taskInfoList.add(pyTask);

      String outputFileName =
          OUTPUT_DIR_PREFIX + File.separator + "export_file_single_python_job.txt";
      long jobId = session.commitTransformJob(taskInfoList, ExportType.FILE, outputFileName);

      verifyJobFinishedBlocked(jobId);
      verifySinglePythonJob(outputFileName, 200);
    } catch (SessionException | InterruptedException | IOException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  @Test
  public void commitAndUpdateUDFTest() {
    LOGGER.info("commitAndUpdateUDFTest");
    try {
      dropAllTask();
      // at first, increase() will add 1
      String task = "increase",
          className = "AddOneTransformer",
          filename = OUTPUT_DIR_PREFIX + File.separator + "transformer_add_one.py";
      registerTask(task, className, filename);

      List<TaskInfo> taskInfoList = new ArrayList<>();

      TaskInfo iginxTask = new TaskInfo(TaskType.IGINX, DataFlowType.STREAM);
      iginxTask.setSqlList(Collections.singletonList(QUERY_SQL_3));

      TaskInfo pyTask = new TaskInfo(TaskType.PYTHON, DataFlowType.STREAM);
      pyTask.setPyTaskName("increase");

      taskInfoList.add(iginxTask);
      taskInfoList.add(pyTask);

      String schedule = "every 20 second";

      long jobId = session.commitTransformJob(taskInfoList, ExportType.IGINX, "", schedule);
      // make the script add 2 now
      alterPythonScriptWithReplace(filename, "+ 1", "+ 2");
      try {
        Thread.sleep(13000); // sleep 13s for 1st execution to complete.
        SessionExecuteSqlResult queryResult1 = session.executeSql("SELECT * FROM transform;");

        // then, increase() will add 2
        registerTask(task, className, filename);

        Thread.sleep(20000); // sleep 20s for 2nd execution to complete.
        SessionExecuteSqlResult queryResult2 = session.executeSql("SELECT * FROM transform;");
        // 2nd result will be appended from 11th line

        verifyIncreaseResult(queryResult1, 1, 0, 10);
        verifyIncreaseResult(queryResult2, 2, 10, 10);
      } finally {
        session.cancelTransformJob(jobId);
        alterPythonScriptWithReplace(filename, "+ 2", "+ 1");
        dropTask("increase");
      }
    } catch (SessionException | InterruptedException | IOException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  private void verifyIncreaseResult(
      SessionExecuteSqlResult queryResult, int increment, int offset, int lineCount) {
    queryResult.print(false, "");
    int timeIndex = queryResult.getPaths().indexOf("transform.key");
    int s1Index = queryResult.getPaths().indexOf("transform.us.d1.s1");
    int s2Index = queryResult.getPaths().indexOf("transform.us.d1.s2");
    if (needCompareResult) {
      assertNotEquals(-1, timeIndex);
      assertNotEquals(-1, s1Index);
      assertNotEquals(-1, s2Index);
    }
    long index = 0;
    for (int i = offset; i < queryResult.getValues().size(); i++) {
      List<Object> row = queryResult.getValues().get(i);
      assertEquals(index + increment, row.get(timeIndex));
      assertEquals(index + increment, row.get(s1Index));
      assertEquals(index + increment + 1, row.get(s2Index));
      index++;
    }
    assertEquals(lineCount, index);
  }

  // have to modify file content because UDF script name is usually not allowed to change
  private void alterPythonScriptWithReplace(String filePath, String oldStr, String newStr)
      throws IOException {
    final Path path = Paths.get(filePath);
    String content = new String(Files.readAllBytes(path));
    content = content.replace(oldStr, newStr);
    Files.write(path, content.getBytes());
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
      long jobId = session.commitTransformJob(String.format(COMMIT_SQL_FORMATTER, yamlFileName));

      verifyJobFinishedBlocked(jobId);
      verifySinglePythonJob(outputFileName, 200);
    } catch (SessionException | InterruptedException | IOException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  private void verifySinglePythonJob(String outputFileName, int expectedLineCount)
      throws IOException {
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

    assertEquals(expectedLineCount, index);
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

      TaskInfo iginxTask = new TaskInfo(TaskType.IGINX, DataFlowType.STREAM);
      iginxTask.setSqlList(Collections.singletonList(QUERY_SQL_2));

      TaskInfo addOnePyTask = new TaskInfo(TaskType.PYTHON, DataFlowType.STREAM);
      addOnePyTask.setPyTaskName("AddOneTransformer");

      TaskInfo rowSumPyTask = new TaskInfo(TaskType.PYTHON, DataFlowType.STREAM);
      rowSumPyTask.setPyTaskName("RowSumTransformer");

      taskInfoList.add(iginxTask);
      taskInfoList.add(addOnePyTask);
      taskInfoList.add(rowSumPyTask);

      String outputFileName =
          OUTPUT_DIR_PREFIX + File.separator + "export_file_multiple_python_jobs.txt";
      long jobId = session.commitTransformJob(taskInfoList, ExportType.FILE, outputFileName);

      verifyJobFinishedBlocked(jobId);
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
      long jobId = session.commitTransformJob(String.format(COMMIT_SQL_FORMATTER, yamlFileName));

      verifyJobFinishedBlocked(jobId);
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
      long jobId = session.commitTransformJob(String.format(COMMIT_SQL_FORMATTER, yamlFileName));

      verifyJobFinishedBlocked(jobId);

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

  /**
   * add picture dummy dir as engine; convert to bytes in transform function and export to iginx;
   * compare result and original pic data
   */
  @Test
  public void commitPythonExportBinaryToIginxTest() {
    LOGGER.info("commitPythonExportBinaryToIginxTest");
    String picDirSuffix = "pics";
    String picDir = DOWNLOAD_DIR + File.separator + picDirSuffix;
    try {
      String[] taskList = {"ToBytesTransformer"};
      for (String task : taskList) {
        registerTask(task);
      }
      downloadFile(
          "https://raw.githubusercontent.com/IGinX-THU/IGinX-resources/main/iginx-python-example/image/small.png",
          picDir + File.separator + "small.png");
      Map<String, String> params = new HashMap<>();
      params.put("iginx_port", "6888");
      params.put("dummy_dir", "test/src/test/resources/downloads/pics");
      params.put("is_read_only", "true");
      params.put("has_data", "true");
      session.addStorageEngine("127.0.0.1", 6660, StorageEngineType.filesystem, params);

      String yamlFileName =
          OUTPUT_DIR_PREFIX + File.separator + "TransformBinaryExportToIginx.yaml";
      long jobId = session.commitTransformJob(String.format(COMMIT_SQL_FORMATTER, yamlFileName));

      verifyJobFinishedBlocked(jobId);

      SessionExecuteSqlResult queryResult = session.executeSql("SELECT * FROM transform;");
      SessionExecuteSqlResult oriResult = session.executeSql("select * from pics;");
      assertEquals(queryResult.getPaths().size() - 1, oriResult.getPaths().size());
      List<Object> oriRow, row;
      for (int i = 0; i < queryResult.getValues().size(); i++) {
        row = queryResult.getValues().get(i);
        // remove transform.key
        row.remove(0);
        oriRow = oriResult.getValues().get(i);
        CollectionUtil.isEqualList(row, oriRow);
        i++;
      }
    } catch (SessionException | InterruptedException | IOException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    } finally {
      try {
        deleteFileOrDir(Paths.get(picDir).toFile());
      } catch (IOException e) {
        LOGGER.error("Remove test resource dir failed:", e);
      }
      try {
        session.removeStorageEngine(
            Collections.singletonList(new RemovedStorageEngineInfo("127.0.0.1", 6660, "", "")),
            true);
      } catch (SessionException e) {
        LOGGER.error("Remove read-only dummy engine failed:", e);
      }
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

      TaskInfo iginxTask = new TaskInfo(TaskType.IGINX, DataFlowType.STREAM);
      iginxTask.setSqlList(Collections.singletonList(QUERY_SQL_2));

      TaskInfo addOnePyTask = new TaskInfo(TaskType.PYTHON, DataFlowType.STREAM);
      addOnePyTask.setPyTaskName("AddOneTransformer");

      TaskInfo sumPyTask = new TaskInfo(TaskType.PYTHON, DataFlowType.BATCH);
      sumPyTask.setPyTaskName("SumTransformer");

      TaskInfo rowSumPyTask = new TaskInfo(TaskType.PYTHON, DataFlowType.STREAM);
      rowSumPyTask.setPyTaskName("RowSumTransformer");

      taskInfoList.add(iginxTask);
      taskInfoList.add(addOnePyTask);
      taskInfoList.add(sumPyTask);
      taskInfoList.add(rowSumPyTask);

      String outputFileName =
          OUTPUT_DIR_PREFIX + File.separator + "export_file_mixed_python_jobs.txt";
      long jobId = session.commitTransformJob(taskInfoList, ExportType.FILE, outputFileName);

      verifyJobFinishedBlocked(jobId);
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
      long jobId = session.commitTransformJob(String.format(COMMIT_SQL_FORMATTER, yamlFileName));

      verifyJobFinishedBlocked(jobId);
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

      long jobId = session.commitTransformJob(String.format(COMMIT_SQL_FORMATTER, yamlFileName));

      verifyJobFinishedBlocked(jobId);
      verifyMixedPythonJobs(outputFileName);
    } catch (SessionException | InterruptedException | IOException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  public void redundantCancellationTest(long jobId) {
    // On cancelling finished/closed/failed jobs, user should be notified of jobs' states.
    LOGGER.info("redundantCancellationTest");
    try {
      JobState state = session.queryTransformJobStatus(jobId);
      LOGGER.info("job({}) is in state:{}. Trying to cancel it...", jobId, state);
      String info = "null";
      switch (state) {
        case JOB_FINISHED:
          info = "has finished";
          break;
        case JOB_FAILED:
          info = "has failed";
          break;
        case JOB_CLOSED:
          info = "has closed";
          break;
        default:
          LOGGER.error("expecting finished/closed/failed state, got:{}", state);
          fail();
      }
      try {
        session.cancelTransformJob(jobId);
        fail(); // should throw exception.
      } catch (SessionException e) {
        String msg = e.getMessage();
        if (msg != null && msg.contains(info)) {
          LOGGER.info("successfully passed cancellation error message");
        } else {
          fail();
        }
      }
    } catch (SessionException e) {
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

      TaskInfo iginxTask = new TaskInfo(TaskType.IGINX, DataFlowType.STREAM);
      iginxTask.setSqlList(Collections.singletonList(QUERY_SQL_2));

      TaskInfo sleepPyTask = new TaskInfo(TaskType.PYTHON, DataFlowType.STREAM);
      sleepPyTask.setPyTaskName("SleepTransformer");

      taskInfoList.add(iginxTask);
      taskInfoList.add(sleepPyTask);

      long jobId = session.commitTransformJob(taskInfoList, ExportType.LOG, "");
      LOGGER.info("job is {}", jobId);
      JobState jobState = session.queryTransformJobStatus(jobId);
      LOGGER.info("job {} state is {}", jobId, jobState.toString());

      session.cancelTransformJob(jobId);
      jobState = session.queryTransformJobStatus(jobId);
      LOGGER.info("After cancellation, job {} state is {}", jobId, jobState.toString());
      assertEquals(JobState.JOB_CLOSED, jobState);

      List<Long> closedJobIds =
          session.showEligibleJob(JobState.JOB_CLOSED).get(JobState.JOB_CLOSED);
      assertTrue(closedJobIds.contains(jobId));
    } catch (SessionException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }

  // file <filename> contains <content>
  private void fileResultContains(String filename, String content) {
    boolean contains = false;
    try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
      String line = reader.readLine();
      while (line != null) {
        if (line.contains(content)) {
          contains = true;
          break;
        }
        line = reader.readLine();
      }
    } catch (IOException e) {
      LOGGER.error("Verify file export result failed.", e);
      fail();
    }
    assertTrue(contains);
  }

  @Rule public final GreenMailRule greenMail = new GreenMailRule(ServerSetupTest.SMTPS);

  @Test
  public void commitSingleSqlStatementByYamlWithEmailTest() throws MessagingException {
    LOGGER.info("commitSingleSqlStatementByYamlWithSingleEmailNotificationTest");
    try {
      greenMail.setUser("from@localhost", "password");
      String yamlFileName =
          OUTPUT_DIR_PREFIX + File.separator + "TransformSingleSqlStatementWithEmail.yaml";
      long jobId = session.commitTransformJob(String.format(COMMIT_SQL_FORMATTER, yamlFileName));

      verifyJobFinishedBlocked(jobId);

      Assert.assertTrue(greenMail.waitForIncomingEmail(10 * 1000, 2));

      assertEquals(2, greenMail.getReceivedMessages().length);
      assertEquals("Job " + jobId + " is created", greenMail.getReceivedMessages()[0].getSubject());
      assertEquals(
          "Job " + jobId + " is finished", greenMail.getReceivedMessages()[1].getSubject());
    } catch (SessionException | InterruptedException e) {
      LOGGER.error("Transform:  execute fail. Caused by:", e);
      fail();
    }
  }
}
