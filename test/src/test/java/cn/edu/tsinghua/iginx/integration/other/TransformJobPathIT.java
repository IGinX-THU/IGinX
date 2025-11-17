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
package cn.edu.tsinghua.iginx.integration.other;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.func.session.InsertAPIType;
import cn.edu.tsinghua.iginx.integration.tool.ClientLauncher;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformJobPathIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransformJobPathIT.class);

  private static Session session;

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  // host info
  private static String defaultTestHost = "127.0.0.1";
  private static int defaultTestPort = 6888;
  private static String defaultTestUser = "root";
  private static String defaultTestPass = "root";

  private static final String CREATE_SQL_FORMATTER =
      "CREATE FUNCTION TRANSFORM \"%s\" FROM \"%s\" IN \"%s\";";

  private static final Map<String, String> TASK_MAP = new HashMap<>();

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

  static {
    TASK_MAP.put(
        "RowSumTransformer", OUTPUT_DIR_PREFIX + File.separator + "transformer_row_sum.py");
    TASK_MAP.put(
        "AddOneTransformer", OUTPUT_DIR_PREFIX + File.separator + "transformer_add_one.py");
  }

  @BeforeClass
  public static void setUp() throws SessionException {
    session = new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass);
    session.openSession();
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    session.closeSession();
  }

  @Test
  public void testTransformJobPath() throws SessionException, IOException, InterruptedException {
    prepare();
    registerJobByClientUsingRelativePath();
    // waiting job to be completed
    Thread.sleep(3000);
    verifyResult();
  }

  public void insertData() {
    List<String> pathList =
        new ArrayList<String>() {
          {
            add("us.d1.s1");
            add("us.d1.s2");
          }
        };
    List<DataType> dataTypeList =
        new ArrayList<DataType>() {
          {
            add(DataType.LONG);
            add(DataType.LONG);
          }
        };
    List<Long> keyList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      keyList.add((long) i);
      valuesList.add(Arrays.asList((long) i, (long) i + 1));
    }
    Controller.writeRowsData(
        session,
        pathList,
        keyList,
        dataTypeList,
        valuesList,
        new ArrayList<>(),
        InsertAPIType.Row,
        false);
    Controller.after(session);
  }

  private void prepare() throws SessionException {
    insertData();
    SessionExecuteSqlResult result = session.executeSql("select count(*) from *;");
    result.print(false, "");
    for (String task : TASK_MAP.keySet()) {
      session.executeSql(String.format(CREATE_SQL_FORMATTER, task, task, TASK_MAP.get(task)));
    }
  }

  private void verifyMultiplePythonJobs(
      SessionExecuteSqlResult queryResult, int timeIndex, int sumIndex) {
    long index = 0;
    for (List<Object> row : queryResult.getValues()) {
      assertEquals(index + 1, row.get(timeIndex));
      assertEquals(index + 1 + index + 1 + 1, row.get(sumIndex));
      index++;
    }
    assertEquals(200, index);
  }

  private void registerJobByClientUsingRelativePath() throws IOException {
    ClientLauncher client = new ClientLauncher();
    Path source =
        Paths.get(
            "src",
            "test",
            "resources",
            "transform",
            "TransformMultiplePythonJobsWithExportToIginx.yaml");
    Path target =
        Paths.get(client.getClientRootDir(), "TransformMultiplePythonJobsWithExportToIginx.yaml");
    Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);

    try {
      String statement =
          "commit transform job \"TransformMultiplePythonJobsWithExportToIginx.yaml\";";
      client.readLine(statement);
      assertTrue(client.expectedOutputContains("job id:"));
    } finally {
      client.close();
    }
  }

  private void verifyResult() throws SessionException {
    SessionExecuteSqlResult queryResult = session.executeSql("SELECT * FROM transform;");
    queryResult.print(false, "");
    int timeIndex = queryResult.getPaths().indexOf("transform.key");
    int sumIndex = queryResult.getPaths().indexOf("transform.sum");
    assertNotEquals(-1, timeIndex);
    assertNotEquals(-1, sumIndex);

    verifyMultiplePythonJobs(queryResult, timeIndex, sumIndex);

    // clean up
    session.executeSql("clear data;");
    for (String task : TASK_MAP.keySet()) {
      session.executeSql(String.format("DROP FUNCTION \"%s\";", task));
    }
  }
}
