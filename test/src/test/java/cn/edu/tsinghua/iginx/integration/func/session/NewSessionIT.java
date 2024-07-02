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
package cn.edu.tsinghua.iginx.integration.func.session;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.WINDOW_END_COL;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.WINDOW_START_COL;
import static cn.edu.tsinghua.iginx.integration.controller.Controller.SUPPORT_KEY;
import static cn.edu.tsinghua.iginx.integration.controller.Controller.clearAllData;
import static cn.edu.tsinghua.iginx.integration.func.session.InsertAPIType.*;
import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.influxdb;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.pool.IginxInfo;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionAggregateQueryDataSet;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.thrift.TagFilterType;
import cn.edu.tsinghua.iginx.utils.ShellRunner;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewSessionIT {

  protected static final Logger LOGGER = LoggerFactory.getLogger(NewSessionIT.class);

  protected static MultiConnection conn;
  protected static boolean isForSession = true;
  protected static boolean isForSessionPool = false;

  // host info
  protected static String defaultTestHost = "127.0.0.1";
  protected static int defaultTestPort = 6888;
  protected static String defaultTestUser = "root";
  protected static String defaultTestPass = "root";

  private static final long START_KEY = 0L;

  private static final long END_KEY = 16000L;

  private static final double DELTA = 0.0001D;

  private static final TestDataSection baseDataSection = buildBaseDataSection();

  private static boolean isInfluxdb = false;

  private static boolean isAbleToDelete = true;

  private static boolean dummyNoData = true;
  private static boolean isScaling = false;

  private static boolean needCompareResult = true;

  public NewSessionIT() {}

  private static TestDataSection buildBaseDataSection() {
    List<String> paths =
        Arrays.asList(
            "us.d1.s1", "us.d1.s2", "us.d1.s3", "us.d1.s4", "us.d1.s5", "us.d1.s6", "us.d1.s7");
    List<DataType> types =
        Arrays.asList(
            DataType.BOOLEAN,
            DataType.INTEGER,
            DataType.LONG,
            DataType.FLOAT,
            DataType.DOUBLE,
            DataType.BINARY,
            DataType.BINARY);
    List<Map<String, String>> tagsList =
        Arrays.asList(
            new HashMap<>(),
            new HashMap<>(),
            new HashMap<>(),
            new HashMap<>(),
            new HashMap<>(),
            new HashMap<>(),
            new HashMap<String, String>() {
              {
                put("k1", "v1");
                put("k2", "v2");
              }
            });
    List<Long> keys = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();

    long size = END_KEY - START_KEY;
    for (int i = 0; i < size; i++) {
      keys.add(START_KEY + i);
      values.add(
          Arrays.asList(
              i % 2 == 0,
              i,
              (long) i,
              i + 0.1f,
              i + 0.2d,
              String.valueOf(i).getBytes(),
              new String(RandomStringUtils.randomAlphanumeric(10).getBytes()).getBytes()));
    }
    return new TestDataSection(keys, types, paths, values, tagsList);
  }

  @BeforeClass
  public static void setUp() throws SessionException {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    if (StorageEngineType.valueOf(conf.getStorageType(false).toLowerCase()) == influxdb) {
      isInfluxdb = true;
    }
    if (!SUPPORT_KEY.get(conf.getStorageType()) && conf.isScaling()) {
      needCompareResult = false;
    }
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    isScaling = conf.isScaling();
    isAbleToDelete = dbConf.getEnumValue(DBConf.DBConfType.isAbleToDelete);
    if (isForSession) {
      conn =
          new MultiConnection(
              new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass));
    } else if (isForSessionPool) {
      conn =
          new MultiConnection(
              new SessionPool(
                  new ArrayList<IginxInfo>() {
                    {
                      add(
                          new IginxInfo.Builder()
                              .host("0.0.0.0")
                              .port(6888)
                              .user("root")
                              .password("root")
                              .build());

                      add(
                          new IginxInfo.Builder()
                              .host("0.0.0.0")
                              .port(7888)
                              .user("root")
                              .password("root")
                              .build());
                    }
                  }));
    }
    conn.openSession();
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    clearAllData(conn);
    conn.closeSession();
  }

  @Before
  public void insertBaseData() {
    // insert base data using all types of insert API.
    List<InsertAPIType> insertAPITypes =
        Arrays.asList(Row, NonAlignedRow, Column, InsertAPIType.NonAlignedColumn);
    long size = (END_KEY - START_KEY) / insertAPITypes.size();
    for (int i = 0; i < insertAPITypes.size(); i++) {
      long start = i * size, end = start + size;
      TestDataSection subBaseData = baseDataSection.getSubDataSectionWithKey(start, end);
      insertData(subBaseData, insertAPITypes.get(i));
    }
    dummyNoData = false;
  }

  @After
  public void clearData() throws SessionException {
    Controller.clearData(conn);
  }

  private void insertData(TestDataSection data, InsertAPIType type) {
    switch (type) {
      case Row:
      case NonAlignedRow:
        Controller.writeRowsData(
            conn,
            data.getPaths(),
            data.getKeys(),
            data.getTypes(),
            data.getValues(),
            data.getTagsList(),
            type,
            dummyNoData);
        break;
      case Column:
      case NonAlignedColumn:
        List<List<Object>> values =
            IntStream.range(0, data.getPaths().size())
                .mapToObj(
                    col ->
                        IntStream.range(0, data.getValues().size())
                            .mapToObj(row -> data.getValues().get(row).get(col))
                            .collect(Collectors.toList()))
                .collect(Collectors.toList());
        Controller.writeColumnsData(
            conn,
            data.getPaths(),
            IntStream.range(0, data.getPaths().size())
                .mapToObj(i -> new ArrayList<>(data.getKeys()))
                .collect(Collectors.toList()),
            data.getTypes(),
            values,
            data.getTagsList(),
            type,
            dummyNoData);
    }
  }

  private Object[][] transpose(Object[][] array) {
    int maxLength = 0;
    for (Object[] objects : array) {
      maxLength = Math.max(maxLength, objects.length);
    }
    Object[][] transposed = new Object[maxLength][array.length];
    for (int i = 0; i < array.length; i++) {
      for (int j = 0; j < array[i].length; j++) {
        transposed[j][i] = array[i][j];
      }
    }
    return transposed;
  }

  private void compare(TestDataSection expected, SessionQueryDataSet actual) {
    if (!needCompareResult) {
      return;
    }
    compareKeys(expected.getKeys(), actual.getKeys());
    comparePaths(expected.getPaths(), actual.getPaths(), expected.getTagsList());
    compareValues(expected.getValues(), actual.getValues());
  }

  private void compare(TestDataSection expected, SessionAggregateQueryDataSet actual) {
    if (!needCompareResult) {
      return;
    }
    assertNull(actual.getKeys());
    comparePaths(expected.getPaths(), actual.getPaths(), expected.getTagsList());
    compareValues(expected.getValues(), actual.getValues());
  }

  private void compareKeys(List<Long> expectedKeys, long[] actualKeys) {
    assertEquals(expectedKeys.size(), actualKeys.length);
    for (int i = 0; i < expectedKeys.size(); i++) {
      if (expectedKeys.get(i) == null) {
        fail();
      }
      long expectedKey = expectedKeys.get(i); // unboxing
      assertEquals(expectedKey, actualKeys[i]);
    }
  }

  private void comparePaths(
      List<String> expectedPaths, List<String> actualPaths, List<Map<String, String>> tagList) {
    assertEquals(expectedPaths.size(), actualPaths.size());
    for (int i = 0; i < expectedPaths.size(); i++) {
      assertEquals(getPathWithTag(expectedPaths.get(i), tagList.get(i)), actualPaths.get(i));
    }
  }

  private String getPathWithTag(String path, Map<String, String> tags) {
    StringBuilder builder = new StringBuilder();
    if (tags.isEmpty()) {
      return path;
    }
    builder.append("{");
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      builder.append(key).append("=").append(value).append(",");
    }
    if (builder.charAt(builder.length() - 1) == ',') {
      builder.deleteCharAt(builder.length() - 1);
    }
    builder.append("}");
    String formattedTags = builder.toString();
    return path + formattedTags;
  }

  private void compareValues(List<List<Object>> expectedValues, List<List<Object>> actualValues) {
    assertEquals(expectedValues.size(), actualValues.size());
    for (int i = 0; i < expectedValues.size(); i++) {
      List<Object> expectedRowValues = expectedValues.get(i);
      List<Object> actualRowValues = actualValues.get(i);
      assertEquals(expectedRowValues.size(), actualRowValues.size());
      for (int j = 0; j < expectedRowValues.size(); j++) {
        compareObjectValue(expectedRowValues.get(j), actualRowValues.get(j));
      }
    }
  }

  private void compareValues(List<List<Object>> expectedValues, Object[] actualValues) {
    assertEquals(1, expectedValues.size());
    List<Object> rowValues = expectedValues.get(0);
    assertEquals(rowValues.size(), actualValues.length);
    for (int i = 0; i < rowValues.size(); i++) {
      compareObjectValue(rowValues.get(i), actualValues[i]);
    }
  }

  private void compareObjectValue(Object expected, Object actual) {
    if (expected.getClass() != actual.getClass() && !isInfluxdb) {
      LOGGER.error(
          "Inconsistent data types, expected:{}, actual:{}",
          expected.getClass(),
          actual.getClass());
      fail();
    }
    if (expected instanceof Boolean) {
      boolean expectedVal = (boolean) expected;
      boolean actualVal = (boolean) actual;
      assertEquals(expectedVal, actualVal);
    } else if (expected instanceof Integer) {
      if (isInfluxdb) {
        long expectedVal = ((Integer) expected).longValue();
        long actualVal = (long) actual;
        assertEquals(expectedVal, actualVal);
        return;
      }
      int expectedVal = (int) expected;
      int actualVal = (int) actual;
      assertEquals(expectedVal, actualVal);
    } else if (expected instanceof Long) {
      long expectedVal = (long) expected;
      long actualVal = (long) actual;
      assertEquals(expectedVal, actualVal);
    } else if (expected instanceof Float) {
      if (isInfluxdb) {
        double expectedVal = ((Float) expected).doubleValue();
        double actualVal = (double) actual;
        assertEquals(expectedVal, actualVal, expectedVal * DELTA);
        return;
      }
      float expectedVal = (float) expected;
      float actualVal = (float) actual;
      assertEquals(expectedVal, actualVal, expectedVal * DELTA);
    } else if (expected instanceof Double) {
      double expectedVal = (double) expected;
      double actualVal = (double) actual;
      assertEquals(expectedVal, actualVal, expectedVal * DELTA);
    } else if (expected instanceof byte[]) {
      String expectedVal = new String((byte[]) expected);
      String actualVal = new String((byte[]) actual);
      assertEquals(expectedVal, actualVal);
    } else {
      String expectedVal = (String) expected;
      String actualVal = (String) actual;
      assertEquals(expectedVal, actualVal);
    }
  }

  @Test
  public void testCancelSession() {
    try {
      List<Long> sessionIDs = conn.getSessionIDs();

      List<Long> existsSessionIDs = conn.executeSql("show sessionid;").getSessionIDs();

      if (!new HashSet<>(existsSessionIDs).equals(new HashSet<>(sessionIDs))) {
        LOGGER.error("server session_id_list does not equal to active_session_id_list.");
        fail();
      }

      conn.closeSession();
      conn.openSession();

      existsSessionIDs = conn.executeSql("show sessionid;").getSessionIDs();
      for (long sessionID : sessionIDs) {
        if (existsSessionIDs.contains(sessionID)) {
          LOGGER.error("the ID for a closed session is still in the server session_id_list.");
          fail();
        }
      }
    } catch (SessionException e) {
      LOGGER.error("execute query session id failed.");
      fail();
    }
  }

  @Test
  public void testCancelClient() {
    File clientDir = new File("../client/target/");
    File[] matchingFiles = clientDir.listFiles((dir, name) -> name.startsWith("iginx-client-"));
    matchingFiles = Arrays.stream(matchingFiles).filter(File::isDirectory).toArray(File[]::new);
    String version = matchingFiles[0].getName();
    version = version.contains(".jar") ? version.substring(0, version.lastIndexOf(".")) : version;
    // use .sh on unix & .bat on windows(absolute path)
    String clientUnixPath = "../client/target/" + version + "/sbin/start_cli.sh";
    String clientWinPath = null;
    try {
      clientWinPath =
          new File("../client/target/" + version + "/sbin/start_cli.bat").getCanonicalPath();
    } catch (IOException e) {
      LOGGER.info("Can't find script ../client/target/iginx-client-*/sbin/start_cli.bat");
      fail();
    }
    try {
      List<Long> sessionIDs1 = conn.executeSql("show sessionid;").getSessionIDs();
      LOGGER.info("before start a client, session_id_list size: {}", sessionIDs1.size());

      // start a client
      ProcessBuilder pb = new ProcessBuilder();
      if (ShellRunner.isOnWin()) {
        pb.command(clientWinPath);
      } else {
        Process before = Runtime.getRuntime().exec(new String[] {"chmod", "+x", clientUnixPath});
        before.waitFor();
        LOGGER.info("before start a client, exit value: {}", before.exitValue());
        pb.command("bash", "-c", clientUnixPath);
      }
      Process p = pb.start();

      Thread.sleep(3000);
      LOGGER.info("client is alive: {}", p.isAlive());
      if (!p.isAlive()) { // fail to start a client.
        LOGGER.info("exit value: {}", p.exitValue());
        fail();
      }

      List<Long> sessionIDs2 = conn.executeSql("show sessionid;").getSessionIDs();
      LOGGER.info("after start a client, session_id_list size: {}", sessionIDs2.size());

      // kill the client
      try (OutputStream os = p.getOutputStream();
          PrintWriter writer = new PrintWriter(os, true)) {
        // send exit command to client to close session.
        // destroy() won't work on windows.
        writer.println("exit;");
        writer.flush();
      }
      p.destroy();
      Thread.sleep(3000);

      List<Long> sessionIDs3 = conn.executeSql("show sessionid;").getSessionIDs();
      LOGGER.info("after cancel a client, session_id_list size:{}", sessionIDs3.size());

      assertEquals(sessionIDs1, sessionIDs3);
      assertTrue(sessionIDs2.size() - sessionIDs1.size() > 0);
    } catch (SessionException | IOException | InterruptedException e) {
      LOGGER.error("unexpected error: ", e);
      fail();
    }
  }

  @Test
  public void testQuery() {
    List<String> paths =
        Arrays.asList(
            "us.d1.s1", "us.d1.s2", "us.d1.s3", "us.d1.s4", "us.d1.s5", "us.d1.s6", "us.d1.s7");

    // query first corner of the base data
    try {
      long start = START_KEY, end = START_KEY + 100;
      SessionQueryDataSet dataSet = conn.queryData(paths, start, end);
      compare(baseDataSection.getSubDataSectionWithKey(start, end), dataSet);
    } catch (SessionException e) {
      LOGGER.error("execute query data failed.", e);
      fail();
    }

    // query middle of the base data
    try {
      long mid = (START_KEY + END_KEY) / 2;
      long start = mid - 50, end = mid + 50;
      SessionQueryDataSet dataSet = conn.queryData(paths, start, end);
      compare(baseDataSection.getSubDataSectionWithKey(start, end), dataSet);
    } catch (SessionException e) {
      LOGGER.error("execute query data failed.");
      fail();
    }

    // query last corner of the base data
    try {
      long start = END_KEY - 100, end = END_KEY;
      SessionQueryDataSet dataSet = conn.queryData(paths, start, end);
      compare(baseDataSection.getSubDataSectionWithKey(start, end), dataSet);
    } catch (SessionException e) {
      LOGGER.error("execute query data failed.");
      fail();
    }
  }

  @Test
  public void testDeletePaths() {
    if (!isAbleToDelete || isScaling) {
      return;
    }
    // delete single path
    List<String> deleteColumns = Collections.singletonList("us.d1.s2");
    try {
      conn.deleteColumns(deleteColumns);
      SessionQueryDataSet dataSet = conn.queryData(deleteColumns, START_KEY, END_KEY);
      compare(TestDataSection.EMPTY_TEST_DATA_SECTION, dataSet);
    } catch (SessionException e) {
      LOGGER.error("execute delete columns failed.");
      fail();
    }

    // delete multi paths
    deleteColumns = Arrays.asList("us.d1.s1", "us.d1.s3", "us.d1.s5");
    try {
      conn.deleteColumns(deleteColumns);
      SessionQueryDataSet dataSet = conn.queryData(deleteColumns, START_KEY, END_KEY);
      compare(TestDataSection.EMPTY_TEST_DATA_SECTION, dataSet);
    } catch (SessionException e) {
      LOGGER.error("execute delete columns failed.");
      fail();
    }

    // delete path with tag
    deleteColumns = Collections.singletonList("us.d1.s7");
    try {
      conn.deleteColumns(
          deleteColumns,
          Collections.singletonList(
              new HashMap<String, List<String>>() {
                {
                  put("k1", Collections.singletonList("v1"));
                  put("k2", Collections.singletonList("v2"));
                }
              }),
          TagFilterType.Precise);
      SessionQueryDataSet dataSet = conn.queryData(deleteColumns, START_KEY, END_KEY);
      compare(TestDataSection.EMPTY_TEST_DATA_SECTION, dataSet);
    } catch (SessionException e) {
      LOGGER.error("execute delete columns failed.");
      fail();
    }
  }

  @Test
  public void testAggregateQuery() {
    List<String> paths = Arrays.asList("us.d1.s2", "us.d1.s3", "us.d1.s4", "us.d1.s5");
    List<DataType> types =
        Arrays.asList(DataType.INTEGER, DataType.LONG, DataType.FLOAT, DataType.DOUBLE);
    List<Long> keys = null;

    List<AggregateType> aggregateTypes =
        Arrays.asList(
            AggregateType.MAX,
            AggregateType.MIN,
            AggregateType.SUM,
            AggregateType.COUNT,
            AggregateType.AVG,
            AggregateType.FIRST_VALUE,
            AggregateType.LAST_VALUE,
            AggregateType.FIRST,
            AggregateType.LAST);

    List<TestDataSection> expectedResults =
        Arrays.asList(
            new TestDataSection(
                keys,
                types,
                paths.stream().map(s -> "max(" + s + ")").collect(Collectors.toList()),
                Collections.singletonList(Arrays.asList(15999, 15999L, 15999.1f, 15999.2d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                paths.stream().map(s -> "min(" + s + ")").collect(Collectors.toList()),
                Collections.singletonList(Arrays.asList(0, 0L, 0.1f, 0.2d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                paths.stream().map(s -> "sum(" + s + ")").collect(Collectors.toList()),
                Collections.singletonList(
                    Arrays.asList(127992000L, 127992000L, 127993600.0d, 127993600.0d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                paths.stream().map(s -> "count(" + s + ")").collect(Collectors.toList()),
                Collections.singletonList(Arrays.asList(16000L, 16000L, 16000L, 16000L)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                paths.stream().map(s -> "avg(" + s + ")").collect(Collectors.toList()),
                Collections.singletonList(Arrays.asList(7999.5d, 7999.5d, 7999.6d, 7999.7d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                paths.stream().map(s -> "first_value(" + s + ")").collect(Collectors.toList()),
                Collections.singletonList(Arrays.asList(0, 0L, 0.1f, 0.2d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                paths.stream().map(s -> "last_value(" + s + ")").collect(Collectors.toList()),
                Collections.singletonList(Arrays.asList(15999, 15999L, 15999.1f, 15999.2d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                Arrays.asList("path", "value"),
                Collections.singletonList(Arrays.asList("us.d1.s2".getBytes(), "0".getBytes())),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                Arrays.asList("path", "value"),
                Collections.singletonList(Arrays.asList("us.d1.s2".getBytes(), "15999".getBytes())),
                baseDataSection.getTagsList()));

    for (int i = 0; i < aggregateTypes.size(); i++) {
      AggregateType type = aggregateTypes.get(i);
      try {
        SessionAggregateQueryDataSet dataSet = conn.aggregateQuery(paths, START_KEY, END_KEY, type);
        compare(expectedResults.get(i), dataSet);
      } catch (SessionException e) {
        LOGGER.error("execute aggregate query failed, AggType={}", type);
        fail();
      }
    }
  }

  @Test
  public void testDownsampleQuery() {
    List<String> paths = Arrays.asList("us.d1.s2", "us.d1.s3", "us.d1.s4", "us.d1.s5");
    List<String> resPaths =
        Arrays.asList(
            WINDOW_START_COL, WINDOW_END_COL, "us.d1.s2", "us.d1.s3", "us.d1.s4", "us.d1.s5");
    List<DataType> types =
        Arrays.asList(DataType.INTEGER, DataType.LONG, DataType.FLOAT, DataType.DOUBLE);
    List<Long> keys = Arrays.asList(0L, 4000L, 8000L, 12000L);

    List<AggregateType> aggregateTypes =
        Arrays.asList(
            AggregateType.MAX,
            AggregateType.MIN,
            AggregateType.SUM,
            AggregateType.COUNT,
            AggregateType.AVG,
            AggregateType.FIRST_VALUE,
            AggregateType.LAST_VALUE);
    long precision = 4000;

    List<TestDataSection> expectedResults =
        Arrays.asList(
            new TestDataSection(
                keys,
                types,
                resPaths.stream()
                    .map(
                        s ->
                            s.equals(WINDOW_START_COL) || s.equals(WINDOW_END_COL)
                                ? s
                                : "max(" + s + ")")
                    .collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(0L, 3999L, 3999, 3999L, 3999.1f, 3999.2d),
                    Arrays.asList(4000L, 7999L, 7999, 7999L, 7999.1f, 7999.2d),
                    Arrays.asList(8000L, 11999L, 11999, 11999L, 11999.1f, 11999.2d),
                    Arrays.asList(12000L, 15999L, 15999, 15999L, 15999.1f, 15999.2d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                resPaths.stream()
                    .map(
                        s ->
                            s.equals(WINDOW_START_COL) || s.equals(WINDOW_END_COL)
                                ? s
                                : "min(" + s + ")")
                    .collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(0L, 3999L, 0, 0L, 0.1f, 0.2d),
                    Arrays.asList(4000L, 7999L, 4000, 4000L, 4000.1f, 4000.2d),
                    Arrays.asList(8000L, 11999L, 8000, 8000L, 8000.1f, 8000.2d),
                    Arrays.asList(12000L, 15999L, 12000, 12000L, 12000.1f, 12000.2d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                resPaths.stream()
                    .map(
                        s ->
                            s.equals(WINDOW_START_COL) || s.equals(WINDOW_END_COL)
                                ? s
                                : "sum(" + s + ")")
                    .collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(0L, 3999L, 7998000L, 7998000L, 7998400.0d, 7998800.0d),
                    Arrays.asList(4000L, 7999L, 23998000L, 23998000L, 23998400.0d, 23998800.0d),
                    Arrays.asList(8000L, 11999L, 39998000L, 39998000L, 39998400.0d, 39998800.0d),
                    Arrays.asList(12000L, 15999L, 55998000L, 55998000L, 55998400.0d, 55998800.0d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                resPaths.stream()
                    .map(
                        s ->
                            s.equals(WINDOW_START_COL) || s.equals(WINDOW_END_COL)
                                ? s
                                : "count(" + s + ")")
                    .collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(0L, 3999L, 4000L, 4000L, 4000L, 4000L),
                    Arrays.asList(4000L, 7999L, 4000L, 4000L, 4000L, 4000L),
                    Arrays.asList(8000L, 11999L, 4000L, 4000L, 4000L, 4000L),
                    Arrays.asList(12000L, 15999L, 4000L, 4000L, 4000L, 4000L)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                resPaths.stream()
                    .map(
                        s ->
                            s.equals(WINDOW_START_COL) || s.equals(WINDOW_END_COL)
                                ? s
                                : "avg(" + s + ")")
                    .collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(0L, 3999L, 1999.5d, 1999.5d, 1999.6d, 1999.7d),
                    Arrays.asList(4000L, 7999L, 5999.5d, 5999.5d, 5999.6d, 5999.7d),
                    Arrays.asList(8000L, 11999L, 9999.5d, 9999.5d, 9999.6d, 9999.7d),
                    Arrays.asList(12000L, 15999L, 13999.5d, 13999.5d, 13999.6d, 13999.7d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                resPaths.stream()
                    .map(
                        s ->
                            s.equals(WINDOW_START_COL) || s.equals(WINDOW_END_COL)
                                ? s
                                : "first_value(" + s + ")")
                    .collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(0L, 3999L, 0, 0L, 0.1f, 0.2d),
                    Arrays.asList(4000L, 7999L, 4000, 4000L, 4000.1f, 4000.2d),
                    Arrays.asList(8000L, 11999L, 8000, 8000L, 8000.1f, 8000.2d),
                    Arrays.asList(12000L, 15999L, 12000, 12000L, 12000.1f, 12000.2d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                resPaths.stream()
                    .map(
                        s ->
                            s.equals(WINDOW_START_COL) || s.equals(WINDOW_END_COL)
                                ? s
                                : "last_value(" + s + ")")
                    .collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(0L, 3999L, 3999, 3999L, 3999.1f, 3999.2d),
                    Arrays.asList(4000L, 7999L, 7999, 7999L, 7999.1f, 7999.2d),
                    Arrays.asList(8000L, 11999L, 11999, 11999L, 11999.1f, 11999.2d),
                    Arrays.asList(12000L, 15999L, 15999, 15999L, 15999.1f, 15999.2d)),
                baseDataSection.getTagsList()));

    for (int i = 0; i < aggregateTypes.size(); i++) {
      AggregateType type = aggregateTypes.get(i);
      try {
        SessionQueryDataSet dataSet =
            conn.downsampleQuery(paths, START_KEY, END_KEY, type, precision);
        compare(expectedResults.get(i), dataSet);
      } catch (SessionException e) {
        LOGGER.error("execute downsample query failed, AggType={}, Precision={}", type, precision);
        fail();
      }
    }
  }

  @Test
  public void testDownsampleQueryNoInterval() {
    List<String> paths = Arrays.asList("us.d1.s2", "us.d1.s3", "us.d1.s4", "us.d1.s5");
    List<String> resPaths =
        Arrays.asList(
            WINDOW_START_COL, WINDOW_END_COL, "us.d1.s2", "us.d1.s3", "us.d1.s4", "us.d1.s5");
    List<DataType> types =
        Arrays.asList(DataType.INTEGER, DataType.LONG, DataType.FLOAT, DataType.DOUBLE);
    List<Long> keys = Arrays.asList(0L, 4000L, 8000L, 12000L);

    List<AggregateType> aggregateTypes =
        Arrays.asList(
            AggregateType.MAX,
            AggregateType.MIN,
            AggregateType.SUM,
            AggregateType.COUNT,
            AggregateType.AVG,
            AggregateType.FIRST_VALUE,
            AggregateType.LAST_VALUE);
    long precision = 4000;

    List<TestDataSection> expectedResults =
        Arrays.asList(
            new TestDataSection(
                keys,
                types,
                resPaths.stream()
                    .map(
                        s ->
                            s.equals(WINDOW_START_COL) || s.equals(WINDOW_END_COL)
                                ? s
                                : "max(" + s + ")")
                    .collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(0L, 3999L, 3999, 3999L, 3999.1f, 3999.2d),
                    Arrays.asList(4000L, 7999L, 7999, 7999L, 7999.1f, 7999.2d),
                    Arrays.asList(8000L, 11999L, 11999, 11999L, 11999.1f, 11999.2d),
                    Arrays.asList(12000L, 15999L, 15999, 15999L, 15999.1f, 15999.2d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                resPaths.stream()
                    .map(
                        s ->
                            s.equals(WINDOW_START_COL) || s.equals(WINDOW_END_COL)
                                ? s
                                : "min(" + s + ")")
                    .collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(0L, 3999L, 0, 0L, 0.1f, 0.2d),
                    Arrays.asList(4000L, 7999L, 4000, 4000L, 4000.1f, 4000.2d),
                    Arrays.asList(8000L, 11999L, 8000, 8000L, 8000.1f, 8000.2d),
                    Arrays.asList(12000L, 15999L, 12000, 12000L, 12000.1f, 12000.2d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                resPaths.stream()
                    .map(
                        s ->
                            s.equals(WINDOW_START_COL) || s.equals(WINDOW_END_COL)
                                ? s
                                : "sum(" + s + ")")
                    .collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(0L, 3999L, 7998000L, 7998000L, 7998400.0d, 7998800.0d),
                    Arrays.asList(4000L, 7999L, 23998000L, 23998000L, 23998400.0d, 23998800.0d),
                    Arrays.asList(8000L, 11999L, 39998000L, 39998000L, 39998400.0d, 39998800.0d),
                    Arrays.asList(12000L, 15999L, 55998000L, 55998000L, 55998400.0d, 55998800.0d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                resPaths.stream()
                    .map(
                        s ->
                            s.equals(WINDOW_START_COL) || s.equals(WINDOW_END_COL)
                                ? s
                                : "count(" + s + ")")
                    .collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(0L, 3999L, 4000L, 4000L, 4000L, 4000L),
                    Arrays.asList(4000L, 7999L, 4000L, 4000L, 4000L, 4000L),
                    Arrays.asList(8000L, 11999L, 4000L, 4000L, 4000L, 4000L),
                    Arrays.asList(12000L, 15999L, 4000L, 4000L, 4000L, 4000L)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                resPaths.stream()
                    .map(
                        s ->
                            s.equals(WINDOW_START_COL) || s.equals(WINDOW_END_COL)
                                ? s
                                : "avg(" + s + ")")
                    .collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(0L, 3999L, 1999.5d, 1999.5d, 1999.6d, 1999.7d),
                    Arrays.asList(4000L, 7999L, 5999.5d, 5999.5d, 5999.6d, 5999.7d),
                    Arrays.asList(8000L, 11999L, 9999.5d, 9999.5d, 9999.6d, 9999.7d),
                    Arrays.asList(12000L, 15999L, 13999.5d, 13999.5d, 13999.6d, 13999.7d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                resPaths.stream()
                    .map(
                        s ->
                            s.equals(WINDOW_START_COL) || s.equals(WINDOW_END_COL)
                                ? s
                                : "first_value(" + s + ")")
                    .collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(0L, 3999L, 0, 0L, 0.1f, 0.2d),
                    Arrays.asList(4000L, 7999L, 4000, 4000L, 4000.1f, 4000.2d),
                    Arrays.asList(8000L, 11999L, 8000, 8000L, 8000.1f, 8000.2d),
                    Arrays.asList(12000L, 15999L, 12000, 12000L, 12000.1f, 12000.2d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                resPaths.stream()
                    .map(
                        s ->
                            s.equals(WINDOW_START_COL) || s.equals(WINDOW_END_COL)
                                ? s
                                : "last_value(" + s + ")")
                    .collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(0L, 3999L, 3999, 3999L, 3999.1f, 3999.2d),
                    Arrays.asList(4000L, 7999L, 7999, 7999L, 7999.1f, 7999.2d),
                    Arrays.asList(8000L, 11999L, 11999, 11999L, 11999.1f, 11999.2d),
                    Arrays.asList(12000L, 15999L, 15999, 15999L, 15999.1f, 15999.2d)),
                baseDataSection.getTagsList()));

    for (int i = 0; i < aggregateTypes.size(); i++) {
      AggregateType type = aggregateTypes.get(i);
      try {
        SessionQueryDataSet dataSet = conn.downsampleQuery(paths, type, precision);
        compare(expectedResults.get(i), dataSet);
      } catch (SessionException e) {
        LOGGER.error("execute downsample query failed, AggType={}, Precision={}", type, precision);
        fail();
      }
    }
  }

  @Test
  public void testQueryAfterDelete() {
    if (!isAbleToDelete) return;

    // single path delete data
    try {
      // first
      List<String> paths = Collections.singletonList("us.d1.s1");
      conn.deleteDataInColumns(paths, START_KEY, START_KEY + 100);
      SessionQueryDataSet actual = conn.queryData(paths, START_KEY, START_KEY + 200);
      TestDataSection expected =
          baseDataSection.getSubDataSectionWithPath(paths).getSubDataSectionWithKey(100, 200);
      compare(expected, actual);

      // middle
      long mid = (START_KEY + END_KEY) / 2;
      conn.deleteDataInColumns(paths, mid - 50, mid + 50);
      actual = conn.queryData(paths, mid - 100, mid + 100);
      expected =
          baseDataSection
              .getSubDataSectionWithPath(paths)
              .getSubDataSectionWithKey(mid - 100, mid - 50)
              .mergeOther(
                  baseDataSection
                      .getSubDataSectionWithPath(paths)
                      .getSubDataSectionWithKey(mid + 50, mid + 100));
      compare(expected, actual);

      // last
      conn.deleteDataInColumns(paths, END_KEY - 100, END_KEY);
      actual = conn.queryData(paths, END_KEY - 200, END_KEY);
      expected =
          baseDataSection
              .getSubDataSectionWithPath(paths)
              .getSubDataSectionWithKey(END_KEY - 200, END_KEY - 100);
      compare(expected, actual);
    } catch (SessionException e) {
      LOGGER.error("execute delete or query data failed.");
      fail();
    }

    // multi paths delete data
    try {
      // first
      List<String> paths = Arrays.asList("us.d1.s2", "us.d1.s4", "us.d1.s6");
      conn.deleteDataInColumns(paths, START_KEY, START_KEY + 100);
      SessionQueryDataSet actual = conn.queryData(paths, START_KEY, START_KEY + 200);
      TestDataSection expected =
          baseDataSection.getSubDataSectionWithPath(paths).getSubDataSectionWithKey(100, 200);
      compare(expected, actual);

      // middle
      long mid = (START_KEY + END_KEY) / 2;
      conn.deleteDataInColumns(paths, mid - 50, mid + 50);
      actual = conn.queryData(paths, mid - 100, mid + 100);
      expected =
          baseDataSection
              .getSubDataSectionWithPath(paths)
              .getSubDataSectionWithKey(mid - 100, mid - 50)
              .mergeOther(
                  baseDataSection
                      .getSubDataSectionWithPath(paths)
                      .getSubDataSectionWithKey(mid + 50, mid + 100));
      compare(expected, actual);

      // last
      conn.deleteDataInColumns(paths, END_KEY - 100, END_KEY);
      actual = conn.queryData(paths, END_KEY - 200, END_KEY);
      expected =
          baseDataSection
              .getSubDataSectionWithPath(paths)
              .getSubDataSectionWithKey(END_KEY - 200, END_KEY - 100);
      compare(expected, actual);
    } catch (SessionException e) {
      LOGGER.error("execute delete or query data failed.");
      fail();
    }

    // delete with tag
    try {
      List<String> paths = Collections.singletonList("us.d1.s7");
      conn.deleteDataInColumns(
          paths,
          START_KEY,
          START_KEY + 100,
          Collections.singletonList(
              new HashMap<String, List<String>>() {
                {
                  put("k1", Collections.singletonList("v1"));
                }
              }),
          TagFilterType.Precise);
      SessionQueryDataSet actual = conn.queryData(paths, START_KEY, START_KEY + 200);
      TestDataSection expected =
          baseDataSection
              .getSubDataSectionWithPath(paths)
              .getSubDataSectionWithKey(START_KEY, START_KEY + 200);
      compare(expected, actual);

      conn.deleteDataInColumns(
          paths,
          START_KEY,
          START_KEY + 100,
          Collections.singletonList(
              new HashMap<String, List<String>>() {
                {
                  put("k1", Collections.singletonList("v1"));
                  put("k2", Collections.singletonList("v2"));
                }
              }),
          TagFilterType.Precise);
      actual = conn.queryData(paths, START_KEY, START_KEY + 200);
      expected =
          baseDataSection
              .getSubDataSectionWithPath(paths)
              .getSubDataSectionWithKey(START_KEY + 100, START_KEY + 200);
      compare(expected, actual);

      // test OR tagType
      conn.deleteDataInColumns(
          paths,
          START_KEY + 200,
          START_KEY + 300,
          Collections.singletonList(
              new HashMap<String, List<String>>() {
                {
                  put("k2", Collections.singletonList("v2"));
                  put("k4", Collections.singletonList("v4"));
                }
              }),
          TagFilterType.Or);
      actual = conn.queryData(paths, START_KEY + 200, START_KEY + 400);
      expected =
          baseDataSection
              .getSubDataSectionWithPath(paths)
              .getSubDataSectionWithKey(START_KEY + 300, START_KEY + 400);
      compare(expected, actual);

      // test AND tagType
      conn.deleteDataInColumns(
          paths,
          START_KEY + 300,
          START_KEY + 400,
          Collections.singletonList(
              new HashMap<String, List<String>>() {
                {
                  put("k1", Collections.singletonList("v1"));
                  put("k2", Collections.singletonList("v2"));
                }
              }),
          TagFilterType.And);
      actual = conn.queryData(paths, START_KEY + 300, START_KEY + 500);
      expected =
          baseDataSection
              .getSubDataSectionWithPath(paths)
              .getSubDataSectionWithKey(START_KEY + 400, START_KEY + 500);
      compare(expected, actual);
    } catch (SessionException e) {
      LOGGER.error("execute delete or query data failed.");
      fail();
    }
  }
}
