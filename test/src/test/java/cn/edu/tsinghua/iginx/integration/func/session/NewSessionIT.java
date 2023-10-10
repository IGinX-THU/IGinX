package cn.edu.tsinghua.iginx.integration.func.session;

import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.influxdb;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
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
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewSessionIT {

  protected static final Logger logger = LoggerFactory.getLogger(NewSessionIT.class);

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
    if (StorageEngineType.valueOf(conf.getStorageType().toLowerCase()) == influxdb) {
      isInfluxdb = true;
    }
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
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
    conn.closeSession();
  }

  @Before
  public void insertBaseData() {
    // insert base data using all types of insert API.
    List<InsertAPIType> insertAPITypes =
        Arrays.asList(
            InsertAPIType.Row,
            InsertAPIType.NonAlignedRow,
            InsertAPIType.Column,
            InsertAPIType.NonAlignedColumn);
    long size = (END_KEY - START_KEY) / insertAPITypes.size();
    for (int i = 0; i < insertAPITypes.size(); i++) {
      long start = i * size, end = start + size;
      TestDataSection subBaseData = baseDataSection.getSubDataSectionWithKey(start, end);
      insertData(subBaseData, insertAPITypes.get(i));
    }
  }

  @After
  public void clearData() throws SessionException {
    Controller.clearData(conn);
  }

  private void insertData(TestDataSection data, InsertAPIType type) {
    try {
      switch (type) {
        case Row:
          conn.insertRowRecords(
              data.getPaths(),
              data.getKeys().stream().mapToLong(l -> l).toArray(),
              data.getValues().stream()
                  .map(innerList -> innerList.toArray(new Object[0]))
                  .toArray(Object[][]::new),
              data.getTypes(),
              data.getTagsList());
        case NonAlignedRow:
          conn.insertNonAlignedRowRecords(
              data.getPaths(),
              data.getKeys().stream().mapToLong(l -> l).toArray(),
              data.getValues().stream()
                  .map(innerList -> innerList.toArray(new Object[0]))
                  .toArray(Object[][]::new),
              data.getTypes(),
              data.getTagsList());
        case Column:
          conn.insertColumnRecords(
              data.getPaths(),
              data.getKeys().stream().mapToLong(l -> l).toArray(),
              transpose(
                  data.getValues().stream()
                      .map(innerList -> innerList.toArray(new Object[0]))
                      .toArray(Object[][]::new)),
              data.getTypes(),
              data.getTagsList());
        case NonAlignedColumn:
          conn.insertNonAlignedColumnRecords(
              data.getPaths(),
              data.getKeys().stream().mapToLong(l -> l).toArray(),
              transpose(
                  data.getValues().stream()
                      .map(innerList -> innerList.toArray(new Object[0]))
                      .toArray(Object[][]::new)),
              data.getTypes(),
              data.getTagsList());
      }
    } catch (SessionException | ExecutionException e) {
      logger.error("Insert date fail. Caused by: {}.", e.getMessage());
      fail();
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
    compareKeys(expected.getKeys(), actual.getKeys());
    comparePaths(expected.getPaths(), actual.getPaths(), expected.getTagsList());
    compareValues(expected.getValues(), actual.getValues());
  }

  private void compare(TestDataSection expected, SessionAggregateQueryDataSet actual) {
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
      logger.error(
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
  public void testQuery() {
    List<String> paths =
        Arrays.asList(
            "us.d1.s1", "us.d1.s2", "us.d1.s3", "us.d1.s4", "us.d1.s5", "us.d1.s6", "us.d1.s7");

    // query first corner of the base data
    try {
      long start = START_KEY, end = START_KEY + 100;
      SessionQueryDataSet dataSet = conn.queryData(paths, start, end);
      compare(baseDataSection.getSubDataSectionWithKey(start, end), dataSet);
    } catch (SessionException | ExecutionException e) {
      logger.error("execute query data failed.");
      fail();
    }

    // query middle of the base data
    try {
      long mid = (START_KEY + END_KEY) / 2;
      long start = mid - 50, end = mid + 50;
      SessionQueryDataSet dataSet = conn.queryData(paths, start, end);
      compare(baseDataSection.getSubDataSectionWithKey(start, end), dataSet);
    } catch (SessionException | ExecutionException e) {
      logger.error("execute query data failed.");
      fail();
    }

    // query last corner of the base data
    try {
      long start = END_KEY - 100, end = END_KEY;
      SessionQueryDataSet dataSet = conn.queryData(paths, start, end);
      compare(baseDataSection.getSubDataSectionWithKey(start, end), dataSet);
    } catch (SessionException | ExecutionException e) {
      logger.error("execute query data failed.");
      fail();
    }
  }

  @Test
  public void testDeletePaths() {
    // delete single path
    List<String> deleteColumns = Collections.singletonList("us.d1.s2");
    try {
      conn.deleteColumns(deleteColumns);
      SessionQueryDataSet dataSet = conn.queryData(deleteColumns, START_KEY, END_KEY);
      compare(TestDataSection.EMPTY_TEST_DATA_SECTION, dataSet);
    } catch (SessionException | ExecutionException e) {
      logger.error("execute delete columns failed.");
      fail();
    }

    // delete multi paths
    deleteColumns = Arrays.asList("us.d1.s1", "us.d1.s3", "us.d1.s5");
    try {
      conn.deleteColumns(deleteColumns);
      SessionQueryDataSet dataSet = conn.queryData(deleteColumns, START_KEY, END_KEY);
      compare(TestDataSection.EMPTY_TEST_DATA_SECTION, dataSet);
    } catch (SessionException | ExecutionException e) {
      logger.error("execute delete columns failed.");
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
    } catch (SessionException | ExecutionException e) {
      logger.error("execute delete columns failed.");
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
      } catch (SessionException | ExecutionException e) {
        logger.error("execute aggregate query failed, AggType={}", type);
        fail();
      }
    }
  }

  @Test
  public void testDownsampleQuery() {
    List<String> paths = Arrays.asList("us.d1.s2", "us.d1.s3", "us.d1.s4", "us.d1.s5");
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
                paths.stream().map(s -> "max(" + s + ")").collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(3999, 3999L, 3999.1f, 3999.2d),
                    Arrays.asList(7999, 7999L, 7999.1f, 7999.2d),
                    Arrays.asList(11999, 11999L, 11999.1f, 11999.2d),
                    Arrays.asList(15999, 15999L, 15999.1f, 15999.2d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                paths.stream().map(s -> "min(" + s + ")").collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(0, 0L, 0.1f, 0.2d),
                    Arrays.asList(4000, 4000L, 4000.1f, 4000.2d),
                    Arrays.asList(8000, 8000L, 8000.1f, 8000.2d),
                    Arrays.asList(12000, 12000L, 12000.1f, 12000.2d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                paths.stream().map(s -> "sum(" + s + ")").collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(7998000L, 7998000L, 7998400.0d, 7998800.0d),
                    Arrays.asList(23998000L, 23998000L, 23998400.0d, 23998800.0d),
                    Arrays.asList(39998000L, 39998000L, 39998400.0d, 39998800.0d),
                    Arrays.asList(55998000L, 55998000L, 55998400.0d, 55998800.0d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                paths.stream().map(s -> "count(" + s + ")").collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(4000L, 4000L, 4000L, 4000L),
                    Arrays.asList(4000L, 4000L, 4000L, 4000L),
                    Arrays.asList(4000L, 4000L, 4000L, 4000L),
                    Arrays.asList(4000L, 4000L, 4000L, 4000L)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                paths.stream().map(s -> "avg(" + s + ")").collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(1999.5d, 1999.5d, 1999.6d, 1999.7d),
                    Arrays.asList(5999.5d, 5999.5d, 5999.6d, 5999.7d),
                    Arrays.asList(9999.5d, 9999.5d, 9999.6d, 9999.7d),
                    Arrays.asList(13999.5d, 13999.5d, 13999.6d, 13999.7d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                paths.stream().map(s -> "first_value(" + s + ")").collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(0, 0L, 0.1f, 0.2d),
                    Arrays.asList(4000, 4000L, 4000.1f, 4000.2d),
                    Arrays.asList(8000, 8000L, 8000.1f, 8000.2d),
                    Arrays.asList(12000, 12000L, 12000.1f, 12000.2d)),
                baseDataSection.getTagsList()),
            new TestDataSection(
                keys,
                types,
                paths.stream().map(s -> "last_value(" + s + ")").collect(Collectors.toList()),
                Arrays.asList(
                    Arrays.asList(3999, 3999L, 3999.1f, 3999.2d),
                    Arrays.asList(7999, 7999L, 7999.1f, 7999.2d),
                    Arrays.asList(11999, 11999L, 11999.1f, 11999.2d),
                    Arrays.asList(15999, 15999L, 15999.1f, 15999.2d)),
                baseDataSection.getTagsList()));

    for (int i = 0; i < aggregateTypes.size(); i++) {
      AggregateType type = aggregateTypes.get(i);
      try {
        SessionQueryDataSet dataSet =
            conn.downsampleQuery(paths, START_KEY, END_KEY, type, precision);
        compare(expectedResults.get(i), dataSet);
      } catch (SessionException | ExecutionException e) {
        logger.error("execute downsample query failed, AggType={}, Precision={}", type, precision);
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
    } catch (SessionException | ExecutionException e) {
      logger.error("execute delete or query data failed.");
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
    } catch (SessionException | ExecutionException e) {
      logger.error("execute delete or query data failed.");
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
    } catch (SessionException | ExecutionException e) {
      logger.error("execute delete or query data failed.");
      fail();
    }
  }
}
