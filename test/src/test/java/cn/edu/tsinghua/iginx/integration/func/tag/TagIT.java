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
package cn.edu.tsinghua.iginx.integration.func.tag;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.CLEAR_DUMMY_DATA_CAUTION;
import static cn.edu.tsinghua.iginx.integration.controller.Controller.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.func.session.InsertAPIType;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TagIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(TagIT.class);

  private static Session session;

  private static boolean isAbleToClearData;

  private boolean isAbleToDelete = true;

  private boolean isScaling = false;

  public TagIT() {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    this.isAbleToClearData = dbConf.getEnumValue(DBConf.DBConfType.isAbleToClearData);
    this.isAbleToDelete = dbConf.getEnumValue(DBConf.DBConfType.isAbleToDelete);
    this.isScaling = conf.isScaling();
  }

  @BeforeClass
  public static void setUp() throws SessionException {
    isAbleToClearData = true;
    session = new Session("127.0.0.1", 6888, "root", "root");
    session.openSession();
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    clearAllData(session);
    session.closeSession();
  }

  @Before
  public void insertData() throws SessionException {
    //    "insert into ah.hr01 (key, s, v, s[t1=v1, t2=vv1], v[t1=v2, t2=vv1]) values (0, 1, 2, 3,
    // 4), (1, 2, 3, 4, 5), (2, 3, 4, 5, 6), (3, 4, 5, 6, 7);",
    //    "insert into ah.hr02 (key, s, v) values (100, true, \"v1\");",
    //    "insert into ah.hr02[t1=v1] (key, s, v) values (400, false, \"v4\");",
    //    "insert into ah.hr02[t1=v1,t2=v2] (key, v) values (800, \"v8\");",
    //    "insert into ah.hr03 (key, s[t1=vv1,t2=v2], v[t1=vv11]) values (1600, true, 16);",
    //    "insert into ah.hr03 (key, s[t1=v1,t2=vv2], v[t1=v1], v[t1=vv11]) values (3200, true, 16,
    // 32);"

    // round 1
    List<String> pathList1 = Arrays.asList("ah.hr01.s", "ah.hr01.s", "ah.hr01.v", "ah.hr01.v");
    List<DataType> dataTypeList1 =
        Arrays.asList(DataType.LONG, DataType.LONG, DataType.LONG, DataType.LONG);
    List<Long> keyList1 = Arrays.asList(0L, 1L, 2L, 3L);
    List<Map<String, String>> tagList1 =
        Arrays.asList(
            new HashMap<>(),
            new HashMap<String, String>() {
              {
                put("t1", "v1");
                put("t2", "vv1");
              }
            },
            new HashMap<>(),
            new HashMap<String, String>() {
              {
                put("t1", "v2");
                put("t2", "vv1");
              }
            });
    List<List<Object>> valuesList1 =
        Arrays.asList(
            Arrays.asList(1L, 2L, 3L, 4L),
            Arrays.asList(3L, 4L, 5L, 6L),
            Arrays.asList(2L, 3L, 4L, 5L),
            Arrays.asList(4L, 5L, 6L, 7L));
    Controller.writeColumnsData(
        session,
        pathList1,
        IntStream.range(0, pathList1.size())
            .mapToObj(i -> new ArrayList<>(keyList1))
            .collect(Collectors.toList()),
        dataTypeList1,
        valuesList1,
        tagList1,
        InsertAPIType.Column,
        false);

    // round 2
    List<String> pathList2 = Arrays.asList("ah.hr02.s", "ah.hr02.v");
    List<DataType> dataTypeList2 = Arrays.asList(DataType.BOOLEAN, DataType.BINARY);
    List<Long> keyList2 = Arrays.asList(100L);
    List<Map<String, String>> tagList2 = Arrays.asList(new HashMap<>(), new HashMap<>());
    List<List<Object>> valuesList2 =
        Arrays.asList(Arrays.asList(true, "v1".getBytes()), Arrays.asList(false, "v4".getBytes()));
    Controller.writeRowsData(
        session,
        pathList2,
        keyList2,
        dataTypeList2,
        valuesList2,
        tagList2,
        InsertAPIType.Row,
        false);

    // round 3
    List<String> pathList3 = Arrays.asList("ah.hr02.s", "ah.hr02.v");
    List<DataType> dataTypeList3 = Arrays.asList(DataType.BOOLEAN, DataType.BINARY);
    List<Long> keyList3 = Arrays.asList(400L);
    List<Map<String, String>> tagList3 =
        Arrays.asList(
            new HashMap<String, String>() {
              {
                put("t1", "v1");
              }
            },
            new HashMap<String, String>() {
              {
                put("t1", "v1");
              }
            });
    List<List<Object>> valuesList3 = Arrays.asList(Arrays.asList(false, "v4".getBytes()));
    Controller.writeRowsData(
        session,
        pathList3,
        keyList3,
        dataTypeList3,
        valuesList3,
        tagList3,
        InsertAPIType.Row,
        false);

    // round 4
    List<String> pathList4 = Arrays.asList("ah.hr02.v");
    List<DataType> dataTypeList4 = Arrays.asList(DataType.BINARY);
    List<Long> keyList4 = Arrays.asList(800L);
    List<Map<String, String>> tagList4 =
        Arrays.asList(
            new HashMap<String, String>() {
              {
                put("t1", "v1");
                put("t2", "v2");
              }
            });
    List<List<Object>> valuesList4 = Arrays.asList(Arrays.asList("v8".getBytes()));
    Controller.writeRowsData(
        session,
        pathList4,
        keyList4,
        dataTypeList4,
        valuesList4,
        tagList4,
        InsertAPIType.Row,
        false);

    // round 5
    List<String> pathList5 = Arrays.asList("ah.hr03.s", "ah.hr03.v");
    List<DataType> dataTypeList5 = Arrays.asList(DataType.BOOLEAN, DataType.LONG);
    List<Long> keyList5 = Arrays.asList(1600L);
    List<Map<String, String>> tagList5 =
        Arrays.asList(
            new HashMap<String, String>() {
              {
                put("t1", "vv1");
                put("t2", "v2");
              }
            },
            new HashMap<String, String>() {
              {
                put("t1", "vv11");
              }
            });
    List<List<Object>> valuesList5 = Arrays.asList(Arrays.asList(true, 16L));
    Controller.writeRowsData(
        session,
        pathList5,
        keyList5,
        dataTypeList5,
        valuesList5,
        tagList5,
        InsertAPIType.Row,
        false);

    // round 6
    List<String> pathList6 = Arrays.asList("ah.hr03.s", "ah.hr03.v", "ah.hr03.v");
    List<DataType> dataTypeList6 = Arrays.asList(DataType.BOOLEAN, DataType.LONG, DataType.LONG);
    List<Long> keyList6 = Arrays.asList(3200L);
    List<Map<String, String>> tagList6 =
        Arrays.asList(
            new HashMap<String, String>() {
              {
                put("t1", "v1");
                put("t2", "vv2");
              }
            },
            new HashMap<String, String>() {
              {
                put("t1", "v1");
              }
            },
            new HashMap<String, String>() {
              {
                put("t1", "vv11");
              }
            });
    List<List<Object>> valuesList6 = Arrays.asList(Arrays.asList(true, 16L, 32L));
    Controller.writeRowsData(
        session,
        pathList6,
        keyList6,
        dataTypeList6,
        valuesList6,
        tagList6,
        InsertAPIType.Row,
        false);
    Controller.after(session);
  }

  @After
  public void clearData() {
    Controller.clearData(session);
  }

  private void executeAndCompare(String statement, String expectedOutput) {
    String actualOutput = execute(statement);
    assertEquals(expectedOutput, actualOutput);
  }

  private String execute(String statement) {
    LOGGER.info("Execute Statement: \"{}\"", statement);

    SessionExecuteSqlResult res = null;
    try {
      res = session.executeSql(statement);
    } catch (SessionException e) {
      if (e.toString().trim().contains(CLEAR_DUMMY_DATA_CAUTION)) {
        LOGGER.warn(CLEAR_DATA_WARNING);
      } else {
        LOGGER.error("Statement: \"{}\" execute fail. Caused by: ", statement, e);
        fail();
      }
    }

    if (res == null) {
      return "";
    }

    if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      LOGGER.error(
          "Statement: \"{}\" execute fail. Caused by: ", statement, res.getParseErrorMsg());
      fail();
      return "";
    }

    return res.getResultInString(false, "");
  }

  @Test
  public void testShowColumnsWithTags() {
    String statement = "SHOW COLUMNS ah.*;";
    String expected =
        "Columns:\n"
            + "+-----------------------+--------+\n"
            + "|                   Path|DataType|\n"
            + "+-----------------------+--------+\n"
            + "|              ah.hr01.s|    LONG|\n"
            + "|ah.hr01.s{t1=v1,t2=vv1}|    LONG|\n"
            + "|              ah.hr01.v|    LONG|\n"
            + "|ah.hr01.v{t1=v2,t2=vv1}|    LONG|\n"
            + "|              ah.hr02.s| BOOLEAN|\n"
            + "|       ah.hr02.s{t1=v1}| BOOLEAN|\n"
            + "|              ah.hr02.v|  BINARY|\n"
            + "|       ah.hr02.v{t1=v1}|  BINARY|\n"
            + "| ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
            + "|ah.hr03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
            + "|ah.hr03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
            + "|       ah.hr03.v{t1=v1}|    LONG|\n"
            + "|     ah.hr03.v{t1=vv11}|    LONG|\n"
            + "+-----------------------+--------+\n"
            + "Total line number = 13\n";
    executeAndCompare(statement, expected);

    statement = "SHOW COLUMNS ah.* limit 6;";
    expected =
        "Columns:\n"
            + "+-----------------------+--------+\n"
            + "|                   Path|DataType|\n"
            + "+-----------------------+--------+\n"
            + "|              ah.hr01.s|    LONG|\n"
            + "|ah.hr01.s{t1=v1,t2=vv1}|    LONG|\n"
            + "|              ah.hr01.v|    LONG|\n"
            + "|ah.hr01.v{t1=v2,t2=vv1}|    LONG|\n"
            + "|              ah.hr02.s| BOOLEAN|\n"
            + "|       ah.hr02.s{t1=v1}| BOOLEAN|\n"
            + "+-----------------------+--------+\n"
            + "Total line number = 6\n";
    executeAndCompare(statement, expected);

    statement = "SHOW COLUMNS ah.* limit 3 offset 7;";
    expected =
        "Columns:\n"
            + "+-----------------------+--------+\n"
            + "|                   Path|DataType|\n"
            + "+-----------------------+--------+\n"
            + "|       ah.hr02.v{t1=v1}|  BINARY|\n"
            + "| ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
            + "|ah.hr03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
            + "+-----------------------+--------+\n"
            + "Total line number = 3\n";
    executeAndCompare(statement, expected);

    statement = "SHOW COLUMNS ah.* limit 7, 3;";
    expected =
        "Columns:\n"
            + "+-----------------------+--------+\n"
            + "|                   Path|DataType|\n"
            + "+-----------------------+--------+\n"
            + "|       ah.hr02.v{t1=v1}|  BINARY|\n"
            + "| ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
            + "|ah.hr03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
            + "+-----------------------+--------+\n"
            + "Total line number = 3\n";
    executeAndCompare(statement, expected);

    statement = "SHOW COLUMNS ah.hr02.*;";
    expected =
        "Columns:\n"
            + "+----------------------+--------+\n"
            + "|                  Path|DataType|\n"
            + "+----------------------+--------+\n"
            + "|             ah.hr02.s| BOOLEAN|\n"
            + "|      ah.hr02.s{t1=v1}| BOOLEAN|\n"
            + "|             ah.hr02.v|  BINARY|\n"
            + "|      ah.hr02.v{t1=v1}|  BINARY|\n"
            + "|ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
            + "+----------------------+--------+\n"
            + "Total line number = 5\n";
    executeAndCompare(statement, expected);

    statement = "SHOW COLUMNS ah.hr02.* limit 3 offset 2;";
    expected =
        "Columns:\n"
            + "+----------------------+--------+\n"
            + "|                  Path|DataType|\n"
            + "+----------------------+--------+\n"
            + "|             ah.hr02.v|  BINARY|\n"
            + "|      ah.hr02.v{t1=v1}|  BINARY|\n"
            + "|ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
            + "+----------------------+--------+\n"
            + "Total line number = 3\n";
    executeAndCompare(statement, expected);

    statement = "SHOW COLUMNS ah.hr02.*, ah.hr03.*;";
    expected =
        "Columns:\n"
            + "+-----------------------+--------+\n"
            + "|                   Path|DataType|\n"
            + "+-----------------------+--------+\n"
            + "|              ah.hr02.s| BOOLEAN|\n"
            + "|       ah.hr02.s{t1=v1}| BOOLEAN|\n"
            + "|              ah.hr02.v|  BINARY|\n"
            + "|       ah.hr02.v{t1=v1}|  BINARY|\n"
            + "| ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
            + "|ah.hr03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
            + "|ah.hr03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
            + "|       ah.hr03.v{t1=v1}|    LONG|\n"
            + "|     ah.hr03.v{t1=vv11}|    LONG|\n"
            + "+-----------------------+--------+\n"
            + "Total line number = 9\n";
    executeAndCompare(statement, expected);

    statement = "SHOW COLUMNS ah.hr02.* with t1=v1;";
    expected =
        "Columns:\n"
            + "+----------------------+--------+\n"
            + "|                  Path|DataType|\n"
            + "+----------------------+--------+\n"
            + "|      ah.hr02.s{t1=v1}| BOOLEAN|\n"
            + "|      ah.hr02.v{t1=v1}|  BINARY|\n"
            + "|ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
            + "+----------------------+--------+\n"
            + "Total line number = 3\n";
    executeAndCompare(statement, expected);

    statement = "SHOW COLUMNS ah.hr02.* with t1=v1 limit 2 offset 1;";
    expected =
        "Columns:\n"
            + "+----------------------+--------+\n"
            + "|                  Path|DataType|\n"
            + "+----------------------+--------+\n"
            + "|      ah.hr02.v{t1=v1}|  BINARY|\n"
            + "|ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
            + "+----------------------+--------+\n"
            + "Total line number = 2\n";
    executeAndCompare(statement, expected);

    statement = "SHOW COLUMNS ah.hr02.* with_precise t1=v1;";
    expected =
        "Columns:\n"
            + "+----------------+--------+\n"
            + "|            Path|DataType|\n"
            + "+----------------+--------+\n"
            + "|ah.hr02.s{t1=v1}| BOOLEAN|\n"
            + "|ah.hr02.v{t1=v1}|  BINARY|\n"
            + "+----------------+--------+\n"
            + "Total line number = 2\n";
    executeAndCompare(statement, expected);

    statement = "SHOW COLUMNS ah.hr02.* with_precise t1=v1 AND t2=v2;";
    expected =
        "Columns:\n"
            + "+----------------------+--------+\n"
            + "|                  Path|DataType|\n"
            + "+----------------------+--------+\n"
            + "|ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
            + "+----------------------+--------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "SHOW COLUMNS ah.hr02.* with t1=v1 AND t2=v2;";
    expected =
        "Columns:\n"
            + "+----------------------+--------+\n"
            + "|                  Path|DataType|\n"
            + "+----------------------+--------+\n"
            + "|ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
            + "+----------------------+--------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "SHOW COLUMNS  ah.* WITHOUT TAG;";
    expected =
        "Columns:\n"
            + "+---------+--------+\n"
            + "|     Path|DataType|\n"
            + "+---------+--------+\n"
            + "|ah.hr01.s|    LONG|\n"
            + "|ah.hr01.v|    LONG|\n"
            + "|ah.hr02.s| BOOLEAN|\n"
            + "|ah.hr02.v|  BINARY|\n"
            + "+---------+--------+\n"
            + "Total line number = 4\n";
    executeAndCompare(statement, expected);
  }

  @Test
  public void testCountPoints() {
    if (isScaling) return;
    String statement = "COUNT POINTS;";
    String expected = "Points num: 26\n";
    executeAndCompare(statement, expected);
  }

  @Test
  public void testCountPath() {
    String statement = "SELECT COUNT(*) FROM ah;";
    String expected =
        "ResultSets:\n"
            + "+----------------+------------------------------+----------------+------------------------------+----------------+-----------------------+----------------+-----------------------------+-----------------------+------------------------------+------------------------------+-----------------------+-------------------------+\n"
            + "|count(ah.hr01.s)|count(ah.hr01.s{t1=v1,t2=vv1})|count(ah.hr01.v)|count(ah.hr01.v{t1=v2,t2=vv1})|count(ah.hr02.s)|count(ah.hr02.s{t1=v1})|count(ah.hr02.v)|count(ah.hr02.v{t1=v1,t2=v2})|count(ah.hr02.v{t1=v1})|count(ah.hr03.s{t1=v1,t2=vv2})|count(ah.hr03.s{t1=vv1,t2=v2})|count(ah.hr03.v{t1=v1})|count(ah.hr03.v{t1=vv11})|\n"
            + "+----------------+------------------------------+----------------+------------------------------+----------------+-----------------------+----------------+-----------------------------+-----------------------+------------------------------+------------------------------+-----------------------+-------------------------+\n"
            + "|               4|                             4|               4|                             4|               1|                      1|               1|                            1|                      1|                             1|                             1|                      1|                        2|\n"
            + "+----------------+------------------------------+----------------+------------------------------+----------------+-----------------------+----------------+-----------------------------+-----------------------+------------------------------+------------------------------+-----------------------+-------------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);
  }

  @Test
  public void testBasicQuery() {
    String statement = "SELECT * FROM ah;";
    String expected =
        "ResultSets:\n"
            + "+----+---------+-----------------------+---------+-----------------------+---------+----------------+---------+----------------------+----------------+-----------------------+-----------------------+----------------+------------------+\n"
            + "| key|ah.hr01.s|ah.hr01.s{t1=v1,t2=vv1}|ah.hr01.v|ah.hr01.v{t1=v2,t2=vv1}|ah.hr02.s|ah.hr02.s{t1=v1}|ah.hr02.v|ah.hr02.v{t1=v1,t2=v2}|ah.hr02.v{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|ah.hr03.v{t1=v1}|ah.hr03.v{t1=vv11}|\n"
            + "+----+---------+-----------------------+---------+-----------------------+---------+----------------+---------+----------------------+----------------+-----------------------+-----------------------+----------------+------------------+\n"
            + "|   0|        1|                      3|        2|                      4|     null|            null|     null|                  null|            null|                   null|                   null|            null|              null|\n"
            + "|   1|        2|                      4|        3|                      5|     null|            null|     null|                  null|            null|                   null|                   null|            null|              null|\n"
            + "|   2|        3|                      5|        4|                      6|     null|            null|     null|                  null|            null|                   null|                   null|            null|              null|\n"
            + "|   3|        4|                      6|        5|                      7|     null|            null|     null|                  null|            null|                   null|                   null|            null|              null|\n"
            + "| 100|     null|                   null|     null|                   null|     true|            null|       v1|                  null|            null|                   null|                   null|            null|              null|\n"
            + "| 400|     null|                   null|     null|                   null|     null|           false|     null|                  null|              v4|                   null|                   null|            null|              null|\n"
            + "| 800|     null|                   null|     null|                   null|     null|            null|     null|                    v8|            null|                   null|                   null|            null|              null|\n"
            + "|1600|     null|                   null|     null|                   null|     null|            null|     null|                  null|            null|                   null|                   true|            null|                16|\n"
            + "|3200|     null|                   null|     null|                   null|     null|            null|     null|                  null|            null|                   true|                   null|              16|                32|\n"
            + "+----+---------+-----------------------+---------+-----------------------+---------+----------------+---------+----------------------+----------------+-----------------------+-----------------------+----------------+------------------+\n"
            + "Total line number = 9\n";
    executeAndCompare(statement, expected);
  }

  @Test
  public void testQueryWithoutTags() {
    String statement = "SELECT s FROM ah.*;";
    String expected =
        "ResultSets:\n"
            + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "| key|ah.hr01.s|ah.hr01.s{t1=v1,t2=vv1}|ah.hr02.s|ah.hr02.s{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
            + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "|   0|        1|                      3|     null|            null|                   null|                   null|\n"
            + "|   1|        2|                      4|     null|            null|                   null|                   null|\n"
            + "|   2|        3|                      5|     null|            null|                   null|                   null|\n"
            + "|   3|        4|                      6|     null|            null|                   null|                   null|\n"
            + "| 100|     null|                   null|     true|            null|                   null|                   null|\n"
            + "| 400|     null|                   null|     null|           false|                   null|                   null|\n"
            + "|1600|     null|                   null|     null|            null|                   null|                   true|\n"
            + "|3200|     null|                   null|     null|            null|                   true|                   null|\n"
            + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "Total line number = 8\n";
    executeAndCompare(statement, expected);

    statement = "SELECT s FROM ah.* WITHOUT TAG;";
    expected =
        "ResultSets:\n"
            + "+---+---------+---------+\n"
            + "|key|ah.hr01.s|ah.hr02.s|\n"
            + "+---+---------+---------+\n"
            + "|  0|        1|     null|\n"
            + "|  1|        2|     null|\n"
            + "|  2|        3|     null|\n"
            + "|  3|        4|     null|\n"
            + "|100|     null|     true|\n"
            + "+---+---------+---------+\n"
            + "Total line number = 5\n";
    executeAndCompare(statement, expected);

    statement = "SELECT v FROM ah.*;";
    expected =
        "ResultSets:\n"
            + "+----+---------+-----------------------+---------+----------------------+----------------+----------------+------------------+\n"
            + "| key|ah.hr01.v|ah.hr01.v{t1=v2,t2=vv1}|ah.hr02.v|ah.hr02.v{t1=v1,t2=v2}|ah.hr02.v{t1=v1}|ah.hr03.v{t1=v1}|ah.hr03.v{t1=vv11}|\n"
            + "+----+---------+-----------------------+---------+----------------------+----------------+----------------+------------------+\n"
            + "|   0|        2|                      4|     null|                  null|            null|            null|              null|\n"
            + "|   1|        3|                      5|     null|                  null|            null|            null|              null|\n"
            + "|   2|        4|                      6|     null|                  null|            null|            null|              null|\n"
            + "|   3|        5|                      7|     null|                  null|            null|            null|              null|\n"
            + "| 100|     null|                   null|       v1|                  null|            null|            null|              null|\n"
            + "| 400|     null|                   null|     null|                  null|              v4|            null|              null|\n"
            + "| 800|     null|                   null|     null|                    v8|            null|            null|              null|\n"
            + "|1600|     null|                   null|     null|                  null|            null|            null|                16|\n"
            + "|3200|     null|                   null|     null|                  null|            null|              16|                32|\n"
            + "+----+---------+-----------------------+---------+----------------------+----------------+----------------+------------------+\n"
            + "Total line number = 9\n";
    executeAndCompare(statement, expected);

    statement = "SELECT v FROM ah.* WITHOUT TAG;";
    expected =
        "ResultSets:\n"
            + "+---+---------+---------+\n"
            + "|key|ah.hr01.v|ah.hr02.v|\n"
            + "+---+---------+---------+\n"
            + "|  0|        2|     null|\n"
            + "|  1|        3|     null|\n"
            + "|  2|        4|     null|\n"
            + "|  3|        5|     null|\n"
            + "|100|     null|       v1|\n"
            + "+---+---------+---------+\n"
            + "Total line number = 5\n";
    executeAndCompare(statement, expected);
  }

  @Test
  public void testQueryWithTag() {
    String statement = "SELECT s FROM ah.* with t1=v1;";
    String expected =
        "ResultSets:\n"
            + "+----+-----------------------+----------------+-----------------------+\n"
            + "| key|ah.hr01.s{t1=v1,t2=vv1}|ah.hr02.s{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|\n"
            + "+----+-----------------------+----------------+-----------------------+\n"
            + "|   0|                      3|            null|                   null|\n"
            + "|   1|                      4|            null|                   null|\n"
            + "|   2|                      5|            null|                   null|\n"
            + "|   3|                      6|            null|                   null|\n"
            + "| 400|                   null|           false|                   null|\n"
            + "|3200|                   null|            null|                   true|\n"
            + "+----+-----------------------+----------------+-----------------------+\n"
            + "Total line number = 6\n";
    executeAndCompare(statement, expected);

    statement = "SELECT s FROM ah.* with_precise t1=v1;";
    expected =
        "ResultSets:\n"
            + "+---+----------------+\n"
            + "|key|ah.hr02.s{t1=v1}|\n"
            + "+---+----------------+\n"
            + "|400|           false|\n"
            + "+---+----------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);
  }

  @Test
  public void testQueryWithMultiTags() {
    String statement = "SELECT s FROM ah.* with t1=v1 OR t2=v2;";
    String expected =
        "ResultSets:\n"
            + "+----+-----------------------+----------------+-----------------------+-----------------------+\n"
            + "| key|ah.hr01.s{t1=v1,t2=vv1}|ah.hr02.s{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
            + "+----+-----------------------+----------------+-----------------------+-----------------------+\n"
            + "|   0|                      3|            null|                   null|                   null|\n"
            + "|   1|                      4|            null|                   null|                   null|\n"
            + "|   2|                      5|            null|                   null|                   null|\n"
            + "|   3|                      6|            null|                   null|                   null|\n"
            + "| 400|                   null|           false|                   null|                   null|\n"
            + "|1600|                   null|            null|                   null|                   true|\n"
            + "|3200|                   null|            null|                   true|                   null|\n"
            + "+----+-----------------------+----------------+-----------------------+-----------------------+\n"
            + "Total line number = 7\n";
    executeAndCompare(statement, expected);

    statement = "SELECT s FROM ah.* with t1=v1 AND t2=vv2;";
    expected =
        "ResultSets:\n"
            + "+----+-----------------------+\n"
            + "| key|ah.hr03.s{t1=v1,t2=vv2}|\n"
            + "+----+-----------------------+\n"
            + "|3200|                   true|\n"
            + "+----+-----------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "SELECT s FROM ah.* with_precise t1=v1 AND t2=vv2 OR t1=vv1 AND t2=v2;";
    expected =
        "ResultSets:\n"
            + "+----+-----------------------+-----------------------+\n"
            + "| key|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
            + "+----+-----------------------+-----------------------+\n"
            + "|1600|                   null|                   true|\n"
            + "|3200|                   true|                   null|\n"
            + "+----+-----------------------+-----------------------+\n"
            + "Total line number = 2\n";
    executeAndCompare(statement, expected);

    statement = "SELECT s FROM ah.* with_precise t1=v1;";
    expected =
        "ResultSets:\n"
            + "+---+----------------+\n"
            + "|key|ah.hr02.s{t1=v1}|\n"
            + "+---+----------------+\n"
            + "|400|           false|\n"
            + "+---+----------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);
  }

  @Test
  public void testDeleteWithTag() {
    if (!isAbleToDelete || isScaling) return;
    String statement = "SELECT s FROM ah.*;";
    String expected =
        "ResultSets:\n"
            + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "| key|ah.hr01.s|ah.hr01.s{t1=v1,t2=vv1}|ah.hr02.s|ah.hr02.s{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
            + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "|   0|        1|                      3|     null|            null|                   null|                   null|\n"
            + "|   1|        2|                      4|     null|            null|                   null|                   null|\n"
            + "|   2|        3|                      5|     null|            null|                   null|                   null|\n"
            + "|   3|        4|                      6|     null|            null|                   null|                   null|\n"
            + "| 100|     null|                   null|     true|            null|                   null|                   null|\n"
            + "| 400|     null|                   null|     null|           false|                   null|                   null|\n"
            + "|1600|     null|                   null|     null|            null|                   null|                   true|\n"
            + "|3200|     null|                   null|     null|            null|                   true|                   null|\n"
            + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "Total line number = 8\n";
    executeAndCompare(statement, expected);

    statement = "DELETE FROM ah.*.s WHERE key > 10 WITH t1=v1;";
    execute(statement);

    statement = "SELECT s FROM ah.*;";
    expected =
        "ResultSets:\n"
            + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "| key|ah.hr01.s|ah.hr01.s{t1=v1,t2=vv1}|ah.hr02.s|ah.hr02.s{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
            + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "|   0|        1|                      3|     null|            null|                   null|                   null|\n"
            + "|   1|        2|                      4|     null|            null|                   null|                   null|\n"
            + "|   2|        3|                      5|     null|            null|                   null|                   null|\n"
            + "|   3|        4|                      6|     null|            null|                   null|                   null|\n"
            + "| 100|     null|                   null|     true|            null|                   null|                   null|\n"
            + "|1600|     null|                   null|     null|            null|                   null|                   true|\n"
            + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "Total line number = 6\n";
    executeAndCompare(statement, expected);
  }

  @Test
  public void testDeleteWithMultiTags() {
    if (!isAbleToDelete || isScaling) return;
    String statement = "SELECT s FROM ah.*;";
    String expected =
        "ResultSets:\n"
            + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "| key|ah.hr01.s|ah.hr01.s{t1=v1,t2=vv1}|ah.hr02.s|ah.hr02.s{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
            + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "|   0|        1|                      3|     null|            null|                   null|                   null|\n"
            + "|   1|        2|                      4|     null|            null|                   null|                   null|\n"
            + "|   2|        3|                      5|     null|            null|                   null|                   null|\n"
            + "|   3|        4|                      6|     null|            null|                   null|                   null|\n"
            + "| 100|     null|                   null|     true|            null|                   null|                   null|\n"
            + "| 400|     null|                   null|     null|           false|                   null|                   null|\n"
            + "|1600|     null|                   null|     null|            null|                   null|                   true|\n"
            + "|3200|     null|                   null|     null|            null|                   true|                   null|\n"
            + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "Total line number = 8\n";
    executeAndCompare(statement, expected);

    statement = "DELETE FROM ah.*.s WHERE key > 10 WITH t1=v1 AND t2=vv2;";
    execute(statement);

    statement = "SELECT s FROM ah.*;";
    expected =
        "ResultSets:\n"
            + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "| key|ah.hr01.s|ah.hr01.s{t1=v1,t2=vv1}|ah.hr02.s|ah.hr02.s{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
            + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "|   0|        1|                      3|     null|            null|                   null|                   null|\n"
            + "|   1|        2|                      4|     null|            null|                   null|                   null|\n"
            + "|   2|        3|                      5|     null|            null|                   null|                   null|\n"
            + "|   3|        4|                      6|     null|            null|                   null|                   null|\n"
            + "| 100|     null|                   null|     true|            null|                   null|                   null|\n"
            + "| 400|     null|                   null|     null|           false|                   null|                   null|\n"
            + "|1600|     null|                   null|     null|            null|                   null|                   true|\n"
            + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "Total line number = 7\n";
    executeAndCompare(statement, expected);

    statement = "DELETE FROM ah.*.s WHERE key > 10 WITH_PRECISE t1=v1;";
    execute(statement);

    statement = "SELECT s FROM ah.*;";
    expected =
        "ResultSets:\n"
            + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "| key|ah.hr01.s|ah.hr01.s{t1=v1,t2=vv1}|ah.hr02.s|ah.hr02.s{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
            + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "|   0|        1|                      3|     null|            null|                   null|                   null|\n"
            + "|   1|        2|                      4|     null|            null|                   null|                   null|\n"
            + "|   2|        3|                      5|     null|            null|                   null|                   null|\n"
            + "|   3|        4|                      6|     null|            null|                   null|                   null|\n"
            + "| 100|     null|                   null|     true|            null|                   null|                   null|\n"
            + "|1600|     null|                   null|     null|            null|                   null|                   true|\n"
            + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "Total line number = 6\n";
    executeAndCompare(statement, expected);

    statement = "DELETE FROM ah.*.s WHERE key > 10 WITH t1=v1 OR t2=v2;";
    execute(statement);

    statement = "SELECT s FROM ah.*;";
    expected =
        "ResultSets:\n"
            + "+---+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "|key|ah.hr01.s|ah.hr01.s{t1=v1,t2=vv1}|ah.hr02.s|ah.hr02.s{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
            + "+---+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "|  0|        1|                      3|     null|            null|                   null|                   null|\n"
            + "|  1|        2|                      4|     null|            null|                   null|                   null|\n"
            + "|  2|        3|                      5|     null|            null|                   null|                   null|\n"
            + "|  3|        4|                      6|     null|            null|                   null|                   null|\n"
            + "|100|     null|                   null|     true|            null|                   null|                   null|\n"
            + "+---+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
            + "Total line number = 5\n";
    executeAndCompare(statement, expected);
  }

  @Test
  public void testDeleteTSWithTag() {
    if (!isAbleToDelete || isScaling) return;
    String showColumns = "SHOW COLUMNS ah.*;";
    String expected =
        "Columns:\n"
            + "+-----------------------+--------+\n"
            + "|                   Path|DataType|\n"
            + "+-----------------------+--------+\n"
            + "|              ah.hr01.s|    LONG|\n"
            + "|ah.hr01.s{t1=v1,t2=vv1}|    LONG|\n"
            + "|              ah.hr01.v|    LONG|\n"
            + "|ah.hr01.v{t1=v2,t2=vv1}|    LONG|\n"
            + "|              ah.hr02.s| BOOLEAN|\n"
            + "|       ah.hr02.s{t1=v1}| BOOLEAN|\n"
            + "|              ah.hr02.v|  BINARY|\n"
            + "|       ah.hr02.v{t1=v1}|  BINARY|\n"
            + "| ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
            + "|ah.hr03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
            + "|ah.hr03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
            + "|       ah.hr03.v{t1=v1}|    LONG|\n"
            + "|     ah.hr03.v{t1=vv11}|    LONG|\n"
            + "+-----------------------+--------+\n"
            + "Total line number = 13\n";
    executeAndCompare(showColumns, expected);

    String deleteTimeSeries = "DELETE COLUMNS ah.*.s WITH t1=v1;";
    execute(deleteTimeSeries);

    showColumns = "SHOW COLUMNS ah.*;";
    expected =
        "Columns:\n"
            + "+-----------------------+--------+\n"
            + "|                   Path|DataType|\n"
            + "+-----------------------+--------+\n"
            + "|              ah.hr01.s|    LONG|\n"
            + "|              ah.hr01.v|    LONG|\n"
            + "|ah.hr01.v{t1=v2,t2=vv1}|    LONG|\n"
            + "|              ah.hr02.s| BOOLEAN|\n"
            + "|              ah.hr02.v|  BINARY|\n"
            + "|       ah.hr02.v{t1=v1}|  BINARY|\n"
            + "| ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
            + "|ah.hr03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
            + "|       ah.hr03.v{t1=v1}|    LONG|\n"
            + "|     ah.hr03.v{t1=vv11}|    LONG|\n"
            + "+-----------------------+--------+\n"
            + "Total line number = 10\n";
    executeAndCompare(showColumns, expected);

    String showColumnsData = "SELECT s FROM ah.* WITH t1=v1;";
    expected = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    executeAndCompare(showColumnsData, expected);

    deleteTimeSeries = "DELETE COLUMNS ah.*.v WITH_PRECISE t1=v1;";
    execute(deleteTimeSeries);

    showColumns = "SHOW COLUMNS ah.*;";
    expected =
        "Columns:\n"
            + "+-----------------------+--------+\n"
            + "|                   Path|DataType|\n"
            + "+-----------------------+--------+\n"
            + "|              ah.hr01.s|    LONG|\n"
            + "|              ah.hr01.v|    LONG|\n"
            + "|ah.hr01.v{t1=v2,t2=vv1}|    LONG|\n"
            + "|              ah.hr02.s| BOOLEAN|\n"
            + "|              ah.hr02.v|  BINARY|\n"
            + "| ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
            + "|ah.hr03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
            + "|     ah.hr03.v{t1=vv11}|    LONG|\n"
            + "+-----------------------+--------+\n"
            + "Total line number = 8\n";
    executeAndCompare(showColumns, expected);

    showColumnsData = "SELECT v FROM ah.* WITH t1=v1;";
    expected =
        "ResultSets:\n"
            + "+---+----------------------+\n"
            + "|key|ah.hr02.v{t1=v1,t2=v2}|\n"
            + "+---+----------------------+\n"
            + "|800|                    v8|\n"
            + "+---+----------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(showColumnsData, expected);
  }

  @Test
  public void testDeleteTSWithMultiTags() {
    if (!isAbleToDelete || isScaling) return;
    String showColumns = "SHOW COLUMNS ah.*;";
    String expected =
        "Columns:\n"
            + "+-----------------------+--------+\n"
            + "|                   Path|DataType|\n"
            + "+-----------------------+--------+\n"
            + "|              ah.hr01.s|    LONG|\n"
            + "|ah.hr01.s{t1=v1,t2=vv1}|    LONG|\n"
            + "|              ah.hr01.v|    LONG|\n"
            + "|ah.hr01.v{t1=v2,t2=vv1}|    LONG|\n"
            + "|              ah.hr02.s| BOOLEAN|\n"
            + "|       ah.hr02.s{t1=v1}| BOOLEAN|\n"
            + "|              ah.hr02.v|  BINARY|\n"
            + "|       ah.hr02.v{t1=v1}|  BINARY|\n"
            + "| ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
            + "|ah.hr03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
            + "|ah.hr03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
            + "|       ah.hr03.v{t1=v1}|    LONG|\n"
            + "|     ah.hr03.v{t1=vv11}|    LONG|\n"
            + "+-----------------------+--------+\n"
            + "Total line number = 13\n";
    executeAndCompare(showColumns, expected);

    String deleteTimeSeries = "DELETE COLUMNS ah.*.v WITH t1=v1 AND t2=v2;";
    execute(deleteTimeSeries);

    showColumns = "SHOW COLUMNS ah.*;";
    expected =
        "Columns:\n"
            + "+-----------------------+--------+\n"
            + "|                   Path|DataType|\n"
            + "+-----------------------+--------+\n"
            + "|              ah.hr01.s|    LONG|\n"
            + "|ah.hr01.s{t1=v1,t2=vv1}|    LONG|\n"
            + "|              ah.hr01.v|    LONG|\n"
            + "|ah.hr01.v{t1=v2,t2=vv1}|    LONG|\n"
            + "|              ah.hr02.s| BOOLEAN|\n"
            + "|       ah.hr02.s{t1=v1}| BOOLEAN|\n"
            + "|              ah.hr02.v|  BINARY|\n"
            + "|       ah.hr02.v{t1=v1}|  BINARY|\n"
            + "|ah.hr03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
            + "|ah.hr03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
            + "|       ah.hr03.v{t1=v1}|    LONG|\n"
            + "|     ah.hr03.v{t1=vv11}|    LONG|\n"
            + "+-----------------------+--------+\n"
            + "Total line number = 12\n";
    executeAndCompare(showColumns, expected);

    String showColumnsData = "SELECT v FROM ah.* WITH t1=v1 AND t2=v2;";
    expected = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    ;
    executeAndCompare(showColumnsData, expected);

    deleteTimeSeries = "DELETE COLUMNS * WITH t1=v1 AND t2=vv2 OR t1=vv1 AND t2=v2;";
    execute(deleteTimeSeries);

    showColumns = "SHOW COLUMNS ah.*;";
    expected =
        "Columns:\n"
            + "+-----------------------+--------+\n"
            + "|                   Path|DataType|\n"
            + "+-----------------------+--------+\n"
            + "|              ah.hr01.s|    LONG|\n"
            + "|ah.hr01.s{t1=v1,t2=vv1}|    LONG|\n"
            + "|              ah.hr01.v|    LONG|\n"
            + "|ah.hr01.v{t1=v2,t2=vv1}|    LONG|\n"
            + "|              ah.hr02.s| BOOLEAN|\n"
            + "|       ah.hr02.s{t1=v1}| BOOLEAN|\n"
            + "|              ah.hr02.v|  BINARY|\n"
            + "|       ah.hr02.v{t1=v1}|  BINARY|\n"
            + "|       ah.hr03.v{t1=v1}|    LONG|\n"
            + "|     ah.hr03.v{t1=vv11}|    LONG|\n"
            + "+-----------------------+--------+\n"
            + "Total line number = 10\n";
    executeAndCompare(showColumns, expected);

    showColumnsData = "SELECT * FROM * WITH t1=v1 AND t2=vv2 OR t1=vv1 AND t2=v2;";
    expected = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    ;
    executeAndCompare(showColumnsData, expected);
  }

  @Test
  public void testQueryWithWildcardTag() {
    String statement = "SELECT s FROM ah.* with t2=*;";
    String expected =
        "ResultSets:\n"
            + "+----+-----------------------+-----------------------+-----------------------+\n"
            + "| key|ah.hr01.s{t1=v1,t2=vv1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
            + "+----+-----------------------+-----------------------+-----------------------+\n"
            + "|   0|                      3|                   null|                   null|\n"
            + "|   1|                      4|                   null|                   null|\n"
            + "|   2|                      5|                   null|                   null|\n"
            + "|   3|                      6|                   null|                   null|\n"
            + "|1600|                   null|                   null|                   true|\n"
            + "|3200|                   null|                   true|                   null|\n"
            + "+----+-----------------------+-----------------------+-----------------------+\n"
            + "Total line number = 6\n";
    executeAndCompare(statement, expected);
  }

  @Test
  public void testQueryWithAggregate() {
    String statement = "SELECT sum(v) FROM ah.hr03 with t1=vv11;";
    String expected =
        "ResultSets:\n"
            + "+-----------------------+\n"
            + "|sum(ah.hr03.v{t1=vv11})|\n"
            + "+-----------------------+\n"
            + "|                     48|\n"
            + "+-----------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "SELECT max(v) FROM ah.hr03 with t1=vv11;";
    expected =
        "ResultSets:\n"
            + "+-----------------------+\n"
            + "|max(ah.hr03.v{t1=vv11})|\n"
            + "+-----------------------+\n"
            + "|                     32|\n"
            + "+-----------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "SELECT min(v) FROM ah.hr03 with t1=vv11;";
    expected =
        "ResultSets:\n"
            + "+-----------------------+\n"
            + "|min(ah.hr03.v{t1=vv11})|\n"
            + "+-----------------------+\n"
            + "|                     16|\n"
            + "+-----------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "SELECT avg(v) FROM ah.hr03 with t1=vv11;";
    expected =
        "ResultSets:\n"
            + "+-----------------------+\n"
            + "|avg(ah.hr03.v{t1=vv11})|\n"
            + "+-----------------------+\n"
            + "|                   24.0|\n"
            + "+-----------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "SELECT count(v) FROM ah.hr03 with t1=vv11;";
    expected =
        "ResultSets:\n"
            + "+-------------------------+\n"
            + "|count(ah.hr03.v{t1=vv11})|\n"
            + "+-------------------------+\n"
            + "|                        2|\n"
            + "+-------------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);
  }

  @Test
  public void testMixQueryWithAggregate() {
    String statement = "select last(s) from ah.hr01;";
    String expected =
        "ResultSets:\n"
            + "+---+-----------------------+-----+\n"
            + "|key|                   path|value|\n"
            + "+---+-----------------------+-----+\n"
            + "|  3|              ah.hr01.s|    4|\n"
            + "|  3|ah.hr01.s{t1=v1,t2=vv1}|    6|\n"
            + "+---+-----------------------+-----+\n"
            + "Total line number = 2\n";
    executeAndCompare(statement, expected);

    statement = "select last(v) from ah.hr01;";
    expected =
        "ResultSets:\n"
            + "+---+-----------------------+-----+\n"
            + "|key|                   path|value|\n"
            + "+---+-----------------------+-----+\n"
            + "|  3|              ah.hr01.v|    5|\n"
            + "|  3|ah.hr01.v{t1=v2,t2=vv1}|    7|\n"
            + "+---+-----------------------+-----+\n"
            + "Total line number = 2\n";
    executeAndCompare(statement, expected);

    statement = "select first(s) from ah.hr01;";
    expected =
        "ResultSets:\n"
            + "+---+-----------------------+-----+\n"
            + "|key|                   path|value|\n"
            + "+---+-----------------------+-----+\n"
            + "|  0|              ah.hr01.s|    1|\n"
            + "|  0|ah.hr01.s{t1=v1,t2=vv1}|    3|\n"
            + "+---+-----------------------+-----+\n"
            + "Total line number = 2\n";
    executeAndCompare(statement, expected);

    statement = "select first(v) from ah.hr01;";
    expected =
        "ResultSets:\n"
            + "+---+-----------------------+-----+\n"
            + "|key|                   path|value|\n"
            + "+---+-----------------------+-----+\n"
            + "|  0|              ah.hr01.v|    2|\n"
            + "|  0|ah.hr01.v{t1=v2,t2=vv1}|    4|\n"
            + "+---+-----------------------+-----+\n"
            + "Total line number = 2\n";
    executeAndCompare(statement, expected);

    statement = "select first(s), last(v) from ah.hr01;";
    expected =
        "ResultSets:\n"
            + "+---+-----------------------+-----+\n"
            + "|key|                   path|value|\n"
            + "+---+-----------------------+-----+\n"
            + "|  0|              ah.hr01.s|    1|\n"
            + "|  0|ah.hr01.s{t1=v1,t2=vv1}|    3|\n"
            + "|  3|              ah.hr01.v|    5|\n"
            + "|  3|ah.hr01.v{t1=v2,t2=vv1}|    7|\n"
            + "+---+-----------------------+-----+\n"
            + "Total line number = 4\n";
    executeAndCompare(statement, expected);

    statement = "select first(v), last(s) from ah.hr01;";
    expected =
        "ResultSets:\n"
            + "+---+-----------------------+-----+\n"
            + "|key|                   path|value|\n"
            + "+---+-----------------------+-----+\n"
            + "|  0|              ah.hr01.v|    2|\n"
            + "|  0|ah.hr01.v{t1=v2,t2=vv1}|    4|\n"
            + "|  3|              ah.hr01.s|    4|\n"
            + "|  3|ah.hr01.s{t1=v1,t2=vv1}|    6|\n"
            + "+---+-----------------------+-----+\n"
            + "Total line number = 4\n";
    executeAndCompare(statement, expected);

    statement = "select first(v), last(v) from ah.hr01;";
    expected =
        "ResultSets:\n"
            + "+---+-----------------------+-----+\n"
            + "|key|                   path|value|\n"
            + "+---+-----------------------+-----+\n"
            + "|  0|              ah.hr01.v|    2|\n"
            + "|  0|ah.hr01.v{t1=v2,t2=vv1}|    4|\n"
            + "|  3|              ah.hr01.v|    5|\n"
            + "|  3|ah.hr01.v{t1=v2,t2=vv1}|    7|\n"
            + "+---+-----------------------+-----+\n"
            + "Total line number = 4\n";
    executeAndCompare(statement, expected);

    statement = "select first(s), last(s) from ah.hr01;";
    expected =
        "ResultSets:\n"
            + "+---+-----------------------+-----+\n"
            + "|key|                   path|value|\n"
            + "+---+-----------------------+-----+\n"
            + "|  0|              ah.hr01.s|    1|\n"
            + "|  0|ah.hr01.s{t1=v1,t2=vv1}|    3|\n"
            + "|  3|              ah.hr01.s|    4|\n"
            + "|  3|ah.hr01.s{t1=v1,t2=vv1}|    6|\n"
            + "+---+-----------------------+-----+\n"
            + "Total line number = 4\n";
    executeAndCompare(statement, expected);

    statement = "select first_value(*) from ah.hr01;";
    expected =
        "ResultSets:\n"
            + "+----------------------+------------------------------------+----------------------+------------------------------------+\n"
            + "|first_value(ah.hr01.s)|first_value(ah.hr01.s{t1=v1,t2=vv1})|first_value(ah.hr01.v)|first_value(ah.hr01.v{t1=v2,t2=vv1})|\n"
            + "+----------------------+------------------------------------+----------------------+------------------------------------+\n"
            + "|                     1|                                   3|                     2|                                   4|\n"
            + "+----------------------+------------------------------------+----------------------+------------------------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "select last_value(*) from ah.hr01;";
    expected =
        "ResultSets:\n"
            + "+---------------------+-----------------------------------+---------------------+-----------------------------------+\n"
            + "|last_value(ah.hr01.s)|last_value(ah.hr01.s{t1=v1,t2=vv1})|last_value(ah.hr01.v)|last_value(ah.hr01.v{t1=v2,t2=vv1})|\n"
            + "+---------------------+-----------------------------------+---------------------+-----------------------------------+\n"
            + "|                    4|                                  6|                    5|                                  7|\n"
            + "+---------------------+-----------------------------------+---------------------+-----------------------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "select max(*) from ah.hr01;";
    expected =
        "ResultSets:\n"
            + "+--------------+----------------------------+--------------+----------------------------+\n"
            + "|max(ah.hr01.s)|max(ah.hr01.s{t1=v1,t2=vv1})|max(ah.hr01.v)|max(ah.hr01.v{t1=v2,t2=vv1})|\n"
            + "+--------------+----------------------------+--------------+----------------------------+\n"
            + "|             4|                           6|             5|                           7|\n"
            + "+--------------+----------------------------+--------------+----------------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "select min(*) from ah.hr01;";
    expected =
        "ResultSets:\n"
            + "+--------------+----------------------------+--------------+----------------------------+\n"
            + "|min(ah.hr01.s)|min(ah.hr01.s{t1=v1,t2=vv1})|min(ah.hr01.v)|min(ah.hr01.v{t1=v2,t2=vv1})|\n"
            + "+--------------+----------------------------+--------------+----------------------------+\n"
            + "|             1|                           3|             2|                           4|\n"
            + "+--------------+----------------------------+--------------+----------------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "select sum(*) from ah.hr01;";
    expected =
        "ResultSets:\n"
            + "+--------------+----------------------------+--------------+----------------------------+\n"
            + "|sum(ah.hr01.s)|sum(ah.hr01.s{t1=v1,t2=vv1})|sum(ah.hr01.v)|sum(ah.hr01.v{t1=v2,t2=vv1})|\n"
            + "+--------------+----------------------------+--------------+----------------------------+\n"
            + "|            10|                          18|            14|                          22|\n"
            + "+--------------+----------------------------+--------------+----------------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "select count(*) from ah.hr01;";
    expected =
        "ResultSets:\n"
            + "+----------------+------------------------------+----------------+------------------------------+\n"
            + "|count(ah.hr01.s)|count(ah.hr01.s{t1=v1,t2=vv1})|count(ah.hr01.v)|count(ah.hr01.v{t1=v2,t2=vv1})|\n"
            + "+----------------+------------------------------+----------------+------------------------------+\n"
            + "|               4|                             4|               4|                             4|\n"
            + "+----------------+------------------------------+----------------+------------------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "select avg(*) from ah.hr01;";
    expected =
        "ResultSets:\n"
            + "+--------------+----------------------------+--------------+----------------------------+\n"
            + "|avg(ah.hr01.s)|avg(ah.hr01.s{t1=v1,t2=vv1})|avg(ah.hr01.v)|avg(ah.hr01.v{t1=v2,t2=vv1})|\n"
            + "+--------------+----------------------------+--------------+----------------------------+\n"
            + "|           2.5|                         4.5|           3.5|                         5.5|\n"
            + "+--------------+----------------------------+--------------+----------------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "SELECT first(v) FROM ah.hr03;";
    expected =
        "ResultSets:\n"
            + "+----+------------------+-----+\n"
            + "| key|              path|value|\n"
            + "+----+------------------+-----+\n"
            + "|1600|ah.hr03.v{t1=vv11}|   16|\n"
            + "|3200|  ah.hr03.v{t1=v1}|   16|\n"
            + "+----+------------------+-----+\n"
            + "Total line number = 2\n";
    executeAndCompare(statement, expected);

    statement = "SELECT last(v) FROM ah.hr03;";
    expected =
        "ResultSets:\n"
            + "+----+------------------+-----+\n"
            + "| key|              path|value|\n"
            + "+----+------------------+-----+\n"
            + "|3200|  ah.hr03.v{t1=v1}|   16|\n"
            + "|3200|ah.hr03.v{t1=vv11}|   32|\n"
            + "+----+------------------+-----+\n"
            + "Total line number = 2\n";
    executeAndCompare(statement, expected);

    statement = "SELECT first_value(v) FROM ah.hr03;";
    expected =
        "ResultSets:\n"
            + "+-----------------------------+-------------------------------+\n"
            + "|first_value(ah.hr03.v{t1=v1})|first_value(ah.hr03.v{t1=vv11})|\n"
            + "+-----------------------------+-------------------------------+\n"
            + "|                           16|                             16|\n"
            + "+-----------------------------+-------------------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "SELECT last_value(v) FROM ah.hr03;";
    expected =
        "ResultSets:\n"
            + "+----------------------------+------------------------------+\n"
            + "|last_value(ah.hr03.v{t1=v1})|last_value(ah.hr03.v{t1=vv11})|\n"
            + "+----------------------------+------------------------------+\n"
            + "|                          16|                            32|\n"
            + "+----------------------------+------------------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "SELECT max(v) FROM ah.hr03;";
    expected =
        "ResultSets:\n"
            + "+---------------------+-----------------------+\n"
            + "|max(ah.hr03.v{t1=v1})|max(ah.hr03.v{t1=vv11})|\n"
            + "+---------------------+-----------------------+\n"
            + "|                   16|                     32|\n"
            + "+---------------------+-----------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "SELECT min(v) FROM ah.hr03;";
    expected =
        "ResultSets:\n"
            + "+---------------------+-----------------------+\n"
            + "|min(ah.hr03.v{t1=v1})|min(ah.hr03.v{t1=vv11})|\n"
            + "+---------------------+-----------------------+\n"
            + "|                   16|                     16|\n"
            + "+---------------------+-----------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "SELECT sum(v) FROM ah.hr03;";
    expected =
        "ResultSets:\n"
            + "+---------------------+-----------------------+\n"
            + "|sum(ah.hr03.v{t1=v1})|sum(ah.hr03.v{t1=vv11})|\n"
            + "+---------------------+-----------------------+\n"
            + "|                   16|                     48|\n"
            + "+---------------------+-----------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "SELECT count(v) FROM ah.hr03;";
    expected =
        "ResultSets:\n"
            + "+-----------------------+-------------------------+\n"
            + "|count(ah.hr03.v{t1=v1})|count(ah.hr03.v{t1=vv11})|\n"
            + "+-----------------------+-------------------------+\n"
            + "|                      1|                        2|\n"
            + "+-----------------------+-------------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "SELECT avg(v) FROM ah.hr03;";
    expected =
        "ResultSets:\n"
            + "+---------------------+-----------------------+\n"
            + "|avg(ah.hr03.v{t1=v1})|avg(ah.hr03.v{t1=vv11})|\n"
            + "+---------------------+-----------------------+\n"
            + "|                 16.0|                   24.0|\n"
            + "+---------------------+-----------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);
  }

  @Test
  public void testAlias() {
    String statement = "SELECT s AS ts FROM ah.hr02;";
    String expected =
        "ResultSets:\n"
            + "+---+----+---------+\n"
            + "|key|  ts|ts{t1=v1}|\n"
            + "+---+----+---------+\n"
            + "|100|true|     null|\n"
            + "|400|null|    false|\n"
            + "+---+----+---------+\n"
            + "Total line number = 2\n";
    executeAndCompare(statement, expected);

    statement = "SELECT s FROM ah.hr02 AS result_set;";
    expected =
        "ResultSets:\n"
            + "+---+------------+-------------------+\n"
            + "|key|result_set.s|result_set.s{t1=v1}|\n"
            + "+---+------------+-------------------+\n"
            + "|100|        true|               null|\n"
            + "|400|        null|              false|\n"
            + "+---+------------+-------------------+\n"
            + "Total line number = 2\n";
    executeAndCompare(statement, expected);

    statement = "SELECT s AS ts FROM ah.hr02 AS result_set;";
    expected =
        "ResultSets:\n"
            + "+---+----+---------+\n"
            + "|key|  ts|ts{t1=v1}|\n"
            + "+---+----+---------+\n"
            + "|100|true|     null|\n"
            + "|400|null|    false|\n"
            + "+---+----+---------+\n"
            + "Total line number = 2\n";
    executeAndCompare(statement, expected);

    statement = "SELECT * FROM (SELECT s AS ts FROM ah.hr02) AS result_set;";
    expected =
        "ResultSets:\n"
            + "+---+-------------+--------------------+\n"
            + "|key|result_set.ts|result_set.ts{t1=v1}|\n"
            + "+---+-------------+--------------------+\n"
            + "|100|         true|                null|\n"
            + "|400|         null|               false|\n"
            + "+---+-------------+--------------------+\n"
            + "Total line number = 2\n";
    executeAndCompare(statement, expected);
  }

  @Test
  public void testSubQuery() {
    String statement = "SELECT SUM(ts2) FROM (SELECT s AS ts1, v AS ts2 FROM ah.hr03 with t1=v1);";
    String expected =
        "ResultSets:\n"
            + "+---------------+\n"
            + "|sum(ts2{t1=v1})|\n"
            + "+---------------+\n"
            + "|             16|\n"
            + "+---------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "SELECT AVG(ts2) FROM (SELECT s AS ts1, v AS ts2 FROM ah.hr03 with t1=v1);";
    expected =
        "ResultSets:\n"
            + "+---------------+\n"
            + "|avg(ts2{t1=v1})|\n"
            + "+---------------+\n"
            + "|           16.0|\n"
            + "+---------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "SELECT MAX(ts2) FROM (SELECT s AS ts1, v AS ts2 FROM ah.hr03 with t1=v1);";
    expected =
        "ResultSets:\n"
            + "+---------------+\n"
            + "|max(ts2{t1=v1})|\n"
            + "+---------------+\n"
            + "|             16|\n"
            + "+---------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement = "SELECT COUNT(ts2) FROM (SELECT s AS ts1, v AS ts2 FROM ah.hr03 with t1=v1);";
    expected =
        "ResultSets:\n"
            + "+-----------------+\n"
            + "|count(ts2{t1=v1})|\n"
            + "+-----------------+\n"
            + "|                1|\n"
            + "+-----------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(statement, expected);

    statement =
        "SELECT ah.* FROM (SELECT * FROM ah.hr03 with t1=v1), (SELECT v FROM ah.hr02 with t1=v1);";
    expected =
        "ResultSets:\n"
            + "+-----------+----------------------+----------------+-----------+-----------------------+----------------+\n"
            + "|ah.hr02.key|ah.hr02.v{t1=v1,t2=v2}|ah.hr02.v{t1=v1}|ah.hr03.key|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.v{t1=v1}|\n"
            + "+-----------+----------------------+----------------+-----------+-----------------------+----------------+\n"
            + "|        400|                  null|              v4|       3200|                   true|              16|\n"
            + "|        800|                    v8|            null|       3200|                   true|              16|\n"
            + "+-----------+----------------------+----------------+-----------+-----------------------+----------------+\n"
            + "Total line number = 2\n";
    executeAndCompare(statement, expected);
  }

  @Test
  public void testTagInsertWithSubQuery() {

    String query = "SELECT s AS ts1, v AS ts2 FROM ah.hr03 with t1=v1;";
    String expected =
        "ResultSets:\n"
            + "+----+-----------------+----------+\n"
            + "| key|ts1{t1=v1,t2=vv2}|ts2{t1=v1}|\n"
            + "+----+-----------------+----------+\n"
            + "|3200|             true|        16|\n"
            + "+----+-----------------+----------+\n"
            + "Total line number = 1\n";
    executeAndCompare(query, expected);

    String insert =
        "INSERT INTO copy.ah.hr01(key, s, v) VALUES (SELECT s AS ts1, v AS ts2 FROM ah.hr03 with t1=v1);";
    execute(insert);

    query = "SELECT s, v FROM copy.ah.hr01;";
    expected =
        "ResultSets:\n"
            + "+----+----------------------------+---------------------+\n"
            + "| key|copy.ah.hr01.s{t1=v1,t2=vv2}|copy.ah.hr01.v{t1=v1}|\n"
            + "+----+----------------------------+---------------------+\n"
            + "|3200|                        true|                   16|\n"
            + "+----+----------------------------+---------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(query, expected);

    insert =
        "INSERT INTO copy.ah.hr02(key, s, v[t2=v2]) VALUES (SELECT s AS ts1, v AS ts2 FROM ah.hr03 with t1=v1);";
    execute(insert);

    query = "SELECT s, v FROM copy.ah.hr02;";
    expected =
        "ResultSets:\n"
            + "+----+----------------------------+---------------------------+\n"
            + "| key|copy.ah.hr02.s{t1=v1,t2=vv2}|copy.ah.hr02.v{t1=v1,t2=v2}|\n"
            + "+----+----------------------------+---------------------------+\n"
            + "|3200|                        true|                         16|\n"
            + "+----+----------------------------+---------------------------+\n"
            + "Total line number = 1\n";
    executeAndCompare(query, expected);
  }

  @Test
  public void testClearData() throws SessionException {
    if (!isAbleToClearData || isScaling) return;
    clearData();

    String countPoints = "COUNT POINTS;";
    String expected = "Points num: 0\n";
    executeAndCompare(countPoints, expected);

    String showColumns = "SELECT * FROM *;";
    expected = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    executeAndCompare(showColumns, expected);
  }
}
