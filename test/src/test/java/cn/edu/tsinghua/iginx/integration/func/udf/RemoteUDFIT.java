package cn.edu.tsinghua.iginx.integration.func.udf;

import static cn.edu.tsinghua.iginx.integration.controller.Controller.clearAllData;
import static org.junit.Assert.assertTrue;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.integration.func.session.InsertAPIType;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteUDFIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteUDFIT.class);

  private static final String DROP_SQL = "DROP FUNCTION \"%s\";";

  private static UDFTestTools tool;
  private static Session session;
  private static boolean dummyNoData = true;
  private static List<String> taskToBeRemoved;

  @BeforeClass
  public static void setUp() throws SessionException {
    session = new Session("127.0.0.1", 6888, "root", "root");
    session.openSession();
    tool = new UDFTestTools(session);
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    clearAllData(session);
    session.closeSession();
  }

  @Before
  public void insertData() {
    long startKey = 0L;
    long endKey = 15000L;
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
    int size = (int) (endKey - startKey);
    for (int i = 0; i < size; i++) {
      keyList.add(startKey + i);
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

  @Before
  public void resetTaskToBeDropped() {
    if (taskToBeRemoved == null) {
      taskToBeRemoved = new ArrayList<>();
    } else {
      taskToBeRemoved.clear();
    }
  }

  // drop unwanted UDFs no matter what
  @After
  public void dropTasks() {
    for (String name : taskToBeRemoved) {
      tool.execute(String.format(DROP_SQL, name));
    }
    taskToBeRemoved.clear();
  }

  // before this test, remote UDFs have been registered in test_remote_udf.sh
  // after this test, we expect all functions are deleted
  @Test
  public void remoteRegisterTest() {
    List<String> udfList =
        Arrays.asList(
            "mock_udf", "udf_a", "udf_b", "udf_sub", "udf_a_file", "udf_b_file", "udf_c_file");
    assertTrue(tool.isUDFsRegistered(udfList));
    taskToBeRemoved.addAll(udfList);

    String sql = "insert into test(key,a) values(0,1), (1,2);", expect;
    tool.execute(sql);

    // test each usage
    // these UDFs are registered in 3 groups, for efficiency we only test one in each group.
    sql = "select mock_udf(a,1) from test;";
    expect =
        "ResultSets:\n"
            + "+---+---+\n"
            + "|key|col|\n"
            + "+---+---+\n"
            + "|  0|  1|\n"
            + "|  1|  1|\n"
            + "+---+---+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, sql, expect);
    sql = "select udf_b(a,1) from test;";
    expect =
        "ResultSets:\n"
            + "+---+-----------+\n"
            + "|key|col_outer_b|\n"
            + "+---+-----------+\n"
            + "|  0|          1|\n"
            + "|  1|          1|\n"
            + "+---+-----------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, sql, expect);
    sql = "select udf_b_file(a,1) from test;";
    expect =
        "ResultSets:\n"
            + "+-----------+\n"
            + "|col_outer_b|\n"
            + "+-----------+\n"
            + "|          1|\n"
            + "+-----------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, sql, expect);
  }
}
