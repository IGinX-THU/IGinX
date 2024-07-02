package cn.edu.tsinghua.iginx.integration.func.session;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.pool.IginxInfo;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLCompareIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(SQLCompareIT.class);

  protected static MultiConnection conn;
  protected static boolean isForSession = true;
  protected static boolean isForSessionPool = false;

  // host info
  protected static String defaultTestHost = "127.0.0.1";
  protected static int defaultTestPort = 6888;
  protected static String defaultTestUser = "root";
  protected static String defaultTestPass = "root";

  private final List<String> insertSQLGroup = new ArrayList<>();

  private final List<String> testSQLGroupA = new ArrayList<>();

  private final List<String> testSQLGroupB = new ArrayList<>();

  public SQLCompareIT() {
    readFile(
        insertSQLGroup,
        Paths.get("src", "test", "resources", "compare", "insertGroup.txt").toString());
    readFile(
        testSQLGroupA,
        Paths.get("src", "test", "resources", "compare", "testGroupA.txt").toString());
    readFile(
        testSQLGroupB,
        Paths.get("src", "test", "resources", "compare", "testGroupB.txt").toString());
  }

  private void readFile(List<String> testSQLGroup, String filename) {
    try {
      BufferedReader reader = new BufferedReader(new FileReader(filename));
      StringBuilder builder = new StringBuilder();

      String line = reader.readLine();
      while (line != null) {
        builder.append(line);
        line = reader.readLine();
      }
      reader.close();

      String fileContent = builder.toString();
      String[] sqls = fileContent.replaceAll("\\R", "").split(";");
      for (String sql : sqls) {
        sql = sql.trim() + ";";
        if (!sql.trim().equals("")) {
          testSQLGroup.add(sql);
        }
      }
    } catch (IOException e) {
      LOGGER.error("read file failed, filename: {}, cause: ", filename, e);
      fail();
    }
  }

  @BeforeClass
  public static void setUp() throws SessionException {
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
  public void insertData() throws SessionException {
    for (String insertSQL : insertSQLGroup) {
      SessionExecuteSqlResult res = conn.executeSql(insertSQL);
      if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
        LOGGER.error("Insert date execute fail. Caused by: {}.", res.getParseErrorMsg());
        fail();
      }
    }
  }

  @After
  public void clearData() throws SessionException {
    Controller.clearData(conn);
  }

  private void executeAndCompare(String sqlA, String sqlB) {
    String resultA = execute(sqlA);
    String resultB = execute(sqlB);
    assertEquals(resultA, resultB);
  }

  private String execute(String statement) {
    if (!statement.toLowerCase().startsWith("insert")) {
      LOGGER.info("Execute Statement: \"{}\"", statement);
    }

    SessionExecuteSqlResult res = null;
    try {
      res = conn.executeSql(statement);
    } catch (SessionException e) {
      LOGGER.error("Statement: \"{}\" execute fail. Caused by:", statement, e);
      fail();
    }

    if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      LOGGER.error(
          "Statement: \"{}\" execute fail. Caused by: {}.", statement, res.getParseErrorMsg());
      fail();
      return "";
    }

    return res.getResultInString(false, "");
  }

  @Test
  public void compareTest() {
    if (testSQLGroupA.size() != testSQLGroupB.size()) {
      LOGGER.error("two test groups' size are not equal.");
      fail();
    }

    for (int i = 0; i < testSQLGroupA.size(); i++) {
      String sqlA = testSQLGroupA.get(i);
      String sqlB = testSQLGroupB.get(i);
      executeAndCompare(sqlA, sqlB);
    }
  }
}
