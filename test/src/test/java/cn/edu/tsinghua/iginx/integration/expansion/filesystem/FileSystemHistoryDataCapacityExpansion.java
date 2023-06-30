package cn.edu.tsinghua.iginx.integration.expansion.filesystem;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.session.Session;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemHistoryDataCapacityExpansion {
  private static final Logger logger =
      LoggerFactory.getLogger(FileSystemHistoryDataCapacityExpansion.class);

  protected static Session session;
  protected static SessionPool sessionPool;
  protected String ENGINE_TYPE;

  @After
  public void clearData() throws ExecutionException, SessionException {
    Controller.clearData(session);
  }

  @BeforeClass
  public static void setUp() throws SessionException {
    session = new Session("127.0.0.1", 6888, "root", "root");
    sessionPool =
        new SessionPool.Builder()
            .host("127.0.0.1")
            .port(6888)
            .user("root")
            .password("root")
            .maxSize(3)
            .build();
    session.openSession();
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    session.closeSession();
    sessionPool.close();
  }

  @Test
  public void test() throws Exception {
    testInitialNodeInsertAndQuery();
    testCapacityExpansion();
    testWriteAndQueryAfterCapacityExpansion();
  }

  public void testInitialNodeInsertAndQuery() throws SessionException, ExecutionException {
    session.executeSql(
        "insert into file.us (key, cpu_usage, engine, desc) values (10, 12.1, 1, \"normal\");");
    session.executeSql(
        "insert into file.us (key, cpu_usage, engine, desc) values (11, 32.2, 2, \"normal\");");
    session.executeSql(
        "insert into file.us (key, cpu_usage, engine, desc) values (12, 66.8, 3, \"high\");");

    String statement = "select * from file";
    String expect =
        "ResultSets:\n"
            + "+---+-----------------+------------+--------------+\n"
            + "|key|file.us.cpu_usage|file.us.desc|file.us.engine|\n"
            + "+---+-----------------+------------+--------------+\n"
            + "| 10|             12.1|      normal|             1|\n"
            + "| 11|             32.2|      normal|             2|\n"
            + "| 12|             66.8|        high|             3|\n"
            + "+---+-----------------+------------+--------------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "count points";
    expect = "Points num: 9\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select count(*) from file.us";
    expect =
        "ResultSets:\n"
            + "+------------------------+-------------------+---------------------+\n"
            + "|count(file.us.cpu_usage)|count(file.us.desc)|count(file.us.engine)|\n"
            + "+------------------------+-------------------+---------------------+\n"
            + "|                       3|                  3|                    3|\n"
            + "+------------------------+-------------------+---------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  private void testCapacityExpansion() throws Exception {
    session.executeSql(
        "ADD STORAGEENGINE (\"127.0.0.1\", 6669, \"filesystem\", \"data_prefix:file, has_data:true, is_read_only:true\");");

    String statement = "select * from file.us";
    String expect =
        "ResultSets:\n"
            + "+---+-----------------+------------+--------------+\n"
            + "|key|file.us.cpu_usage|file.us.desc|file.us.engine|\n"
            + "+---+-----------------+------------+--------------+\n"
            + "| 10|             12.1|      normal|             1|\n"
            + "| 11|             32.2|      normal|             2|\n"
            + "| 12|             66.8|        high|             3|\n"
            + "+---+-----------------+------------+--------------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select * from file";
    expect =
        "ResultSets:\n"
            + "+---+--------------+-----------+-----------+-----------------+------------+--------------+\n"
            + "|key|file.cpu_usage|file.engine|file.status|file.us.cpu_usage|file.us.desc|file.us.engine|\n"
            + "+---+--------------+-----------+-----------+-----------------+------------+--------------+\n"
            + "|  0|           123|        456|        789|             null|        null|          null|\n"
            + "| 10|          null|       null|       null|             12.1|      normal|             1|\n"
            + "| 11|          null|       null|       null|             32.2|      normal|             2|\n"
            + "| 12|          null|       null|       null|             66.8|        high|             3|\n"
            + "+---+--------------+-----------+-----------+-----------------+------------+--------------+\n"
            + "Total line number = 4\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "count points";
    expect = "Points num: 12\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  private void testWriteAndQueryAfterCapacityExpansion() throws Exception {
    session.executeSql(
        "insert into file.us (key, cpu_usage, engine, desc) values (13, 88.8, 2, \"high\");");

    String statement = "select * from file";
    String expect =
        "ResultSets:\n"
            + "+---+--------------+-----------+-----------+-----------------+------------+--------------+\n"
            + "|key|file.cpu_usage|file.engine|file.status|file.us.cpu_usage|file.us.desc|file.us.engine|\n"
            + "+---+--------------+-----------+-----------+-----------------+------------+--------------+\n"
            + "|  0|           123|        456|        789|             null|        null|          null|\n"
            + "| 10|          null|       null|       null|             12.1|      normal|             1|\n"
            + "| 11|          null|       null|       null|             32.2|      normal|             2|\n"
            + "| 12|          null|       null|       null|             66.8|        high|             3|\n"
            + "| 13|          null|       null|       null|             88.8|        high|             2|\n"
            + "+---+--------------+-----------+-----------+-----------------+------------+--------------+\n"
            + "Total line number = 5\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "count points";
    expect = "Points num: 15\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    statement = "select count(*) from file.us";
    expect =
        "ResultSets:\n"
            + "+------------------------+-------------------+---------------------+\n"
            + "|count(file.us.cpu_usage)|count(file.us.desc)|count(file.us.engine)|\n"
            + "+------------------------+-------------------+---------------------+\n"
            + "|                       4|                  4|                    4|\n"
            + "+------------------------+-------------------+---------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
    SQLTestTools.executeAndCompare(session, statement, expect);
  }
}
