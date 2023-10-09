package cn.edu.tsinghua.iginx.integration.client;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.integration.tool.SQLExecutor;
import cn.edu.tsinghua.iginx.session.Session;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ImportFileIT {

  protected static SQLExecutor executor;

  @BeforeClass
  public static void setUp() throws SessionException {
    MultiConnection session = new MultiConnection(new Session("127.0.0.1", 6888, "root", "root"));
    ;
    executor = new SQLExecutor(session);
    executor.open();
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    executor.close();
  }

  @After
  public void clearData() {
    String clearData = "CLEAR DATA;";
    executor.execute(clearData);
  }

  @Test
  public void testLoadData() {
    String query = "SELECT * FROM t;";
    String expected =
        "ResultSets:\n"
            + "+---+---+---+-----+---+\n"
            + "|key|t.a|t.b|  t.c|t.d|\n"
            + "+---+---+---+-----+---+\n"
            + "|  0|  0|0.5| true|aaa|\n"
            + "|  1|  1|1.5|false|bbb|\n"
            + "|  2|  2|2.5| true|ccc|\n"
            + "|  3|  3|3.5|false|ddd|\n"
            + "|  4|  4|4.5| true|eee|\n"
            + "+---+---+---+-----+---+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);
  }
}
