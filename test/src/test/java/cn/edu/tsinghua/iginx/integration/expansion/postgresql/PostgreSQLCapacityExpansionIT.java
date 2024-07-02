package cn.edu.tsinghua.iginx.integration.expansion.postgresql;

import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import java.sql.Connection;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgreSQLCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLCapacityExpansionIT.class);

  public PostgreSQLCapacityExpansionIT() {
    super(
        StorageEngineType.relational,
        "engine:postgresql, username:postgres, password:postgres",
        new PostgreSQLHistoryDataGenerator());
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    Constant.oriPort = dbConf.getDBCEPortMap().get(Constant.ORI_PORT_NAME);
    Constant.expPort = dbConf.getDBCEPortMap().get(Constant.EXP_PORT_NAME);
    Constant.readOnlyPort = dbConf.getDBCEPortMap().get(Constant.READ_ONLY_PORT_NAME);
    wrongExtraParams.add("username:wrong, password:postgres");
    // wrong password situation cannot be tested because trust mode is used
  }

  @Override
  protected void testQuerySpecialHistoryData() {
    testRepeatsQuery();
    testConcatFunction();
  }

  /** 执行一个简单查询1000次，测试是否会使连接池耗尽，来验证PG的dummy查询是否正确释放连接 */
  private void testRepeatsQuery() {
    String statement = "select * from ln;";
    String expect =
        "ResultSets:\n"
            + "+----+--------------+---------------+\n"
            + "| key|ln.wf02.status|ln.wf02.version|\n"
            + "+----+--------------+---------------+\n"
            + "| 100|          true|             v1|\n"
            + "| 400|         false|             v4|\n"
            + "| 800|          null|             v8|\n"
            + "|1600|          null|            v48|\n"
            + "+----+--------------+---------------+\n"
            + "Total line number = 4\n";

    int times = 1000;
    for (int i = 0; i < times; i++) {
      SQLTestTools.executeAndCompare(session, statement, expect);
    }
  }

  /** 创建一个1000列的表，进行查询，看concat函数是否会报错。 */
  protected void testConcatFunction() {
    int cols = 1000;
    // 用pg的session创建一个1000列的表
    String createTableStatement = "CREATE TABLE test_concat (";
    for (int i = 0; i < cols; i++) {
      createTableStatement += "col" + i + " text,";
    }
    createTableStatement = createTableStatement.substring(0, createTableStatement.length() - 1);
    createTableStatement += ");";

    StringBuilder insert1 = new StringBuilder();
    insert1.append("test_concat (");
    for (int i = 0; i < cols; i++) {
      insert1.append("col" + i + ",");
    }
    insert1.deleteCharAt(insert1.length() - 1);
    insert1.append(") ");
    StringBuilder insert2 = new StringBuilder();
    insert2.append("(");
    for (int i = 0; i < cols; i++) {
      insert2.append("'test',");
    }
    insert2.deleteCharAt(insert2.length() - 1);
    insert2.append(");");
    String insertStatement =
        String.format(PostgreSQLHistoryDataGenerator.INSERT_STATEMENT, insert1, insert2);

    try {
      Connection connection =
          PostgreSQLHistoryDataGenerator.connect(5432, true, null, "postgres", "postgres");
      Statement stmt = connection.createStatement();
      stmt.execute(
          String.format(PostgreSQLHistoryDataGenerator.CREATE_DATABASE_STATEMENT, "test_concat"));

      Connection testConnection =
          PostgreSQLHistoryDataGenerator.connect(
              5432, false, "test_concat", "postgres", "postgres");
      Statement testStmt = testConnection.createStatement();
      testStmt.execute(createTableStatement);
      testStmt.execute(insertStatement);

      // 不用插入数据直接查，不报错即可，不需要比较结果
      String query = "SELECT col1 FROM test_concat.*;";
      String expect =
          "ResultSets:\n"
              + "+---------+----------------------------+\n"
              + "|      key|test_concat.test_concat.col1|\n"
              + "+---------+----------------------------+\n"
              + "|262526998|                        test|\n"
              + "+---------+----------------------------+\n"
              + "Total line number = 1\n";
      SQLTestTools.executeAndCompare(session, query, expect);

      testConnection.close();
      stmt.execute(
          String.format(PostgreSQLHistoryDataGenerator.DROP_DATABASE_STATEMENT, "test_concat"));
      connection.close();
    } catch (Exception e) {
      LOGGER.error("create table failed", e);
      assert false;
    }
  }
}
