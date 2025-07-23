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
package cn.edu.tsinghua.iginx.integration.expansion.redis;

import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.redis;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedisCapacityExpansionIT.class);

  public RedisCapacityExpansionIT() {
    super(redis, (String) null, new RedisHistoryDataGenerator());
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    Constant.oriPort = dbConf.getDBCEPortMap().get(Constant.ORI_PORT_NAME);
    Constant.expPort = dbConf.getDBCEPortMap().get(Constant.EXP_PORT_NAME);
    Constant.readOnlyPort = dbConf.getDBCEPortMap().get(Constant.READ_ONLY_PORT_NAME);
  }

  @Override
  protected void testQuerySpecialHistoryData() {
    String statement = "select * from redis;";
    String expect =
        "ResultSets:\n"
            + "+---+--------------+----------------+-----------+---------+----------+\n"
            + "|key|redis.hash.key|redis.hash.value|  redis.key|redis.set|redis.zset|\n"
            + "+---+--------------+----------------+-----------+---------+----------+\n"
            + "|  0|        field1|          value1|redis value|   value1|    value0|\n"
            + "|  1|        field0|          value0|       null|   value0|    value1|\n"
            + "+---+--------------+----------------+-----------+---------+----------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
  }

  // redis中，所有dummy数据都识别为BINARY
  @Override
  protected void testShowColumnsInExpansion(boolean before) {
    String statement = "SHOW COLUMNS nt.wf03.*;";
    String expected =
        "Columns:\n"
            + "+--------------------+--------+\n"
            + "|                Path|DataType|\n"
            + "+--------------------+--------+\n"
            + "|nt.wf03.wt01.status2|  BINARY|\n"
            + "+--------------------+--------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expected);

    statement = "SHOW COLUMNS;";
    if (before) {
      expected =
          "Columns:\n"
              + "+--------------------------------------------------------------------------------------+--------+\n"
              + "|                                                                                  Path|DataType|\n"
              + "+--------------------------------------------------------------------------------------+--------+\n"
              + "|                                                                                 b.b.b|  BINARY|\n"
              + "|                                                                        ln.wf02.status| BOOLEAN|\n"
              + "|                                                                       ln.wf02.version|  BINARY|\n"
              + "|                                                                  nt.wf03.wt01.status2|  BINARY|\n"
              + "|                                                              nt.wf04.wt01.temperature|  BINARY|\n"
              + "|zzzzzzzzzzzzzzzzzzzzzzzzzzzz.zzzzzzzzzzzzzzzzzzzzzzzzzzz.zzzzzzzzzzzzzzzzzzzzzzzzzzzzz|  BINARY|\n"
              + "+--------------------------------------------------------------------------------------+--------+\n"
              + "Total line number = 6\n";
    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
      expected =
          "Columns:\n"
              + "+--------------------------------------------------------------------------------------+--------+\n"
              + "|                                                                                  Path|DataType|\n"
              + "+--------------------------------------------------------------------------------------+--------+\n"
              + "|                                                                                 b.b.b|  BINARY|\n"
              + "|                                                                        ln.wf02.status| BOOLEAN|\n"
              + "|                                                                       ln.wf02.version|  BINARY|\n"
              + "|                                                                  nt.wf03.wt01.status2|  BINARY|\n"
              + "|                                                              nt.wf04.wt01.temperature|  BINARY|\n"
              + "|                                                               p1.nt.wf03.wt01.status2|  BINARY|\n"
              + "|zzzzzzzzzzzzzzzzzzzzzzzzzzzz.zzzzzzzzzzzzzzzzzzzzzzzzzzz.zzzzzzzzzzzzzzzzzzzzzzzzzzzzz|  BINARY|\n"
              + "+--------------------------------------------------------------------------------------+--------+\n"
              + "Total line number = 7\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected, true);

    if (before) {
      statement = "SHOW COLUMNS p1.*;";
      expected =
          "Columns:\n"
              + "+----+--------+\n"
              + "|Path|DataType|\n"
              + "+----+--------+\n"
              + "+----+--------+\n"
              + "Empty set.\n";
    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
      statement = "SHOW COLUMNS p1.*;";
      expected =
          "Columns:\n"
              + "+-----------------------+--------+\n"
              + "|                   Path|DataType|\n"
              + "+-----------------------+--------+\n"
              + "|p1.nt.wf03.wt01.status2|  BINARY|\n"
              + "+-----------------------+--------+\n"
              + "Total line number = 1\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected);

    statement = "SHOW COLUMNS *.wf03.wt01.*;";
    if (before) {
      expected =
          "Columns:\n"
              + "+--------------------+--------+\n"
              + "|                Path|DataType|\n"
              + "+--------------------+--------+\n"
              + "|nt.wf03.wt01.status2|  BINARY|\n"
              + "+--------------------+--------+\n"
              + "Total line number = 1\n";
    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
      expected =
          "Columns:\n"
              + "+-----------------------+--------+\n"
              + "|                   Path|DataType|\n"
              + "+-----------------------+--------+\n"
              + "|   nt.wf03.wt01.status2|  BINARY|\n"
              + "|p1.nt.wf03.wt01.status2|  BINARY|\n"
              + "+-----------------------+--------+\n"
              + "Total line number = 2\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected, true);
  }

  // redis中，所有dummy数据都识别为BINARY
  @Override
  protected void testShowColumnsRemoveStorageEngine(boolean before) {
    String statement = "SHOW COLUMNS p1.*, p2.*, p3.*;";
    String expected;
    if (before) {
      expected =
          "Columns:\n"
              + "+---------------------------+--------+\n"
              + "|                       Path|DataType|\n"
              + "+---------------------------+--------+\n"
              + "|    p1.nt.wf03.wt01.status2|  BINARY|\n"
              + "|    p2.nt.wf03.wt01.status2|  BINARY|\n"
              + "|    p3.nt.wf03.wt01.status2|  BINARY|\n"
              + "|p3.nt.wf04.wt01.temperature|  BINARY|\n"
              + "+---------------------------+--------+\n"
              + "Total line number = 4\n";
    } else { // 移除schemaPrefix为p2及p3，dataPrefix为nt.wf03的数据源
      expected =
          "Columns:\n"
              + "+---------------------------+--------+\n"
              + "|                       Path|DataType|\n"
              + "+---------------------------+--------+\n"
              + "|    p1.nt.wf03.wt01.status2|  BINARY|\n"
              + "|p3.nt.wf04.wt01.temperature|  BINARY|\n"
              + "+---------------------------+--------+\n"
              + "Total line number = 2\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected, true);
  }

  // redis中，所有dummy数据都识别为BINARY
  @Override
  public void testShowColumns() {
    String statement = "SHOW COLUMNS mn.*;";
    String expected =
        "Columns:\n"
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|     mn.wf01.wt01.status|  BINARY|\n"
            + "|mn.wf01.wt01.temperature|  BINARY|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expected, true);

    statement = "SHOW COLUMNS nt.*;";
    expected =
        "Columns:\n"
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|    nt.wf03.wt01.status2|  BINARY|\n"
            + "|nt.wf04.wt01.temperature|  BINARY|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expected, true);

    statement = "SHOW COLUMNS tm.*;";
    expected =
        "Columns:\n"
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|     tm.wf05.wt01.status|  BINARY|\n"
            + "|tm.wf05.wt01.temperature|  BINARY|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expected, true);
  }

  // no param is allowed to be updated
  @Override
  protected void updateParams(int port) {}

  @Override
  protected void restoreParams(int port) {}

  @Override
  protected void shutdownDatabase(int port) {
    shutOrRestart(port, true, "redis", 30);
  }

  @Override
  protected void startDatabase(int port) {
    shutOrRestart(port, false, "redis", 30);
  }

  protected void testPathOverlappedDataNotOverlapped() throws SessionException {
    // before
    String statement = "select status from mn.wf01.wt01;";
    String expected =
        "ResultSets:\n"
            + "+---+-------------------+\n"
            + "|key|mn.wf01.wt01.status|\n"
            + "+---+-------------------+\n"
            + "|  0|           22222222|\n"
            + "|  1|           11111111|\n"
            + "+---+-------------------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expected);

    String insert =
        "insert into mn.wf01.wt01 (key, status) values (10, \"33333333\"), (100, \"44444444\");";
    session.executeSql(insert);

    // after
    statement = "select status from mn.wf01.wt01;";
    expected =
        "ResultSets:\n"
            + "+---+-------------------+\n"
            + "|key|mn.wf01.wt01.status|\n"
            + "+---+-------------------+\n"
            + "|  0|           22222222|\n"
            + "|  1|           11111111|\n"
            + "| 10|           33333333|\n"
            + "|100|           44444444|\n"
            + "+---+-------------------+\n"
            + "Total line number = 4\n";
    SQLTestTools.executeAndCompare(session, statement, expected);
  }
}
