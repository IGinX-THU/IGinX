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
package cn.edu.tsinghua.iginx.integration.client;

import cn.edu.tsinghua.iginx.exception.SessionException;
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
            + "|  0|aaa|0.5| true|0.0|\n"
            + "|  1|bbb|1.5|false|1.0|\n"
            + "|  2|ccc|2.5| true|2.0|\n"
            + "|  3|ddd|3.5|false|3.0|\n"
            + "|  4|eee|4.5| true|4.0|\n"
            + "+---+---+---+-----+---+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);

    String query1 = "SELECT * FROM t1;";
    String expected1 =
        "ResultSets:\n"
            + "+---+------+----+----+------+\n"
            + "|key|t2._c_|t2.a|t2.b|t2.d_m|\n"
            + "+---+------+----+----+------+\n"
            + "| 10|  true| aaa| 0.5|   0.0|\n"
            + "| 11| false| bbb| 1.5|   1.0|\n"
            + "| 12|  true| ccc| 2.5|   2.0|\n"
            + "| 13| false| ddd| 3.5|   3.0|\n"
            + "| 14|  true| eee| 4.5|   4.0|\n"
            + "+---+------+----+----+------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);
  }
}
