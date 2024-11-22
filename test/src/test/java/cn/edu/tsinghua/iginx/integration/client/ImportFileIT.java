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

    query = "SELECT * FROM t1;";
    expected =
        "ResultSets:\n"
            + "+---+------+----+----+------+\n"
            + "|key|t1._c_|t1.a|t1.b|t1.d_m|\n"
            + "+---+------+----+----+------+\n"
            + "|  0|  true| aaa| 0.5|   0.0|\n"
            + "|  1| false| bbb| 1.5|   1.0|\n"
            + "|  2|  true| ccc| 2.5|   2.0|\n"
            + "|  3| false| ddd| 3.5|   3.0|\n"
            + "|  4|  true| eee| 4.5|   4.0|\n"
            + "+---+------+----+----+------+\n"
            + "Total line number = 5\n";
    executor.executeAndCompare(query, expected);
  }
}
