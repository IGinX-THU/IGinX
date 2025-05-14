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
package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.integration.tool.SQLExecutor;
import cn.edu.tsinghua.iginx.session.Session;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class testPushdown {
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
  public void testPushDownSpeed() {
    // 测试谓词下推前后的速度

    // 先通过SHOW RULES命令，获取所有与谓词下推相关的命令，
    // 然后通过SET RULES命令，逐个打开，测试谓词下推前后的速度
    String showRules = "SHOW RULES;";
    String rules = executor.execute(showRules);
    List<String> pushDownRules =
        Arrays.stream(rules.split("\n"))
            .filter(s -> s.contains("FilterPush"))
            .map(s -> s.split("\\|")[1].trim())
            .collect(Collectors.toList());
    StringBuilder openRules = new StringBuilder("SET RULES ");
    StringBuilder closeRules = new StringBuilder("SET RULES ");
    for (String rule : pushDownRules) {
      openRules.append(rule).append("=on, ");
      closeRules.append(rule).append("=off, ");
    }
    openRules.delete(openRules.length() - 2, openRules.length());
    openRules.append(";");
    closeRules.delete(closeRules.length() - 2, closeRules.length());
    closeRules.append(";");

    // 插入数据
    StringBuilder insert = new StringBuilder();
    insert.append("INSERT INTO us (key, d1.s1, d1.s2, d2.s1, d2.s2, d3.s1, d3.s2) VALUES ");
    int rows = 50000;
    for (int i = 0; i < rows; i++) {
      insert.append(
          String.format(
              "(%d, %d, %d, %d, %d, %d, %d)",
              i, i % 100, i % 1000, i % 200, i % 2000, i % 300, i % 3000));
      if (i != rows - 1) {
        insert.append(",");
      }
    }
    insert.append(";");
    executor.execute(insert.toString());

    // 测试语句
    List<String> sqls = new ArrayList<>();
    //        sqls.add("SELECT * FROM us WHERE d1.s1 > 50 AND d1.s2 < 500;"); // 普通的下推
    //    //
    //        sqls.add("SELECT * FROM us.d1 JOIN us.d2 ON us.d1.s2 = us.d2.s2 AND us.d2.s1 <
    //   10;");
    // //
    // JOIN中的条件下推
    //        sqls.add("SELECT s2, MAX(s1) FROM us.d3 GROUP BY s2 HAVING MAX(s1) > 10 AND s2 <
    // 10;");
    //        sqls.add(
    //          "SELECT * FROM (SELECT s2 as col2, max(s1) as max_s1 FROM us.d3 GROUP BY s2) as a
    // WHERE max_s1 > 10 && col2 < 10;"); // 谓词下推过GROUP BY
    //        sqls.add(
    //            "SELECT * FROM us.d1 LEFT OUTER JOIN us.d2 ON us.d1.s1 = us.d2.s1 WHERE us.d2.s2 <
    // 1000;"); // LEFT OUTER 变 INNER
    //    sqls.add("SELECT * FROM us.d1, us.d2 WHERE us.d1.s1 = us.d2.s1;"); // CROSS变INNER
    List<Integer> openTime = new ArrayList<>();
    List<Integer> closeTime = new ArrayList<>();

    // 先关闭所有谓词下推
    executor.execute(closeRules.toString());
    int repeats = 50;

    //            for (String sql : sqls) {
    //              closeTime.add(0);
    //              for (int i = 0; i < repeats; i++) {
    //                long start = System.currentTimeMillis();
    //                executor.execute(sql);
    //                long end = System.currentTimeMillis();
    //                closeTime.set(
    //                    closeTime.size() - 1, closeTime.get(closeTime.size() - 1) + (int) (end -
    //     start));
    //              }
    //            }

    // 打开所有谓词下推
    executor.execute(openRules.toString());
    for (String sql : sqls) {
      openTime.add(0);
      for (int i = 0; i < repeats; i++) {
        long start = System.currentTimeMillis();
        executor.execute(sql);
        long end = System.currentTimeMillis();
        openTime.set(openTime.size() - 1, openTime.get(openTime.size() - 1) + (int) (end - start));
      }
    }

    for (int i = 0; i < sqls.size(); i++) {
      System.out.println(
          String.format(
              "SQL%d, CLOSE: %dms, OPEN: %dms",
              i, closeTime.get(i) / repeats, openTime.get(i) / repeats));
    }
  }

  //  @Test
  //  public void testAggPushDownSpeed() {
  //    // 插入数据，100000行，分别是test.a b c d,其中分别是1-10, 1-100, 1-1000, 1-10000的循环
  //    StringBuilder insert = new StringBuilder();
  //    insert.append("INSERT INTO test (key, a, b, c, d) VALUES ");
  //    int rows = 1000000;
  //    for (int i = 0; i < rows; i++) {
  //      insert.append(
  //          String.format(
  //              "(%d, %d, %d, %d, %d)", i + 1, i % 10 + 1, i % 100 + 1, i % 1000 + 1, i % 10000 +
  // 1));
  //      if (i != rows - 1) {
  //        insert.append(",");
  //      }
  //    }
  //    insert.append(";");
  //    executor.execute(insert.toString());
  //
  //    List<String> sql = new ArrayList<>();
  //    sql.add("SELECT sum(a) FROM test GROUP BY d;");
  //    sql.add("SELECT sum(a), min(b) FROM test GROUP BY d;");
  //    sql.add("SELECT sum(a), min(b), max(c) FROM test GROUP BY d;");
  //
  //    List<Integer> time = new ArrayList<>();
  //
  //    // 各重复10遍，记录各自时间
  //    for (String s : sql) {
  //      long start = System.currentTimeMillis();
  //      for (int i = 0; i < 100; i++) {
  //        executor.execute(s);
  //      }
  //      long end = System.currentTimeMillis();
  //      time.add((int) (end - start));
  //    }
  //
  //    for (int i = 0; i < 3; i++) {
  //      System.out.println(sql.get(i) + " : " + time.get(i));
  //    }
  //  }
}
