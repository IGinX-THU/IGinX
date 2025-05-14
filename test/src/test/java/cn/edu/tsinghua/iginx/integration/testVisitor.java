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

import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.integration.tool.SQLExecutor;
import cn.edu.tsinghua.iginx.session.Session;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Test;

// 测试并行访问的时间耗时
public class testVisitor {
  protected List<SQLExecutor> executors = new ArrayList<>();
  //  protected static SQLExecutor executor;
  //
  //  @BeforeClass
  //  public static void setUp() throws SessionException {
  //    MultiConnection session = new MultiConnection(new Session("127.0.0.1", 6888, "root",
  // "root"));
  //    executor = new SQLExecutor(session);
  //    executor.open();
  //  }
  //
  //  @AfterClass
  //  public static void tearDown() throws SessionException {
  //    executor.close();
  //  }

  //  @After
  //  public void clearData() {
  //    //        String clearData = "CLEAR DATA;";
  //    //        executor.execute(clearData);
  //    for (SQLExecutor executor : executors) {
  //      try {
  //        executor.close();
  //      } catch (SessionException e) {
  //        throw new RuntimeException(e);
  //      }
  //    }
  //  }

  @Test
  public void testVisitorParallel() {
    //    // 插入数据
    //    StringBuilder insert = new StringBuilder();
    //    insert.append("INSERT INTO us (key, d1.s1, d1.s2, d2.s1, d2.s2, d3.s1, d3.s2) VALUES ");
    //    int rows = 50000;
    //    for (int i = 0; i < rows; i++) {
    //      insert.append(
    //          String.format(
    //              "(%d, %d, %d, %d, %d, %d, %d)",
    //              i, i % 100, i % 1000, i % 200, i % 2000, i % 300, i % 3000));
    //      if (i != rows - 1) {
    //        insert.append(",");
    //      }
    //    }
    //    insert.append(";");
    //    executor.execute(insert.toString());

    int parallelism = 100; // 并发线程数
    int repeats = 1; // 每个线程重复次数
    List<String> sqls =
        Arrays.asList(
            "SELECT * FROM us WHERE d1.s1 > 50 AND d1.s2 < 500;",
            "SELECT * FROM us WHERE d2.s1 > 50 AND d2.s2 < 500;",
            "SELECT * FROM us WHERE d3.s1 > 50 AND d3.s2 < 500;"
            // 你可以添加其他 SQL
            );

    ExecutorService executorService = Executors.newFixedThreadPool(parallelism);

    List<Future<Long>> futures = new ArrayList<>();
    for (int j = 0; j < repeats; j++) {
      for (int i = 0; i < parallelism; i++) {
        futures.add(
            executorService.submit(
                () -> {
                  Session session = new Session("127.0.0.1", 6888, "root", "root");
                  MultiConnection conn = new MultiConnection(session);
                  SQLExecutor exec = new SQLExecutor(conn);
                  executors.add(exec);
                  long total = 0;
                  try {
                    exec.open();
                    for (String sql : sqls) {
                      long start = System.currentTimeMillis();
                      exec.execute(sql);
                      total += System.currentTimeMillis() - start;
                    }
                  } finally {
                    //                        exec.close();
                  }
                  return total;
                }));
      }
    }
    // 等待所有任务完成并统计平均时间
    long totalTime = 0;
    for (Future<Long> future : futures) {
      try {
        totalTime += future.get();
      } catch (java.util.concurrent.ExecutionException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    System.out.println(
        String.format("Parallel Avg Time: %dms", totalTime / (parallelism * repeats)));

    executorService.shutdown();
  }
}
