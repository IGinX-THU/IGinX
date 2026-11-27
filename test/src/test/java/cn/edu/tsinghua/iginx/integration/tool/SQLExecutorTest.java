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
package cn.edu.tsinghua.iginx.integration.tool;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;
import org.junit.Test;

public class SQLExecutorTest {
  private SQLExecutor executor(boolean fail) {
    return new SQLExecutor(new MultiConnection((Session) null)) {
      @Override
      public String execute(String statement) {
        if (fail) throw new AssertionError("worker failure");
        return statement;
      }
    };
  }

  @Test(timeout = 5000)
  public void workerAssertionReachesTestThread() throws Exception {
    SQLExecutor executor = executor(true);
    try {
      try {
        executor.concurrentExecute(Arrays.asList("broken"));
        fail("Expected worker failure");
      } catch (AssertionError expected) {
        assertEquals("worker failure", expected.getCause().getMessage());
      }
      try {
        executor.concurrentExecuteAndCompare(Arrays.asList(new Pair<>("broken", "expected")));
        fail("Expected worker failure");
      } catch (AssertionError expected) {
        assertEquals("worker failure", expected.getCause().getMessage());
      }
    } finally {
      executor.close();
    }
  }

  @Test(timeout = 5000)
  public void moreStatementsThanPoolSizeComplete() throws Exception {
    SQLExecutor executor = executor(false);
    try {
      List<Pair<String, String>> statements = new ArrayList<>();
      for (int i = 0; i < 65; i++) statements.add(new Pair<>("query" + i, "query" + i));
      executor.concurrentExecuteAndCompare(statements);
    } finally {
      executor.close();
    }
  }
}
