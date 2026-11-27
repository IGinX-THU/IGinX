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
package cn.edu.tsinghua.iginx.filesystem.security;

import static cn.edu.tsinghua.iginx.filesystem.security.SecurityTestEnvironment.*;
import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.thrift.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.junit.Test;

public class InternalRequestIT {
  @Test(timeout = 180000)
  public void igt22_realPythonTasksCleanupAndCompaction() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment()) {
      SecurityTestEnvironment.Node node = env.node(0, secure("creator", "required"));
      node.start();
      Connection root = node.admin;
      node.user("indagent_data_internal", AuthType.Read, AuthType.Write);
      Connection service = node.connect("indagent_data_internal", "password");
      CodeTestFixture fixture = new CodeTestFixture(node);
      root.execute(fixture.create("identity"));
      root.execute("INSERT INTO ordinary (key,a) VALUES (1,11),(2,22);");
      for (DataFlowType flow : DataFlowType.values())
        for (boolean scheduled : Arrays.asList(false, true)) {
          String temporary = "intermediate" + flow.name().toLowerCase() + scheduled;
          Path exported = node.home.resolve(temporary + ".txt");
          List<TaskInfo> tasks =
              Arrays.asList(
                  new TaskInfo(TaskType.SQL, DataFlowType.STREAM)
                      .setSqlList(Arrays.asList("SELECT * FROM ordinary;")),
                  new TaskInfo(TaskType.PYTHON, flow)
                      .setPyTaskName("identity")
                      .setOutputPrefix(temporary),
                  new TaskInfo(TaskType.SQL, DataFlowType.STREAM)
                      .setSqlList(Arrays.asList("SELECT * FROM " + temporary + ";")));
          CommitTransformJobReq request =
              new CommitTransformJobReq(root.id, tasks, ExportType.FILE)
                  .setFileName(exported.toString());
          if (scheduled) request.setSchedule("after 1 second");
          long id = CodeTestFixture.submit(root, request);
          CodeTestFixture.await(root, id, JobState.JOB_FINISHED);
          String output = new String(Files.readAllBytes(exported), StandardCharsets.UTF_8);
          assertTrue(output, output.contains("11") && output.contains("22"));
          assertEquals(0, root.execute("SELECT * FROM " + temporary + ";").getPathsSize());
        }
      for (String sql :
          Arrays.asList(
              "SELECT * FROM indagent.t;", "INSERT INTO indagent.t (key,a) VALUES (9,9);")) {
        long id =
            CodeTestFixture.submit(
                root,
                new CommitTransformJobReq(
                    root.id,
                    Arrays.asList(
                        new TaskInfo(TaskType.SQL, DataFlowType.STREAM)
                            .setSqlList(Arrays.asList(sql, "SELECT * FROM ordinary;"))),
                    ExportType.LOG));
        long until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(20);
        while (CodeTestFixture.state(root, id) != JobState.JOB_FAILED && System.nanoTime() < until)
          Thread.sleep(50);
        assertEquals(JobState.JOB_FAILED, CodeTestFixture.state(root, id));
      }
      assertTrue(node.logs(), node.logs().toLowerCase().contains("reserved"));
      try (Connection raw = node.raw()) {
        denied(raw.client.executeSql(new ExecuteSqlReq(0, "SHOW COLUMNS;")).status);
      }
      for (java.lang.reflect.Field field : ExecuteSqlReq.class.getDeclaredFields())
        assertFalse(field.getName().toLowerCase().contains("internal"));
      service.execute("INSERT INTO indagent.t (key,a,b) VALUES (1,11,12),(2,21,22);");
      service.execute("INSERT INTO bigfile.t (key,a,b) VALUES (1,31,32),(2,41,42);");
      String before = service.execute("SELECT * FROM indagent.t, bigfile.t;").toString();
      root.execute("SET CONFIG \"enableInstantCompaction\" \"true\";");
      root.execute("COMPACT;");
      assertEquals(before, service.execute("SELECT * FROM indagent.t, bigfile.t;").toString());
    }
  }
}
