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
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.Test;

public class CodeRegistrationIT {
  @Test(timeout = 90000)
  public void igt16_deniedDirectoryRegistrationDoesNotInvokePip() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment()) {
      SecurityTestEnvironment.Node node = env.node(0, secure("creator", "required"));
      java.nio.file.Path marker = node.home.resolve("pip.log");
      node.environment.put("PIP_LOG", marker.toString());
      node.environment.put("PIP_NO_INDEX", "1");
      node.environment.put("PIP_DISABLE_PIP_VERSION_CHECK", "1");
      java.nio.file.Path module =
          java.nio.file.Files.createDirectories(node.home.resolve("pip_fixture"));
      java.nio.file.Files.write(module.resolve("identity.py"), CodeTestFixture.PYTHON);
      java.nio.file.Files.write(module.resolve("requirements.txt"), new byte[0]);
      node.start();
      node.user("reader", AuthType.Read);
      node.user("writer", AuthType.Read, AuthType.Write);
      node.user("administrator", AuthType.Admin);
      String sql =
          "CREATE FUNCTION TRANSFORM \"pip_fixture\" FROM \"identity.Identity\" IN \""
              + module.toString().replace('\\', '/')
              + "\";";
      for (String user : Arrays.asList("reader", "writer"))
        try (Connection c = node.connect(user, "password")) {
          Map<String, String> files = CodeTestFixture.files(node.home.resolve("udf"));
          String before = SqlSecurityAcceptanceIT.snapshot(node.admin);
          denied(c.sql(sql).status);
          denied(
              c.client.registerTask(
                  new RegisterTaskReq(
                      c.id,
                      module.toString(),
                      Arrays.asList(new UDFClassPair("pip_fixture", "identity.Identity")),
                      Arrays.asList(UDFType.TRANSFORM),
                      ByteBuffer.allocate(0),
                      false)));
          assertFalse("Rejected registration invoked pip", java.nio.file.Files.exists(marker));
          assertEquals(files, CodeTestFixture.files(node.home.resolve("udf")));
          assertEquals(before, SqlSecurityAcceptanceIT.snapshot(node.admin));
        }
      // A real, offline pip invocation proves the marker can observe the side effect.
      try (Connection admin = node.connect("administrator", "password")) {
        // Installation can exceed the normal RPC timeout on a cold CI runner.
        admin.socket.setTimeout(60000);
        admin.execute(sql);
        assertTrue("Positive control did not invoke pip", java.nio.file.Files.size(marker) > 0);
        admin.execute("DROP FUNCTION \"pip_fixture\";");
      }
    }
  }

  @Test(timeout = 120000)
  public void igt16_registrationAndJobPermissionsThroughSqlAndRpc() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment()) {
      SecurityTestEnvironment.Node node = env.node(0, secure("creator", "required"));
      node.start();
      node.user("read", AuthType.Read);
      node.user("ordinary", AuthType.Read, AuthType.Write);
      node.user("admin", AuthType.Admin);
      CodeTestFixture fixture = new CodeTestFixture(node);
      Connection root = node.admin;
      Connection reader = node.connect("read", "password"),
          ordinary = node.connect("ordinary", "password"),
          admin = node.connect("admin", "password");
      long job = CodeTestFixture.submit(root, fixture.scheduled(root.id));
      CodeTestFixture.await(root, job, JobState.JOB_IDLE);
      root.execute(fixture.create("existing"));
      for (Connection c : Arrays.asList(reader, ordinary, admin, root)) {
        boolean canRead = c != admin;
        for (String sql :
            Arrays.asList(
                "SHOW FUNCTIONS;",
                "SHOW TRANSFORM JOB STATUS " + job + ";",
                "SHOW TRANSFORM JOBS;")) {
          Status s = c.sql(sql).status;
          if (canRead) ok(s);
          else denied(s);
        }
        for (Status s :
            Arrays.asList(
                c.client.getRegisterTaskInfo(new GetRegisterTaskInfoReq(c.id)).status,
                c.client.queryTransformJobStatus(new QueryTransformJobStatusReq(c.id, job)).status,
                c.client.showEligibleJob(new ShowEligibleJobReq(c.id)).status)) {
          if (canRead) ok(s);
          else denied(s);
        }
      }
      for (Connection c : Arrays.asList(reader, ordinary)) {
        String before = SqlSecurityAcceptanceIT.snapshot(root);
        Map<String, String> files = CodeTestFixture.files(node.home.resolve("udf"));
        for (boolean remote : Arrays.asList(false, true)) {
          denied(c.sql(fixture.create("denied")).status);
          denied(
              c.client.loadUDF(
                      new LoadUDFReq(c.id, fixture.create("denied"), remote)
                          .setUdfFile(ByteBuffer.wrap(fixture.archive)))
                  .status);
          denied(c.client.registerTask(fixture.register(c.id, "denied", remote)));
        }
        denied(c.sql("DROP FUNCTION \"existing\";").status);
        denied(c.client.dropTask(new DropTaskReq(c.id, "existing")));
        denied(
            c.sql("COMMIT TRANSFORM JOB \"" + fixture.yaml.toString().replace('\\', '/') + "\";")
                .status);
        denied(c.client.commitTransformJob(fixture.scheduled(c.id)).status);
        denied(c.sql("CANCEL TRANSFORM JOB " + job + ";").status);
        denied(c.client.cancelTransformJob(new CancelTransformJobReq(c.id, job)));
        assertEquals(before, SqlSecurityAcceptanceIT.snapshot(root));
        assertEquals(files, CodeTestFixture.files(node.home.resolve("udf")));
        assertEquals(JobState.JOB_IDLE, CodeTestFixture.state(root, job));
      }
      root.execute("DROP FUNCTION \"existing\";");
      for (Connection c : Arrays.asList(admin, root)) {
        for (boolean remote : Arrays.asList(false, true)) {
          ok(c.client.registerTask(fixture.register(c.id, "allowed", remote)));
          ok(c.client.dropTask(new DropTaskReq(c.id, "allowed")));
          ok(
              c.client.loadUDF(
                      new LoadUDFReq(c.id, fixture.create("allowed"), remote)
                          .setUdfFile(ByteBuffer.wrap(fixture.archive)))
                  .status);
          c.execute("DROP FUNCTION \"allowed\";");
        }
        c.execute(fixture.create("allowed"));
        c.execute("DROP FUNCTION \"allowed\";");
        long id = CodeTestFixture.submit(c, fixture.scheduled(c.id));
        CodeTestFixture.await(root, id, JobState.JOB_IDLE);
        ok(c.client.cancelTransformJob(new CancelTransformJobReq(c.id, id)));
        ExecuteSqlResp submitted =
            c.execute(
                "COMMIT TRANSFORM JOB \"" + fixture.yaml.toString().replace('\\', '/') + "\";");
        assertEquals(
            fixture.yaml.toString().replace('\\', '/'), submitted.jobYamlPath.replace('\\', '/'));
        // SQL COMMIT returns a client-side YAML path; the client submits the parsed request.
        long sqlJob = CodeTestFixture.submit(c, fixture.scheduled(c.id));
        CodeTestFixture.await(root, sqlJob, JobState.JOB_IDLE);
        c.execute("CANCEL TRANSFORM JOB " + sqlJob + ";");
      }
      ok(root.client.cancelTransformJob(new CancelTransformJobReq(root.id, job)));
    }
  }
}
