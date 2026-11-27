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
package cn.edu.tsinghua.iginx.integration.func.auth;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.util.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.junit.*;

/** Pins legacy behavior only when security.properties is absent. */
public class LegacyBehaviorIT {
  @BeforeClass
  public static void legacyOnly() {
    Assume.assumeFalse("true".equalsIgnoreCase(System.getenv("IGINX_TEST_SECURITY")));
  }

  @Test
  public void originalSessionAndSqlBehavior() throws Exception {
    try (TSocket socket = new TSocket("127.0.0.1", 6888, 15000)) {
      socket.open();
      IService.Client client = new IService.Client(new TBinaryProtocol(socket));
      long root =
          client
              .openSession(new OpenSessionReq().setUsername("root").setPassword("root"))
              .getSessionId();
      long read = 0;
      try {
        ok(
            client.addUser(
                new AddUserReq()
                    .setSessionId(root)
                    .setUsername("legacy_security_test")
                    .setPassword("password")
                    .setAuths(EnumSet.of(AuthType.Read))));
        read =
            client
                .openSession(
                    new OpenSessionReq()
                        .setUsername("legacy_security_test")
                        .setPassword("password"))
                .getSessionId();
        ok(
            client
                .executeSql(
                    new ExecuteSqlReq(
                        0, "INSERT INTO legacy_security_test (key, a) VALUES (1, 1), (2, 2);"))
                .getStatus());
        ok(
            client
                .executeSql(new ExecuteSqlReq(0, "SELECT * FROM legacy_security_test;"))
                .getStatus());
        for (String sql :
            Arrays.asList(
                "INSERT INTO indagent.legacy_security_test (key, a) VALUES (1, 3);",
                "SHOW CONFIG;",
                "SHOW SESSIONID;")) ok(client.executeSql(new ExecuteSqlReq(read, sql)).getStatus());
        ExecuteStatementResp query =
            client.executeStatement(
                new ExecuteStatementReq()
                    .setSessionId(read)
                    .setStatement("SELECT * FROM legacy_security_test;")
                    .setFetchSize(1));
        ok(query.getStatus());
        ok(
            client
                .fetchResults(
                    new FetchResultsReq()
                        .setSessionId(root)
                        .setQueryId(query.getQueryId())
                        .setFetchSize(10))
                .getStatus());
        ok(
            client.closeStatement(
                new CloseStatementReq().setSessionId(root).setQueryId(query.getQueryId())));
        ok(
            client.updateUser(
                new UpdateUserReq()
                    .setSessionId(root)
                    .setUsername("legacy_security_test")
                    .setPassword("changed")));
        ok(client.getReplicaNum(new GetReplicaNumReq(read)).getStatus());
      } finally {
        client.executeSql(
            new ExecuteSqlReq(
                root, "DELETE COLUMNS legacy_security_test.*, indagent.legacy_security_test.*;"));
        client.deleteUser(new DeleteUserReq(root, "legacy_security_test"));
        if (read != 0) client.closeSession(new CloseSessionReq(read));
        client.closeSession(new CloseSessionReq(root));
      }
    }
  }

  private static void ok(Status status) {
    assertEquals(status.toString(), RpcUtils.SUCCESS.code, status.code);
  }
}
