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
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/** IGT-30 runs separately for each metadata backend; no timing threshold is configurable. */
public class SessionRevokeAcceptanceIT {
  static void backend(
      SecurityTestEnvironment env, SecurityTestEnvironment.Node node, EtcdTestServer etcd) {
    if (etcd != null) {
      node.config.setProperty("metaStorage", "etcd");
      node.config.setProperty("etcdEndpoints", etcd.endpoint);
    }
  }

  static void relogin(Connection c, String user, String password) throws Exception {
    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    OpenSessionResp r;
    do {
      r = c.client.openSession(new OpenSessionReq().setUsername(user).setPassword(password));
      if (r.status.code == 200) {
        c.id = r.sessionId;
        return;
      }
      Thread.sleep(20);
    } while (System.nanoTime() < end);
    ok(r.status);
  }

  @Test
  public void igt32_foreignNodeSessionRejected() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment()) {
      for (int i = 0; i < 2; i++) {
        SecurityTestEnvironment.Node n = env.node(0, secure("creator", "required"));
        n.start();
      }
      Connection foreign = env.nodes.get(1).raw();
      denied(
          foreign.client.executeSql(
                  new ExecuteSqlReq(env.nodes.get(0).admin.id, "SHOW REPLICA NUMBER;"))
              .status);
      env.nodes.get(0).admin.execute("SHOW REPLICA NUMBER;");
    }
  }
}
