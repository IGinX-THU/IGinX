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

  @Test(timeout = 240000)
  public void igt30_twentyRoundsAllChangesAllNodes() throws Exception {
    String backend = System.getProperty("iginx.security.backend", "zookeeper");
    try (SecurityTestEnvironment env = new SecurityTestEnvironment()) {
      EtcdTestServer etcd = backend.equals("etcd") ? new EtcdTestServer(env.directory) : null;
      if (etcd != null) env.resources.add(etcd);
      for (int i = 0; i < 3; i++) {
        SecurityTestEnvironment.Node n = env.node(0, secure("creator", "required"));
        backend(env, n, etcd);
        n.start();
        // Initialize the query fixture before nodes join. Revocation measurements start only
        // after all three nodes see the same data and permissions.
        if (i == 0) n.admin.execute("INSERT INTO revoke (key,a) VALUES (1,11),(2,22),(3,33);");
      }
      ClusterTestFixture.waitForCluster(env.nodes.get(0), 3, 3);
      for (SecurityTestEnvironment.Node n : env.nodes) n.admin.execute("SHOW COLUMNS;");
      Connection root = env.nodes.get(0).admin;
      env.nodes.get(0).user("u", AuthType.Read, AuthType.Write);
      env.nodes.get(0).user("v", AuthType.Read);
      Connection[][] u = new Connection[3][2];
      Connection[] v = new Connection[3];
      for (int n = 0; n < 3; n++) {
        for (int s = 0; s < 2; s++) {
          u[n][s] = env.nodes.get(n).raw();
          relogin(u[n][s], "u", "password");
        }
        v[n] = env.nodes.get(n).raw();
        relogin(v[n], "v", "password");
      }
      // Thrift starts its minimum workers lazily and does not retire core workers.
      // Fill that configured pool before the baseline, while holding the connections
      // together so one worker cannot serve the entire warmup. Keep all retirement
      // and growth assertions below: late core-worker creation is not a query leak.
      for (SecurityTestEnvironment.Node node : env.nodes) {
        List<Connection> warmup = new ArrayList<>();
        try {
          int minimum = Integer.parseInt(node.config.getProperty("minThriftWorkerThreadNum", "20"));
          for (int i = 0; i < minimum; i++) {
            Connection connection = node.raw();
            warmup.add(connection);
            relogin(connection, "v", "password");
          }
        } finally {
          for (Connection connection : warmup) {
            try {
              ok(connection.client.closeSession(new CloseSessionReq(connection.id)));
            } finally {
              connection.close();
            }
          }
        }
      }
      List<String> samples = new ArrayList<>();
      samples.add("backend,round,change,node,latency_ms");
      int[] threads = new int[3];
      List<Set<String>> baselineWorkers = new ArrayList<>();
      long maximum = 0;
      String password = "password";
      // Warm all lazy worker pools before measuring thread growth; still measure 20 full rounds.
      for (int round = -5; round < 20; round++) {
        for (String change : Arrays.asList("GRANT", "ALTER", "DROP")) {
          for (SecurityTestEnvironment.Node n : env.nodes) {
            long ready = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!n.admin
                .client
                .getUser(new GetUserReq(n.admin.id).setUsernames(Arrays.asList("u")))
                .auths
                .equals(Arrays.asList(EnumSet.of(AuthType.Read, AuthType.Write)))) {
              assertTrue("Restored permissions did not propagate", System.nanoTime() < ready);
              Thread.sleep(20);
            }
          }
          // Reopen sessions on the same transports to keep the server connection count constant.
          for (int n = 0; n < 3; n++) for (int s = 0; s < 2; s++) relogin(u[n][s], "u", password);
          ExecuteStatementResp cursor =
              u[2][0].client.executeStatement(
                  new ExecuteStatementReq(u[2][0].id, "SELECT * FROM revoke;").setFetchSize(1));
          ok(cursor.status);
          assertEquals(1, cursor.queryDataSet.getValuesListSize());
          if (round == 0 && change.equals("GRANT"))
            for (int n = 0; n < 3; n++) {
              Set<String> names = env.nodes.get(n).workerThreadNames();
              threads[n] = names.size();
              baselineWorkers.add(names);
              Files.write(
                  env.directory.resolve("threads-before-" + n + ".txt"),
                  names,
                  StandardCharsets.UTF_8);
            }
          long start = System.nanoTime();
          if (change.equals("GRANT")) root.execute("GRANT READ TO USER u;");
          else if (change.equals("ALTER")) {
            password = "round" + (round + 5);
            root.execute("ALTER USER u IDENTIFIED BY " + password + ";");
          } else root.execute("DROP USER u;");
          long[] delays = {-1, -1, -1};
          while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5)) {
            boolean done = true;
            for (int n = 0; n < 3; n++) {
              boolean revoked = true;
              for (Connection c : u[n])
                if (c.sql("SELECT * FROM revoke;").status.code != 101) revoked = false;
              if (revoked && delays[n] < 0)
                delays[n] = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
              if (!revoked) done = false;
              v[n].execute("SELECT * FROM revoke;");
            }
            if (done) break;
            Thread.sleep(100);
          }
          for (int n = 0; n < 3; n++) {
            assertTrue(
                backend + " " + round + " " + change + " node=" + n,
                delays[n] >= 0 && delays[n] <= 5000);
            if (round >= 0) {
              maximum = Math.max(maximum, delays[n]);
              samples.add(backend + "," + round + "," + change + "," + n + "," + delays[n]);
            }
          }
          long fetchStart = System.nanoTime();
          denied(
              u[2][0].client.fetchResults(
                      new FetchResultsReq(u[2][0].id, cursor.queryId).setFetchSize(1))
                  .status);
          assertTrue(System.nanoTime() - fetchStart < TimeUnit.SECONDS.toNanos(5));
          if (change.equals("DROP")) {
            env.nodes.get(0).user("u", AuthType.Read, AuthType.Write);
            password = "password";
          } else {
            relogin(u[0][0], "u", password);
            u[0][0].execute("SELECT * FROM revoke;");
            if (change.equals("GRANT"))
              denied(u[0][0].sql("INSERT INTO denied (key,a) VALUES (1,1);").status);
            else u[0][0].execute("INSERT INTO ordinary (key,a) VALUES (1,1);");
            root.execute("GRANT READ, WRITE TO USER u;");
          }
        }
      }
      Files.write(
          env.directory.resolve("igt30-" + backend + "-latency.csv"),
          samples,
          StandardCharsets.UTF_8);
      System.out.println(
          "IGT-30 "
              + backend
              + " 20 rounds, 180 node samples, maximum="
              + maximum
              + "ms; "
              + env.directory);
      // Cached executor workers retire after 60 seconds of idleness. Wait for retirement,
      // then require every worker created during measurement to be gone, not just a stable total.
      long retirementDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(70);
      List<Set<String>> remaining = new ArrayList<>();
      do {
        remaining.clear();
        for (int n = 0; n < 3; n++) {
          Set<String> added = env.nodes.get(n).workerThreadNames();
          added.removeAll(baselineWorkers.get(n));
          remaining.add(added);
        }
        if (remaining.stream().allMatch(Set::isEmpty)) break;
        Thread.sleep(500);
      } while (System.nanoTime() < retirementDeadline);
      for (int n = 0; n < 3; n++) {
        Files.write(
            env.directory.resolve("threads-after-" + n + ".txt"),
            env.nodes.get(n).workerThreadNames(),
            StandardCharsets.UTF_8);
        int actual = env.nodes.get(n).workerThreads();
        assertTrue(
            "Worker growth on node " + n + ": " + threads[n] + " -> " + actual,
            actual <= threads[n]);
        assertTrue(
            "Unreleased workers on node " + n + ": " + remaining.get(n),
            remaining.get(n).isEmpty());
      }
    }
  }

  @Test(timeout = 90000)
  public void igt31_revokeAfterTwentySecondDisconnect() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment()) {
      SecurityTcpProxy proxy = new SecurityTcpProxy(env.factory.getLocalPort());
      env.resources.add(proxy);
      for (int i = 0; i < 3; i++) {
        SecurityTestEnvironment.Node n = env.node(0, secure("creator", "required"));
        if (i == 2)
          n.config.setProperty(
              "zookeeperConnectionString", "127.0.0.1:" + proxy.listener.getLocalPort());
        n.start();
      }
      env.nodes.get(0).user("offline", AuthType.Read);
      Connection old = env.nodes.get(2).raw();
      relogin(old, "offline", "password");
      proxy.pause();
      env.nodes.get(0).admin.execute("ALTER USER offline IDENTIFIED BY changed;");
      long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
      List<String> offline = new ArrayList<>();
      while (System.nanoTime() < until) {
        offline.add("offline status=" + old.sql("SHOW REPLICA NUMBER;").status.code);
        Thread.sleep(1000);
      }
      proxy.resume();
      long start = System.nanoTime();
      while (old.sql("SHOW REPLICA NUMBER;").status.code != 101
          && System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5)) Thread.sleep(100);
      denied(old.sql("SHOW REPLICA NUMBER;").status);
      assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5));
      Files.write(
          env.directory.resolve("igt31-disconnection.txt"), offline, StandardCharsets.UTF_8);
      relogin(old, "offline", "changed");
      old.execute("SHOW REPLICA NUMBER;");
    }
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
