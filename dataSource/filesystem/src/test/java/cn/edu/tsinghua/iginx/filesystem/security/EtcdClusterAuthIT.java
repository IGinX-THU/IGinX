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

import cn.edu.tsinghua.iginx.metadata.storage.etcd.EtcdAuthMigration;
import cn.edu.tsinghua.iginx.security.SecurityConfig;
import cn.edu.tsinghua.iginx.thrift.*;
import io.etcd.jetcd.*;
import io.etcd.jetcd.auth.Permission;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import org.junit.Test;

public class EtcdClusterAuthIT {
  static ByteSequence b(String text) {
    return ByteSequence.from(text, StandardCharsets.UTF_8);
  }

  static <T> T await(CompletableFuture<T> future) throws Exception {
    return future.get(10, TimeUnit.SECONDS);
  }

  static Client client(String endpoint, String user) {
    io.etcd.jetcd.ClientBuilder builder = Client.builder().endpoints(endpoint);
    if (user != null) builder.user(b(user)).password(b(SECRET));
    return builder.build();
  }

  interface Operation {
    void run() throws Exception;
  }

  static void rejected(Operation operation) throws Exception {
    try {
      operation.run();
      fail("etcd unauthorized access accepted");
    } catch (ExecutionException expected) {
      io.grpc.Status status = io.grpc.Status.fromThrowable(expected);
      io.grpc.Status.Code code = status.getCode();
      assertTrue(
          expected.toString(),
          code == io.grpc.Status.Code.PERMISSION_DENIED
              || code == io.grpc.Status.Code.UNAUTHENTICATED
              || (code == io.grpc.Status.Code.INVALID_ARGUMENT
                  && "etcdserver: user name is empty".equals(status.getDescription())));
    }
  }

  @Test(timeout = 240000)
  public void igt50_authenticatedClusterWatchIsolationAndReversibleMigration() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment();
        EtcdTestServer server = new EtcdTestServer(env.directory);
        Client anonymous = client(server.endpoint, null)) {
      Auth bootstrap = anonymous.getAuthClient();
      await(bootstrap.userAdd(b("root"), b(SECRET)));
      await(bootstrap.userGrantRole(b("root"), b("root")));
      await(bootstrap.userAdd(b("other"), b(SECRET)));
      await(bootstrap.roleAdd(b("other_role")));
      await(
          bootstrap.roleGrantPermission(
              b("other_role"), b("/other/"), b("/other0"), Permission.Type.READWRITE));
      await(bootstrap.userGrantRole(b("other"), b("other_role")));
      await(anonymous.getKVClient().put(b("/other/retained"), b("unchanged")));
      for (int i = 0; i < 3; i++) {
        SecurityTestEnvironment.Node node = env.node(0, null);
        node.config.setProperty("metaStorage", "etcd");
        node.config.setProperty("etcdEndpoints", server.endpoint);
        node.start();
      }
      ClusterTestFixture.roundTrip(env, 1);
      env.nodes.get(0).user("retained", AuthType.Read, AuthType.Write);
      SecurityTestEnvironment.Node first = env.nodes.get(0);
      first.security("etcd.user=iginx\netcd.password=" + SECRET + "\n");
      SecurityConfig security = SecurityConfig.load(first.home.resolve("security.properties"));
      assertEquals(1, EtcdAuthMigration.run(bootstrap, security, "check"));
      assertEquals(0, EtcdAuthMigration.run(bootstrap, security, "apply"));
      // Pin the actual jetcd compatibility result instead of silently authenticating anonymously.
      // Section 3.10 permits a maintenance window when credentialed clients cannot connect before
      // authEnable.
      try (Client configured = client(server.endpoint, "iginx")) {
        try {
          await(configured.getKVClient().get(b("/user")));
          fail("Revisit maintenance-window test after changing jetcd");
        } catch (ExecutionException expected) {
          assertTrue(
              expected.toString(),
              expected.getCause().getMessage().contains("authentication is not enabled"));
        }
      }
      for (SecurityTestEnvironment.Node node : env.nodes) node.stop();
      await(bootstrap.authEnable());
      try (Client root = client(server.endpoint, "root");
          Client other = client(server.endpoint, "other");
          Client configured = client(server.endpoint, "iginx")) {
        int value = 2;
        for (SecurityTestEnvironment.Node node : env.nodes) {
          node.security("etcd.user=iginx\netcd.password=" + SECRET + "\n");
          node.start();
        }
        ClusterTestFixture.waitForCluster(first, 3, 3);
        ClusterTestFixture.roundTrip(env, value++);
        Connection[] old = new Connection[3];
        for (int i = 0; i < 3; i++) {
          old[i] = env.nodes.get(i).connect("retained", "password");
          old[i].execute("SELECT * FROM rolling;");
        }
        first.admin.execute("ALTER USER retained IDENTIFIED BY changed;");
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        for (Connection c : old) {
          while (c.sql("SELECT * FROM rolling;").status.code != 101 && System.nanoTime() < end)
            Thread.sleep(50);
          denied(c.sql("SELECT * FROM rolling;").status);
        }
        for (SecurityTestEnvironment.Node node : env.nodes)
          try (Connection c = node.connect("retained", "changed")) {
            c.execute("INSERT INTO watched (key,a) VALUES (1,1);");
          }
        assertEquals(0, EtcdAuthMigration.run(root.getAuthClient(), security, "check"));
        for (String key : Arrays.asList("/user/retained", "/storage", "/other/retained")) {
          rejected(() -> await(anonymous.getKVClient().get(b(key))));
          rejected(() -> await(anonymous.getKVClient().put(b(key), b("forbidden"))));
        }
        for (String key :
            Arrays.asList("/other/retained", "/indagent/private", "/application-user", "/")) {
          rejected(() -> await(configured.getKVClient().get(b(key))));
          rejected(() -> await(configured.getKVClient().put(b(key), b("forbidden"))));
        }
        assertEquals(
            "unchanged",
            await(other.getKVClient().get(b("/other/retained")))
                .getKvs()
                .get(0)
                .getValue()
                .toString(StandardCharsets.UTF_8));
        await(other.getKVClient().put(b("/other/new"), b("allowed")));
        assertEquals(1, await(other.getKVClient().get(b("/other/new"))).getCount());
        long revision =
            await(root.getKVClient().get(b("/user/retained"))).getHeader().getRevision();
        assertEquals(0, EtcdAuthMigration.run(root.getAuthClient(), security, "apply"));
        assertEquals(
            revision, await(root.getKVClient().get(b("/user/retained"))).getHeader().getRevision());
        for (SecurityTestEnvironment.Node node : env.nodes) node.stop();
        assertEquals(0, EtcdAuthMigration.run(root.getAuthClient(), security, "revert"));
        assertEquals(0, EtcdAuthMigration.run(root.getAuthClient(), security, "revert"));
        assertEquals(1, EtcdAuthMigration.run(root.getAuthClient(), security, "check"));
        assertEquals(
            Arrays.asList("other_role"),
            await(root.getAuthClient().userGet(b("other"))).getRoles());
        await(root.getAuthClient().authDisable());
        for (SecurityTestEnvironment.Node node : env.nodes) {
          node.security(null);
          node.start();
        }
        ClusterTestFixture.roundTrip(env, value);
        assertEquals(
            value, first.admin.execute("SELECT * FROM rolling;").queryDataSet.valuesList.size());
        assertEquals(
            "unchanged",
            await(anonymous.getKVClient().get(b("/other/retained")))
                .getKvs()
                .get(0)
                .getValue()
                .toString(StandardCharsets.UTF_8));
      }
    }
  }
}
