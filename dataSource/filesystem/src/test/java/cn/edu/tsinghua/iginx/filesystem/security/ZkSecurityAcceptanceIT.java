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

import cn.edu.tsinghua.iginx.metadata.storage.constant.Constant;
import cn.edu.tsinghua.iginx.metadata.storage.zk.*;
import cn.edu.tsinghua.iginx.security.SecurityConfig;
import cn.edu.tsinghua.iginx.thrift.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.server.*;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.Test;

public class ZkSecurityAcceptanceIT {
  interface Operation {
    void run() throws Exception;
  }

  static void noAuth(String label, Operation action) throws Exception {
    try {
      action.run();
      fail("Anonymous operation succeeded: " + label);
    } catch (KeeperException.NoAuthException expected) {
    }
  }

  static CuratorFramework client(SecurityTestEnvironment env, boolean authenticated)
      throws Exception {
    CuratorFrameworkFactory.Builder builder =
        CuratorFrameworkFactory.builder()
            .connectString("127.0.0.1:" + env.factory.getLocalPort())
            .retryPolicy(new ExponentialBackoffRetry(50, 2));
    if (authenticated)
      builder.authorization("digest", ("meta:" + SECRET).getBytes(StandardCharsets.UTF_8));
    CuratorFramework client = builder.build();
    client.start();
    assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS));
    return client;
  }

  static Set<String> constants() throws Exception {
    Set<String> result = new TreeSet<>();
    for (java.lang.reflect.Field field : Constant.class.getFields())
      if (field.getType() == String.class) {
        String value = (String) field.get(null);
        if (value.startsWith("/")) result.add(value);
      }
    result.addAll(ZooKeeperSecurity.lockParents());
    result.add("/policy");
    return result;
  }

  static SecurityConfig security(SecurityTestEnvironment.Node node) {
    return SecurityConfig.load(node.home.resolve("security.properties"));
  }

  static void exerciseLock(CuratorFramework owner, String parent) throws Exception {
    String child = parent + "/acceptance-lock";
    owner.create().withMode(CreateMode.EPHEMERAL).forPath(child);
    owner.delete().forPath(child);
  }

  @Test(timeout = 90000)
  public void igt40_41_42_allMetadataPathsAndPersistentLockParents() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment()) {
      SecurityTestEnvironment.Node node = env.node(0, secure("creator", "required"));
      node.start();
      String password = UUID.randomUUID().toString().replace("-", "").substring(0, 24);
      ok(
          node.admin.client.addUser(
              new AddUserReq(node.admin.id, "random_user", password, EnumSet.of(AuthType.Read))));
      CodeTestFixture fixture = new CodeTestFixture(node);
      node.admin.execute(fixture.create("lock_function"));
      long job = CodeTestFixture.submit(node.admin, fixture.scheduled(node.admin.id));
      try (CuratorFramework owner = client(env, true);
          CuratorFramework anonymous = client(env, false)) {
        // Some statistics paths appear only with a policy workload. Exercise every constant even
        // when the current workload has not populated it yet.
        for (String path : constants())
          if (owner.checkExists().forPath(path) == null)
            owner
                .create()
                .creatingParentsIfNeeded()
                .withACL(ZooDefs.Ids.CREATOR_ALL_ACL)
                .forPath(path);
        assertEquals(0, ZooKeeperAclMigration.run(owner, security(node), "apply"));
        node.stop();
        for (String path : constants()) {
          noAuth("read " + path, () -> anonymous.getData().forPath(path));
          noAuth("list " + path, () -> anonymous.getChildren().forPath(path));
          noAuth("create " + path, () -> anonymous.create().forPath(path + "/unauthorized"));
          noAuth("set " + path, () -> anonymous.setData().forPath(path, new byte[] {1}));
          noAuth("delete " + path, () -> anonymous.delete().forPath(path));
          noAuth("getACL " + path, () -> anonymous.getACL().forPath(path));
          byte[] bytes = owner.getData().forPath(path);
          owner.getChildren().forPath(path);
          owner.setData().forPath(path, bytes);
          String child = path + "/authorized";
          owner.create().withACL(ZooDefs.Ids.CREATOR_ALL_ACL).forPath(child, new byte[] {7});
          assertArrayEquals(new byte[] {7}, owner.getData().forPath(child));
          owner.delete().forPath(child);
        }
        noAuth("create at root", () -> anonymous.create().forPath("/unauthorized"));
        String digest = DigestAuthenticationProvider.generateDigest("meta:" + SECRET);
        try {
          assertFalse(anonymous.getACL().forPath("/").toString().contains(digest));
        } catch (KeeperException.NoAuthException expected) {
        }
        Deque<String> pending = new ArrayDeque<>();
        pending.add("/");
        Set<String> visited = new HashSet<>();
        while (!pending.isEmpty()) {
          String path = pending.remove();
          if (!visited.add(path)) continue;
          try {
            assertFalse(
                path,
                new String(anonymous.getData().forPath(path), StandardCharsets.UTF_8)
                    .contains(password));
          } catch (KeeperException.NoAuthException expected) {
          }
          try {
            for (String child : anonymous.getChildren().forPath(path))
              pending.add(path.equals("/") ? "/" + child : path + "/" + child);
          } catch (KeeperException.NoAuthException expected) {
          }
        }
        node.start();
        for (String path :
            Arrays.asList("/lock/user", "/lock/storage", "/lock/pyFunction", "/lock/transformJob"))
          exerciseLock(owner, path);
        java.lang.reflect.Field field = ZooKeeperServer.class.getDeclaredField("firstProcessor");
        field.setAccessible(true);
        ContainerManager collector =
            new ContainerManager(
                env.zk.getZKDatabase(), (RequestProcessor) field.get(env.zk), 1000, 1000);
        collector.start();
        try {
          long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
          while (System.nanoTime() < end) Thread.sleep(250);
        } finally {
          collector.stop();
        }
        assertEquals(25, ZooKeeperSecurity.lockParents().size());
        for (String path : ZooKeeperSecurity.lockParents()) {
          assertEquals(path, 0, owner.checkExists().forPath(path).getEphemeralOwner());
          assertFalse(path, env.zk.getZKDatabase().getDataTree().getContainers().contains(path));
          noAuth(path, () -> anonymous.create().forPath(path + "/after-idle"));
        }
        node.user("after_idle", AuthType.Read);
        node.admin.execute("INSERT INTO locking (key,a) VALUES (1,1);");
        // Recovery allocates a new execution ID, preserving the persisted schedule name/YAML.
        assertTrue(Files.exists(node.home.resolve("jobs/" + job + ".yaml")));
        assertEquals(
            1,
            node.admin
                .client
                .showEligibleJob(
                    new ShowEligibleJobReq(node.admin.id).setJobState(JobState.JOB_IDLE))
                .jobStateMap
                .get(JobState.JOB_IDLE)
                .size());
      }
    }
  }

  @Test
  public void igt44_otherApplicationsKeepTheirAclAndData() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment();
        CuratorFramework owner = client(env, true);
        CuratorFramework other =
            CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:" + env.factory.getLocalPort())
                .retryPolicy(new ExponentialBackoffRetry(50, 2))
                .authorization(
                    "digest",
                    "another:separate-application-password".getBytes(StandardCharsets.UTF_8))
                .build()) {
      other.start();
      assertTrue(other.blockUntilConnected(5, TimeUnit.SECONDS));
      for (String path : Arrays.asList("/other", "/indagent"))
        other.create().withACL(ZooDefs.Ids.CREATOR_ALL_ACL).forPath(path, new byte[] {7});
      Map<String, List<ACL>> before = new HashMap<>();
      Map<String, byte[]> data = new HashMap<>();
      for (String path : Arrays.asList("/other", "/indagent", "/zookeeper")) {
        before.put(path, other.getACL().forPath(path));
        data.put(path, other.getData().forPath(path));
      }
      SecurityTestEnvironment.Node node = env.node(0, secure("open", "optional"));
      node.start();
      for (String action : Arrays.asList("apply", "revert")) {
        assertEquals(0, ZooKeeperAclMigration.run(owner, security(node), action));
        for (String path : before.keySet()) {
          assertEquals(before.get(path), other.getACL().forPath(path));
          assertArrayEquals(data.get(path), other.getData().forPath(path));
        }
        other.create().forPath("/other/child");
        other.delete().forPath("/other/child");
        other.setData().forPath("/indagent", new byte[] {7});
        if (action.equals("apply"))
          noAuth("other app create at root", () -> other.create().forPath("/not-preprovisioned"));
      }
    }
  }

  static String command(SecurityTestEnvironment.Node node, String action, int expected)
      throws Exception {
    Process process = node.launch(node.script("zk_acl_migrate", action), "migration-" + action);
    try {
      assertTrue("Migration timeout: " + node.log, process.waitFor(25, TimeUnit.SECONDS));
      String out = node.logs();
      assertEquals(out, expected, process.exitValue());
      assertFalse(out.contains(SECRET));
      return out;
    } finally {
      if (process.isAlive()) process.destroyForcibly();
    }
  }

  @Test(timeout = 90000)
  public void igt45_realScriptsFailureExitCodesAndHeldLockRetry() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment();
        CuratorFramework owner = client(env, true)) {
      // Legacy container, held during the first apply. Other paths can be hardened immediately.
      owner
          .create()
          .creatingParentContainersIfNeeded()
          .withMode(CreateMode.CONTAINER)
          .forPath("/lock/user");
      SecurityTestEnvironment.Node node = env.node(0, secure("open", "optional"));
      node.start();
      node.packageScripts();
      command(node, "check", 1);
      owner.create().withMode(CreateMode.EPHEMERAL).forPath("/lock/user/held");
      String apply = command(node, "apply", 1);
      assertTrue(apply, apply.contains("Active lock parent skipped: /lock/user"));
      node.security(secure("open", "optional").replace(SECRET, "incorrect-password-01234567890"));
      String wrong = command(node, "check", 1);
      assertTrue(wrong, wrong.contains("account or password is incorrect"));
      assertFalse(wrong.contains("incorrect-password-01234567890"));
      node.security(secure("open", "optional"));
      String endpoint = node.config.getProperty("zookeeperConnectionString");
      node.config.setProperty("zookeeperConnectionString", "127.0.0.1:" + port());
      node.writeConfig();
      assertTrue(command(node, "check", 1).contains("cannot connect"));
      node.config.setProperty("zookeeperConnectionString", endpoint);
      node.writeConfig();
      owner.delete().forPath("/lock/user/held");
      command(node, "apply", 0);
      command(node, "check", 0);
      assertFalse(env.zk.getZKDatabase().getDataTree().getContainers().contains("/lock/user"));
      command(node, "revert", 0);
      assertTrue(command(node, "revert", 0).contains("0 changed"));
    }
  }
}
