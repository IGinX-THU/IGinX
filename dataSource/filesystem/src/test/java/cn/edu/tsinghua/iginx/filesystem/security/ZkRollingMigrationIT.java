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

import cn.edu.tsinghua.iginx.metadata.storage.zk.*;
import cn.edu.tsinghua.iginx.thrift.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

public class ZkRollingMigrationIT {
  @Test(timeout = 360000)
  public void igt43_migrateAndRollbackWithoutLosingDataOrMetadata() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment();
        CuratorFramework owner = ZkSecurityAcceptanceIT.client(env, true);
        CuratorFramework anonymous = ZkSecurityAcceptanceIT.client(env, false)) {
      owner
          .create()
          .creatingParentContainersIfNeeded()
          .withMode(CreateMode.CONTAINER)
          .forPath("/lock/user");
      ClusterTestFixture.start(env, 0, null, 3);
      SecurityTestEnvironment.Node first = env.nodes.get(0);
      first.packageScripts();
      first.user("retained_user", AuthType.Read);
      CodeTestFixture fixture = new CodeTestFixture(first);
      first.admin.execute(fixture.create("retained_function"));
      long job = CodeTestFixture.submit(first.admin, fixture.scheduled(first.admin.id));
      int value = 0;
      ClusterTestFixture.roundTrip(env, value++);
      for (SecurityTestEnvironment.Node node : env.nodes) {
        node.stop();
        node.security(secure("open", "optional"));
        node.start();
        ClusterTestFixture.roundTrip(env, value++);
      }
      String check = ZkSecurityAcceptanceIT.command(first, "check", 1);
      assertTrue(
          check,
          check.contains("Unexpected ACL")
              && check.contains("Unexpected root ACL")
              && check.contains("may be a container"));
      ZkSecurityAcceptanceIT.command(first, "apply", 0);
      ZkSecurityAcceptanceIT.command(first, "check", 0);
      first.user("during_migration", AuthType.Read);
      for (SecurityTestEnvironment.Node node : env.nodes) {
        node.stop();
        node.security(secure("creator", "required"));
        node.start();
        ClusterTestFixture.roundTrip(env, value++);
      }
      String second = ZkSecurityAcceptanceIT.command(first, "apply", 0);
      assertFalse(second, second.contains("0 changed"));
      ZkSecurityAcceptanceIT.command(first, "check", 0);
      assertTrue(ZkSecurityAcceptanceIT.command(first, "apply", 0).contains("0 changed"));
      for (String path : ZooKeeperSecurity.lockParents()) {
        assertEquals(0, owner.checkExists().forPath(path).getEphemeralOwner());
        assertFalse(env.zk.getZKDatabase().getDataTree().getContainers().contains(path));
      }
      for (SecurityTestEnvironment.Node node : env.nodes) {
        node.stop();
        node.security(secure("open", "optional"));
        node.start();
        ClusterTestFixture.roundTrip(env, value++);
      }
      ZkSecurityAcceptanceIT.command(first, "revert", 0);
      ZkSecurityAcceptanceIT.command(first, "check", 1);
      assertNotNull(anonymous.getData().forPath("/user/retained_user"));
      for (int i = 0; i < env.nodes.size(); i++) {
        SecurityTestEnvironment.Node node = env.nodes.get(i);
        node.stop();
        node.security(null);
        node.start();
        ClusterTestFixture.roundTrip(env, value++);
        if (i == 0) {
          env.nodes.get(1).user("during_rollback", AuthType.Read);
          long end = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
          while (!node.admin
                  .client
                  .getUser(new GetUserReq(node.admin.id))
                  .usernames
                  .contains("during_rollback")
              && System.nanoTime() < end) Thread.sleep(50);
          assertTrue(
              node.admin
                  .client
                  .getUser(new GetUserReq(node.admin.id))
                  .usernames
                  .contains("during_rollback"));
        }
        try (Connection raw = node.raw()) {
          ok(ClusterTestFixture.queryEventually(raw, "SELECT * FROM rolling;").status);
        }
      }
      assertEquals(
          value,
          ClusterTestFixture.queryEventually(first.admin, "SELECT * FROM rolling;")
              .queryDataSet
              .valuesList
              .size());
      assertTrue(
          first
              .admin
              .client
              .getUser(new GetUserReq(first.admin.id))
              .usernames
              .contains("retained_user"));
      assertTrue(
          first.admin.client.getRegisterTaskInfo(new GetRegisterTaskInfoReq(first.admin.id))
              .registerTaskInfoList.stream()
              .anyMatch(f -> f.name.equals("retained_function")));
      assertTrue(java.nio.file.Files.exists(first.home.resolve("jobs/" + job + ".yaml")));
      assertEquals(
          1,
          first
              .admin
              .client
              .showEligibleJob(
                  new ShowEligibleJobReq(first.admin.id).setJobState(JobState.JOB_IDLE))
              .jobStateMap
              .get(JobState.JOB_IDLE)
              .size());
    }
  }
}
