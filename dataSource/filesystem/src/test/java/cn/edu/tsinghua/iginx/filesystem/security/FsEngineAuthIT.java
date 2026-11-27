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
import org.junit.Test;

public class FsEngineAuthIT {
  @Test(timeout = 120000)
  public void igt34_fourMethodsTwoInvalidCredentialsPreserveBothReplicas() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment()) {
      ClusterTestFixture.start(env, 1, secure("creator", "required"), 2);
      env.nodes.get(0).user("indagent_data_fs", AuthType.Read, AuthType.Write);
      try (Connection writer = env.nodes.get(2).connect("indagent_data_fs", "password")) {
        writer.execute("INSERT INTO indagent.t (key,a,b) VALUES (1,11,12),(2,21,22);");
      }
      for (int i = 0; i < 2; i++) {
        long end = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        while (ClusterTestFixture.physical(env.nodes.get(i), SECRET).isEmpty()
            && System.nanoTime() < end) Thread.sleep(50);
        assertFalse(ClusterTestFixture.physical(env.nodes.get(i), SECRET).isEmpty());
        ClusterTestFixture.rejectRaw(env.nodes.get(i));
      }
      try (Connection reader = env.nodes.get(1).connect("indagent_data_fs", "password")) {
        ExecuteSqlResp r = reader.execute("SELECT * FROM indagent.t;");
        assertEquals(2, r.paths.size());
        assertEquals(2, r.queryDataSet.valuesList.size());
      }
      ClusterTestFixture.roundTrip(env, 42);
    }
  }

  @Test(timeout = 240000)
  public void igt35_optionalThenRequiredRollingRestart() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment()) {
      ClusterTestFixture.start(env, 1, null, 2);
      int value = 0;
      ClusterTestFixture.roundTrip(env, value++);
      for (String mode : new String[] {"optional", "required"})
        for (SecurityTestEnvironment.Node node : env.nodes) {
          node.stop();
          node.security("internal.secret=" + SECRET + "\ninternal.auth=" + mode + "\n");
          node.start();
          ClusterTestFixture.roundTrip(env, value++);
        }
      ClusterTestFixture.rejectRaw(env.nodes.get(0));
      ClusterTestFixture.rejectRaw(env.nodes.get(1));
    }
  }
}
