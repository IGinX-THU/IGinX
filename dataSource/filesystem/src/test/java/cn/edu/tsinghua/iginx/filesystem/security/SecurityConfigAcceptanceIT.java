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
import org.junit.Test;

public class SecurityConfigAcceptanceIT {
  @Test(timeout = 90000)
  public void igt01_absentEmptyAndZookeeperOnlyModes() throws Exception {
    for (String security :
        Arrays.asList(
            null,
            "",
            "zookeeper.user=meta\nzookeeper.password=" + SECRET + "\nzookeeper.acl=open\n"))
      try (SecurityTestEnvironment env = new SecurityTestEnvironment()) {
        SecurityTestEnvironment.Node node = env.node(0, security);
        node.start();
        node.admin.execute("INSERT INTO indagent.t (key,a) VALUES (1,1);");
        node.admin.execute("INSERT INTO business (key,a) VALUES (1,2);");
        assertEquals(2, node.admin.execute("SELECT * FROM *;").paths.size());
        assertEquals(1, node.admin.execute("SELECT * FROM indagent.t;").paths.size());
        assertFalse(ClusterTestFixture.physical(node, null).isEmpty());
        try (Connection raw = node.raw()) {
          Status status = raw.client.executeSql(new ExecuteSqlReq(0, "SELECT * FROM *;")).status;
          if (security == null) ok(status);
          else denied(status);
        }
        if (security == null) {
          node.user("legacy_read", AuthType.Read);
          try (Connection c = node.connect("legacy_read", "password")) {
            c.execute("INSERT INTO legacy (key,a) VALUES (1,1);");
            c.execute("SHOW CONFIG;");
            c.execute("SHOW SESSIONID;");
            node.admin.execute("ALTER USER legacy_read IDENTIFIED BY changed;");
            c.execute("SELECT * FROM legacy;");
          }
        }
      }
  }

  @Test
  public void igt02_environmentCannotOverrideSecretsOrLeakThem() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment()) {
      SecurityTestEnvironment.Node node = env.node(0, secure("creator", "required"));
      node.environment.put("internal.secret", "environment-must-not-override-file");
      node.environment.put("reserved.prefixes", "business");
      node.jvmOptions.add("-Dinternal.secret=system-property-must-not-override-file");
      node.start();
      node.admin.execute("INSERT INTO business (key,a) VALUES (1,2);");
      denied(node.admin.sql("SELECT * FROM indagent.t;").status);
      assertFalse(ClusterTestFixture.physical(node, SECRET).isEmpty());
      for (java.lang.reflect.Field field :
          cn.edu.tsinghua.iginx.conf.Config.class.getDeclaredFields())
        assertFalse(
            field.getName(),
            field.getName().equals("internal.secret")
                || field.getName().equals("zookeeper.password")
                || field.getName().equals("etcd.password"));
      assertFalse(node.admin.execute("SHOW CONFIG;").toString().contains(SECRET));
      assertFalse(node.logs().contains(SECRET));
      if (windows()) assertTrue(node.logs(), node.logs().contains("Set OS access controls"));
    }
  }
}
