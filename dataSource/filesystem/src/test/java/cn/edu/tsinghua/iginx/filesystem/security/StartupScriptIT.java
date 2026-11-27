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

import cn.edu.tsinghua.iginx.thrift.ExecuteSqlReq;
import java.net.*;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class StartupScriptIT {
  @Test(timeout = 150000)
  public void igt62_invalidThenCorrectedConfigurationThroughRealScript() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment()) {
      SecurityTestEnvironment.Node node = env.node(0, null);
      node.writeConfig();
      node.packageScripts();
      List<String> invalid =
          new ArrayList<>(Arrays.asList("reserved.prefixs=indagent", "reserved.prefixes=indagent"));
      if (!windows()) invalid.add(secure("creator", "required"));
      for (int i = 0; i < invalid.size(); i++) {
        node.security(invalid.get(i));
        if (i == 2)
          Files.setPosixFilePermissions(
              node.home.resolve("security.properties"),
              EnumSet.of(
                  PosixFilePermission.OWNER_READ,
                  PosixFilePermission.OWNER_WRITE,
                  PosixFilePermission.GROUP_READ,
                  PosixFilePermission.OTHERS_READ));
        node.process = node.launch(node.script("start_iginx"), "bad-script");
        assertTrue(node.logs(), node.process.waitFor(30, TimeUnit.SECONDS));
        assertNotEquals(node.logs(), 0, node.process.exitValue());
        assertTrue(
            node.logs(), node.logs().contains(i == 2 ? "must not be readable" : "reserved."));
        assertFalse(node.logs().contains(SECRET));
        try (Socket socket = new Socket()) {
          try {
            socket.connect(new InetSocketAddress("127.0.0.1", node.rpc), 500);
            fail("Invalid startup opened Thrift");
          } catch (ConnectException | SocketTimeoutException expected) {
          }
        }
        try {
          node.connect("root", "root");
          fail("Invalid startup accepted root");
        } catch (org.apache.thrift.transport.TTransportException expected) {
        }
      }
      node.security(secure("creator", "required"));
      node.process = node.launch(node.script("start_iginx"), "good-script");
      node.awaitReady("root");
      node.rememberScriptProcess();
      try (Connection raw = node.raw()) {
        denied(raw.client.executeSql(new ExecuteSqlReq(0, "SELECT * FROM *;")).status);
      }
    }
  }
}
