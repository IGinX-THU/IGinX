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

import java.net.*;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;

final class EtcdTestServer implements AutoCloseable {
  final Process process;
  final String endpoint;

  EtcdTestServer(Path home) throws Exception {
    String binary = System.getProperty("iginx.test.etcd", System.getenv("IGINX_TEST_ETCD"));
    assertNotNull("Set IGINX_TEST_ETCD to etcd 3.5.13", binary);
    int client = port(), peer = port();
    endpoint = "http://127.0.0.1:" + client;
    process =
        new ProcessBuilder(
                binary,
                "--data-dir",
                home.resolve("etcd-data").toString(),
                "--listen-client-urls",
                endpoint,
                "--advertise-client-urls",
                endpoint,
                "--listen-peer-urls",
                "http://127.0.0.1:" + peer,
                "--initial-advertise-peer-urls",
                "http://127.0.0.1:" + peer,
                "--initial-cluster",
                "default=http://127.0.0.1:" + peer)
            .redirectErrorStream(true)
            .redirectOutput(home.resolve("etcd.log").toFile())
            .start();
    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < end && process.isAlive())
      try (Socket s = new Socket("127.0.0.1", client)) {
        return;
      } catch (java.io.IOException retry) {
        Thread.sleep(50);
      }
    close();
    throw new AssertionError("etcd not ready: " + home.resolve("etcd.log"));
  }

  public void close() throws Exception {
    process.destroy();
    if (!process.waitFor(5, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      assertTrue(process.waitFor(5, TimeUnit.SECONDS));
    }
  }
}
