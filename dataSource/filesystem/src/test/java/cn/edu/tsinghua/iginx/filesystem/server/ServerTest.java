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
package cn.edu.tsinghua.iginx.filesystem.server;

import cn.edu.tsinghua.iginx.filesystem.service.rpc.server.Server;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ServerTest {

  private final InetSocketAddress address;
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  public ServerTest() {
    address = new InetSocketAddress("127.0.0.1", 6668);
  }

  private Server server;
  private Future<?> serverFuture;

  @BeforeEach
  void setUp() throws Exception {
    Assertions.assertNull(server);
    Assertions.assertNull(serverFuture);

    server = new Server(address, null);
  }

  @AfterEach
  void tearDown() {
    if (server != null) {
      server.close();
      server = null;
    }
  }

  @Test
  void testServe() {
    // do nothing
  }
}
