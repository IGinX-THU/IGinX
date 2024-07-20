/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.filestore.server;

import cn.edu.tsinghua.iginx.filestore.service.rpc.server.Server;
import java.net.InetSocketAddress;
import java.util.concurrent.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ServerTest {

  private final InetSocketAddress address;
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  public ServerTest() {
    address = new InetSocketAddress("127.0.0.1", 6667);
  }

  private Server server;
  private Future<?> serverFuture;

  @BeforeEach
  void setUp() throws Exception {
    Assertions.assertNull(server);
    Assertions.assertNull(serverFuture);

    server = new Server(address, null);
    serverFuture = executorService.submit(server::serve);
  }

  @AfterEach
  void tearDown() throws ExecutionException, InterruptedException, TimeoutException {
    if (server != null) {
      server.stop();
      server = null;
    }
    if (serverFuture != null) {
      serverFuture.get(1, TimeUnit.SECONDS);
      serverFuture = null;
    }
  }

  @Test
  void testServe() {
    // do nothing
  }
}
