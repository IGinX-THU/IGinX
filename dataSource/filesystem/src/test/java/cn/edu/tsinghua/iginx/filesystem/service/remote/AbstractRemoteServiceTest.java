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
package cn.edu.tsinghua.iginx.filesystem.service.remote;

import cn.edu.tsinghua.iginx.filesystem.service.Service;
import cn.edu.tsinghua.iginx.filesystem.service.rpc.client.ClientConfig;
import cn.edu.tsinghua.iginx.filesystem.service.rpc.client.RemoteService;
import cn.edu.tsinghua.iginx.filesystem.service.rpc.server.Server;
import cn.edu.tsinghua.iginx.filesystem.service.storage.AbstractStorageServiceTest;
import com.typesafe.config.Config;
import java.net.InetSocketAddress;
import java.util.Random;

public class AbstractRemoteServiceTest extends AbstractStorageServiceTest {

  private final InetSocketAddress address;

  protected AbstractRemoteServiceTest(String type, Config config) {
    super(type, config);
    this.address = new InetSocketAddress("localhost", new Random().nextInt(50000) + 15530);
  }

  private Service service;
  private Server server;

  @Override
  protected Service getService() throws Exception {
    if (service == null) {
      server = new Server(address, super.getService());
      service = new RemoteService(address, new ClientConfig());
    }
    return service;
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (service != null) {
      service.close();
      server.close();
      service = null;
      server = null;
    }
  }
}
