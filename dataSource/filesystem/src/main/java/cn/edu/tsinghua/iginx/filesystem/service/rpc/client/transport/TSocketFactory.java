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
package cn.edu.tsinghua.iginx.filesystem.service.rpc.client.transport;

import cn.edu.tsinghua.iginx.filesystem.service.rpc.client.ClientConfig;
import java.net.InetSocketAddress;
import java.util.Objects;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

public class TSocketFactory implements TTransportFactory {
  private final InetSocketAddress address;
  private final ClientConfig config;

  public TSocketFactory(InetSocketAddress address, ClientConfig config) {
    this.address = Objects.requireNonNull(address);
    this.config = Objects.requireNonNull(config);
  }

  @Override
  public TSocket create() throws TTransportException {
    int socketTimeout = Math.toIntExact(config.getSocketTimeout().toMillis());
    int connectTimeout = Math.toIntExact(config.getConnectTimeout().toMillis());

    return new TSocket(
        new TConfiguration(),
        address.getHostString(),
        address.getPort(),
        socketTimeout,
        connectTimeout);
  }
}
