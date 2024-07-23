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
package cn.edu.tsinghua.iginx.filestore.service.rpc.server;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.filestore.service.Service;
import cn.edu.tsinghua.iginx.filestore.thrift.FileStoreRpc;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {

  private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

  private TServerSocket serverTransport;

  private TServer server;

  public Server(InetSocketAddress address, Service service) throws TTransportException {
    serverTransport = new TServerSocket(address);
    TProcessor processor =
        new FileStoreRpc.Processor<FileStoreRpc.Iface>(new ServerWorker(service));
    Config config = ConfigDescriptor.getInstance().getConfig();
    ExecutorService executorService =
        new ThreadPoolExecutor(
            config.getMinThriftWorkerThreadNum(),
            config.getMaxThriftWrokerThreadNum(),
            60L,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new ThreadFactoryBuilder()
                .setNameFormat("FileStoreServer(" + address + ")-%d")
                .build());
    TThreadPoolServer.Args args =
        new TThreadPoolServer.Args(serverTransport)
            .processor(processor)
            .executorService(executorService)
            .protocolFactory(new TBinaryProtocol.Factory());
    server = new TThreadPoolServer(args);
  }

  public void serve() {
    server.serve();
  }

  public void stop() {
    if (server != null) {
      server.stop();
      server = null;
    }
    if (serverTransport != null) {
      serverTransport.close();
      serverTransport = null;
    }
  }
}
