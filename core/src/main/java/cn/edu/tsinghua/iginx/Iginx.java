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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.monitor.MonitorManager;
import cn.edu.tsinghua.iginx.mqtt.MQTTService;
import cn.edu.tsinghua.iginx.rest.RestServer;
import cn.edu.tsinghua.iginx.thrift.IService;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Iginx {

  private static final Logger LOGGER = LoggerFactory.getLogger(Iginx.class);

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  public static void main(String[] args) throws Exception {
    if (config.isEnableRestService()) {
      new Thread(new RestServer()).start();
    }
    if (config.isEnableMQTT()) {
      new Thread(MQTTService.getInstance()).start();
    }
    if (config.isEnableMonitor()) {
      new Thread(MonitorManager.getInstance()).start();
    }
    Iginx iginx = new Iginx();
    iginx.startServer();
  }

  private void startServer() throws TTransportException {
    TProcessor processor = new IService.Processor<IService.Iface>(IginxWorker.getInstance());
    TServerSocket serverTransport =
        new TServerSocket(ConfigDescriptor.getInstance().getConfig().getPort());
    TThreadPoolServer.Args args =
        new TThreadPoolServer.Args(serverTransport)
            .processor(processor)
            .minWorkerThreads(config.getMinThriftWorkerThreadNum())
            .maxWorkerThreads(config.getMaxThriftWrokerThreadNum());
    args.protocolFactory(new TBinaryProtocol.Factory());
    TServer server = new TThreadPoolServer(args);
    LOGGER.info("iginx starts successfully!");
    System.out.print("\n\nIGinX is now in service......\n\n");
    server.serve();
  }
}
