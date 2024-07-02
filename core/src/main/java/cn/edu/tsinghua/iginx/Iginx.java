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
