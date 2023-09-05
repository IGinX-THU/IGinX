package cn.edu.tsinghua.iginx.filesystem.server;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.filesystem.exec.Executor;
import cn.edu.tsinghua.iginx.filesystem.thrift.FileSystemService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemServer implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(FileSystemServer.class);

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private final int port;

  private final Executor executor;

  private TServerSocket serverTransport;

  private TServer server;

  private ExecutorService executorService;

  public FileSystemServer(int port, Executor executor) {
    this.port = port;
    this.executor = executor;
    initServer();
  }

  private void initServer() {
    try {
      TProcessor processor =
          new FileSystemService.Processor<FileSystemService.Iface>(new FileSystemWorker(executor));
      serverTransport = new TServerSocket(port);
      executorService = Executors.newCachedThreadPool();
      TThreadPoolServer.Args args =
          new TThreadPoolServer.Args(serverTransport)
              .processor(processor)
              .minWorkerThreads(config.getMinThriftWorkerThreadNum())
              .maxWorkerThreads(config.getMaxThriftWrokerThreadNum())
              .protocolFactory(new TBinaryProtocol.Factory())
              .executorService(executorService);
      server = new TThreadPoolServer(args);
      logger.info("File System service starts successfully!");
    } catch (TTransportException e) {
      logger.error("File System service starts failure: {}", e.getMessage());
    }
  }

  public void stop() {
    if (server != null) {
      server.stop();
    }
    if (serverTransport != null) {
      serverTransport.close();
    }
    if (executorService != null) {
      executorService.shutdown();
    }
  }

  @Override
  public void run() {
    server.serve();
  }
}
