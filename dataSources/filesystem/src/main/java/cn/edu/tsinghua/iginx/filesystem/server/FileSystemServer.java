package cn.edu.tsinghua.iginx.filesystem.server;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.filesystem.exec.Executor;
import cn.edu.tsinghua.iginx.filesystem.thrift.FileSystemService;
import cn.edu.tsinghua.iginx.parquet.thrift.ParquetService;
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

    public FileSystemServer(int port, Executor executor) {
        this.port = port;
        this.executor = executor;
    }

    private void startServer() throws TTransportException {
        TProcessor processor = new FileSystemService.Processor<FileSystemService.Iface>(new FileSystemWorker(executor));
        TServerSocket serverTransport = new TServerSocket(port);
        TThreadPoolServer.Args args = new TThreadPoolServer
                .Args(serverTransport)
                .processor(processor)
                .minWorkerThreads(config.getMinThriftWorkerThreadNum())
                .maxWorkerThreads(config.getMaxThriftWrokerThreadNum());
        args.protocolFactory(new TBinaryProtocol.Factory());
        TServer server = new TThreadPoolServer(args);
        logger.info("parquet service starts successfully!");
        server.serve();
    }

    @Override
    public void run() {
        try {
            startServer();
        } catch (TTransportException e) {
            logger.error("FileSystem service starts failure.");
        }
    }
}
