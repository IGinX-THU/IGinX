package cn.edu.tsinghua.iginx.parquet.entity;

import cn.edu.tsinghua.iginx.parquet.thrift.ParquetService.Client;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedDeque;

public class ThriftConnPool {
  private static final Logger logger = LoggerFactory.getLogger(ThriftConnPool.class);

  private final ConcurrentLinkedDeque<TTransport> availableTransportQueue = new ConcurrentLinkedDeque<>();

  private static int size;

  private final int max_size;

  private static final int DEFAULT_MAX_SIZE = 100;

  private static final long WAIT_TIME = 1000;

  private static final long MAX_WAIT_TIME = 30_000;

  private boolean closed = false;

  private final String ip;

  private final int port;

  public ThriftConnPool(String ip, int port) {
    this(ip, port, DEFAULT_MAX_SIZE);
  }

  public ThriftConnPool(String ip, int port, int max_size) {
    this.ip = ip;
    this.port = port;
    this.max_size = max_size;
    size = 0;
  }

  public Client borrowClient(){
    if (isClosed()) {
      logger.error("client pool closed.");
      return null;
    }
    TTransport transport = availableTransportQueue.poll();
    if (transport != null) {
      return new Client(new TBinaryProtocol(transport));
    }

    boolean canCreate = false;
    synchronized (this) {
      if (size < this.max_size) canCreate = true;
    }

    if (canCreate) {
      try {
        if (isClosed()) {
          logger.error("connection pool closed.");
          return null;
        }
        transport = newTransport();
        if (!transport.isOpen()){
          transport.open();
        }
      } catch (TTransportException e) {
        logger.error("creating new connection failed:" + e);
        return null;
      }
      synchronized (this) {
        size++;
      }
    } else {
      long startTime = System.currentTimeMillis();
      while (transport == null) {
        synchronized (this) {
          if (isClosed()) {
            logger.error("connection pool closed.");
            return null;
          }
          try {
            this.wait(WAIT_TIME);
            if (System.currentTimeMillis() - startTime > MAX_WAIT_TIME) {
              logger.error("time out for connection.");
              return null;
            }
          } catch (InterruptedException e) {
            logger.error("the connection pool is damaged", e);
            Thread.currentThread().interrupt();
          }
          transport = availableTransportQueue.poll();
        }
      }
    }

    return new Client(new TBinaryProtocol(transport));
  }

  private synchronized boolean isClosed() {
    return closed;
  }

  public void returnClient(Client client) {
    if (isClosed()) {
      client.getInputProtocol().getTransport().close();
      logger.warn("returning connection to a closed connection pool.");
      return;
    }
    availableTransportQueue.offer(client.getInputProtocol().getTransport());
    synchronized (this) {
      this.notify();
    }
  }

  private TTransport newTransport() throws TTransportException {
    TTransport transport = new TSocket(this.ip, this.port);
    if (!transport.isOpen()){
      transport.open();
    }
    return transport;
  }

  public void close() {
    logger.info("closing connection pool...");
    for (TTransport transport :
            availableTransportQueue) {
      transport.close();
    }
    synchronized (this) {
      closed = true;
      availableTransportQueue.clear();
    }
  }
}
