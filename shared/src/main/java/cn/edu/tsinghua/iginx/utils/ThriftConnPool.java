package cn.edu.tsinghua.iginx.utils;

import java.util.Map;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftConnPool {
  private static final Logger logger = LoggerFactory.getLogger(ThriftConnPool.class);

  private final int maxSize;

  private static final int DEFAULT_MAX_SIZE = 100;

  private static final int MAX_WAIT_TIME = 30000;

  private boolean closed = false;

  private final String ip;

  private final int port;

  private GenericObjectPool<TTransport> pool;

  private final long idleTimeout = 60 * 10000L;

  public ThriftConnPool(String ip, int port) {
    this(ip, port, DEFAULT_MAX_SIZE, MAX_WAIT_TIME);
  }

  public ThriftConnPool(String ip, int port, Map<String, String> extraParams) {
    this(
        ip,
        port,
        DEFAULT_MAX_SIZE,
        Integer.parseInt(
            extraParams.getOrDefault("thrift_timeout", String.valueOf(MAX_WAIT_TIME))));
  }

  public ThriftConnPool(String ip, int port, int maxSize, int maxWaitTime) {
    this.ip = ip;
    this.port = port;
    this.maxSize = maxSize;

    GenericObjectPoolConfig<TTransport> poolConfig = new GenericObjectPoolConfig<>();
    poolConfig.setMaxTotal(maxSize);
    poolConfig.setMinEvictableIdleTimeMillis(idleTimeout); // 设置空闲连接的超时时间

    TSocketFactory socketFactory = new TSocketFactory(ip, port, maxWaitTime);
    pool = new GenericObjectPool<>(socketFactory, poolConfig);
  }

  public TTransport borrowTransport() {
    try {
      return pool.borrowObject();
    } catch (Exception e) {
      logger.error("borrowing connection failed:" + e);
      return null;
    }
  }

  private synchronized boolean isClosed() {
    return closed;
  }

  public void returnTransport(TTransport transport) {
    if (isClosed()) {
      logger.warn("returning connection to a closed connection pool.");
      return;
    }
    pool.returnObject(transport);
  }

  public void close() {
    logger.info("closing connection pool...");
    pool.close();
    closed = true;
  }

  public static class TSocketFactory implements PooledObjectFactory<TTransport> {
    private final String ip;
    private final int port;

    private final int maxWaitTime; // 连接超时时间（毫秒）

    public TSocketFactory(String ip, int port, int maxWaitTime) {
      this.ip = ip;
      this.port = port;
      this.maxWaitTime = maxWaitTime;
    }

    @Override
    public PooledObject<TTransport> makeObject() throws Exception {
      TTransport transport = new TSocket(ip, port, maxWaitTime);
      transport.open();
      return new DefaultPooledObject<>(transport);
    }

    @Override
    public void destroyObject(PooledObject<TTransport> pooledObject) throws Exception {
      TTransport transport = pooledObject.getObject();
      if (transport != null && transport.isOpen()) {
        transport.close();
      }
    }

    @Override
    public boolean validateObject(PooledObject<TTransport> pooledObject) {
      TTransport transport = pooledObject.getObject();
      return transport != null && transport.isOpen();
    }

    @Override
    public void activateObject(PooledObject<TTransport> pooledObject) throws Exception {
      TTransport transport = pooledObject.getObject();
      if (transport == null) {
        TTransport newTransport = new TSocket(ip, port, maxWaitTime);
        newTransport.open();
        pooledObject = new DefaultPooledObject<>(newTransport);
      } else if (!transport.isOpen()) {
        transport.open();
      }
    }

    @Override
    public void passivateObject(PooledObject<TTransport> pooledObject) throws Exception {
      TTransport transport = pooledObject.getObject();
      transport.close();
    }
  }
}
