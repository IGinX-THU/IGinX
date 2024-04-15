package cn.edu.tsinghua.iginx.utils;

import java.time.Duration;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(ThriftConnPool.class);

  private static final int DEFAULT_MAX_SIZE = 100;

  private static final int MAX_WAIT_TIME = 30000;

  private static final long IDLE_TIMEOUT = 10L * 60L * 1000L;

  private final GenericObjectPool<TTransport> pool;

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

    GenericObjectPoolConfig<TTransport> poolConfig = new GenericObjectPoolConfig<>();
    poolConfig.setMaxTotal(maxSize);
    poolConfig.setMinEvictableIdleDuration(Duration.ofMillis(IDLE_TIMEOUT)); // 设置空闲连接的超时时间

    TSocketFactory socketFactory = new TSocketFactory(ip, port, maxWaitTime);
    pool = new GenericObjectPool<>(socketFactory, poolConfig);
  }

  public TTransport borrowTransport() {
    try {
      return pool.borrowObject();
    } catch (Exception e) {
      LOGGER.error("borrowing connection failed:", e);
      return null;
    }
  }

  private synchronized boolean isClosed() {
    return pool.isClosed();
  }

  public void returnTransport(TTransport transport) {
    if (isClosed()) {
      LOGGER.warn("returning connection to a closed connection pool.");
      return;
    }
    pool.returnObject(transport);
  }

  public void close() {
    LOGGER.info("closing connection pool...");
    pool.close();
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
      transport.close();
    }

    @Override
    public boolean validateObject(PooledObject<TTransport> pooledObject) {
      TTransport transport = pooledObject.getObject();
      return transport.isOpen();
    }

    @Override
    public void activateObject(PooledObject<TTransport> pooledObject) throws Exception {}

    @Override
    public void passivateObject(PooledObject<TTransport> pooledObject) throws Exception {}
  }
}
