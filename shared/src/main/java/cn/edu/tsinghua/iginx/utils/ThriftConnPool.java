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
package cn.edu.tsinghua.iginx.utils;

import java.time.Duration;
import java.util.Map;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftConnPool {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThriftConnPool.class);

  private static final int DEFAULT_MAX_SIZE = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;

  private static final int MAX_WAIT_TIME = 0; // infinite, same as Socket default

  private static final long IDLE_TIMEOUT =
      BaseObjectPoolConfig.DEFAULT_MIN_EVICTABLE_IDLE_DURATION.toMillis();

  private static final long TRY_TIMES = 6;

  private final GenericObjectPool<TTransport> pool;

  public ThriftConnPool(String ip, int port) {
    this(ip, port, DEFAULT_MAX_SIZE, MAX_WAIT_TIME, IDLE_TIMEOUT, TRY_TIMES);
  }

  public ThriftConnPool(String ip, int port, Map<String, String> extraParams) {
    this(
        ip,
        port,
        Integer.parseInt(
            extraParams.getOrDefault("thrift_pool_max_size", String.valueOf(DEFAULT_MAX_SIZE))),
        Integer.parseInt(extraParams.getOrDefault("thrift_timeout", String.valueOf(MAX_WAIT_TIME))),
        Long.parseLong(
            extraParams.getOrDefault(
                "thrift_pool_min_evictable_idle_time_millis", String.valueOf(IDLE_TIMEOUT))),
        Long.parseLong(
            extraParams.getOrDefault("thrift_pool_try_connect_times", String.valueOf(TRY_TIMES))));
  }

  public ThriftConnPool(
      String ip, int port, int maxSize, int maxWaitTime, long idleTimeout, long tryTimes) {
    GenericObjectPoolConfig<TTransport> poolConfig = new GenericObjectPoolConfig<>();
    poolConfig.setMaxTotal(maxSize);
    poolConfig.setMinEvictableIdleDuration(Duration.ofMillis(idleTimeout)); // 设置空闲连接的超时时间

    TSocketFactory socketFactory = new TSocketFactory(ip, port, maxWaitTime, tryTimes);
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

    private final int maxWaitTime; // 连接超时时间（毫秒） 以及 socket 超时时间
    private final long tryTimes;

    public TSocketFactory(String ip, int port, int maxWaitTime, long tryTimes) {
      this.ip = ip;
      this.port = port;
      this.maxWaitTime = maxWaitTime;
      this.tryTimes = tryTimes;
    }

    @Override
    public PooledObject<TTransport> makeObject() throws Exception {
      TSocket socket = new TSocket(ip, port, maxWaitTime);
      for (int i = 1; ; i++) {
        try {
          socket.open();
          return new DefaultPooledObject<>(socket);
        } catch (TTransportException e) {
          if (i >= tryTimes) {
            throw e;
          }
          LOGGER.warn("makeObject failed, retrying...", e);
        }
      }
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
