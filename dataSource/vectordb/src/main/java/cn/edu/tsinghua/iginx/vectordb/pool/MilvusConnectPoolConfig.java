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
package cn.edu.tsinghua.iginx.vectordb.pool;

import io.milvus.v2.client.MilvusClientV2;
import java.time.Duration;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MilvusConnectPoolConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(MilvusConnectPoolConfig.class);
  private MilvusConnectPool pool;

  private String username;

  private String password;

  private String host;

  private Integer port;

  /** 最大空闲数 */
  private Integer maxIdle = 10;

  /** 最小空闲数 */
  private Integer minIdle = 4;

  /** 最大总数 */
  private Integer maxTotal = 20;

  /** 连接协议 */
  private String protocol;

  public MilvusConnectPoolConfig(
      String host, Integer port, String protocol, String username, String password) {
    this.username = username;
    this.password = password;
    this.host = host;
    this.port = port;
    this.protocol = protocol;
  }

  public MilvusConnectPoolConfig(
      String host,
      Integer port,
      String protocol,
      String username,
      String password,
      int maxIdle,
      int minIdle,
      int maxTotal) {
    this.username = username;
    this.password = password;
    this.host = host;
    this.port = port;
    this.maxIdle = maxIdle;
    this.minIdle = minIdle;
    this.maxTotal = maxTotal;
    this.protocol = protocol;
  }

  public MilvusConnectPool milvusConnectPool() {
    // 配置连接池的参数
    GenericObjectPoolConfig<MilvusClientV2> config = new GenericObjectPoolConfig<>();
    config.setMaxTotal(maxTotal); // 设置连接池的最大连接数
    config.setMaxIdle(maxIdle); // 设置连接池的最大空闲连接数
    config.setMinIdle(minIdle); // 设置连接池的最小空闲连接数
    config.setMinEvictableIdleTime(Duration.ofMinutes(30)); // 逐出连接的最小空闲时间, 默认1800000毫秒(30分钟)
    config.setTimeBetweenEvictionRuns(Duration.ofMinutes(30)); // 多久执行一次对象扫描，将无用的对象销毁，默认-1不扫描
    config.setTestOnBorrow(true); // 在获取对象的时候检查有效性, 默认false
    config.setTestOnReturn(false); // 在归还对象的时候检查有效性, 默认false
    config.setTestWhileIdle(true); // 在空闲时检查有效性, 默认false
    config.setMaxWait(Duration.ofSeconds(3)); // 最大等待时间， 默认的值为-1，表示无限等待。
    config.setLifo(true); // 是否启用后进先出, 默认true
    config.setBlockWhenExhausted(true); // 连接耗尽时是否阻塞, false立即抛异常,true阻塞直到超时, 默认true
    config.setNumTestsPerEvictionRun(3); // 每次逐出检查时 逐出的最大数目 默认3
    // 此处建议关闭jmx或是设置config.setJmxNameBase(), 因为默认注册的jmx会与项目可能已经存在的其他基于池类的实现bean冲突
    config.setJmxEnabled(false);

    // 创建连接工厂
    MilvusConnectPoolFactory factory =
        new MilvusConnectPoolFactory(host, port, protocol, username, password);

    // 初始化连接池
    pool = new MilvusConnectPool(factory, config);

    // 以最小空闲数量为初始连接数, 添加初始连接
    if (minIdle > 0) {
      for (int i = 0; i < minIdle; i++) {
        try {
          pool.addObject();
        } catch (Exception e) {
          LOGGER.error("添加初始连接失败");
        }
      }
    }
    return pool;
  }

  /** 注销连接池 */
  public void close() {
    if (pool != null) {
      pool.close();
    }
  }
}
