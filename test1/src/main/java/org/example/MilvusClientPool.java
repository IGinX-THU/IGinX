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
package org.example;

import io.milvus.pool.MilvusClientV2Pool;
import io.milvus.pool.PoolConfig;
import io.milvus.v2.client.ConnectConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;


/** MilvusClientPool 提供了一个静态方法来创建和管理 Milvus 客户端池。 这个类主要用于优化对 Milvus 服务的连接管理，通过池化技术减少频繁创建和销毁连接的开销。 */
public class MilvusClientPool {

  /** 日志记录器，用于记录日志信息。 */
  private static final Logger LOGGER = LoggerFactory.getLogger(MilvusClientPool.class);

  /**
   * 创建一个 Milvus 客户端池。
   *
   * @param uri Milvus 服务的连接 URI。
   * @param username 用户名。
   * @param password 数据库密码。
   * @return 一个配置好的 Milvus 客户端池实例。
   * @throws ReflectiveOperationException 如果在创建池过程中发生反射操作异常。
   */
  public static MilvusClientV2Pool createPool(String uri, String username, String password) {
    // 构建默认池配置
    PoolConfig poolConfig =
        PoolConfig.builder()
            .maxIdlePerKey(10) // 每个键的最大空闲连接数
            .maxTotalPerKey(20) // 每个键的最大总连接数
            .maxTotal(100) // 池中所有键的最大总连接数
            .maxBlockWaitDuration(Duration.ofSeconds(60L)) // 获取连接的最大等待时间
            .minEvictableIdleDuration(Duration.ofSeconds(10L)) // 最小可驱逐的空闲时间
            .build();

    return createPool(uri, username, password, poolConfig);
  }

  /**
   * 创建一个 Milvus 客户端池。
   *
   * @param uri Milvus 服务的连接 URI。
   * @param username 用户名。
   * @param password 数据库密码。
   * @param poolConfig 客户端池的配置。
   * @return 一个配置好的 Milvus 客户端池实例。
   * @throws ReflectiveOperationException 如果在创建池过程中发生反射操作异常。
   */
  public static MilvusClientV2Pool createPool(
      String uri, String username, String password, PoolConfig poolConfig) {
    // 构建连接配置
    ConnectConfig.ConnectConfigBuilder builder = ConnectConfig.builder().uri(uri);
    if (!StringUtils.isEmpty(username)) {
      builder.username(username);
    }
    if (!StringUtils.isEmpty(password)) {
      builder.password(password);
    }
    ConnectConfig connectConfig = builder.build();

    MilvusClientV2Pool pool;
    try {
      // 创建并返回客户端池
      pool = new MilvusClientV2Pool(poolConfig, connectConfig);
      return pool;
    } catch (ReflectiveOperationException e) {
      // 记录错误日志
      LOGGER.error("MilvusClientV2Pool create error", e);
      return null;
    }
  }

  /**
   * 从参数映射中获取池配置。 如果参数映射中没有提供某些配置项，则使用默认值。
   *
   * @param params 包含池配置参数的映射。
   * @return 配置好的池配置对象。
   */
  public static PoolConfig getPoolConfig(Map<String, String> params) {
    // 从参数映射中获取配置值，如果没有则使用默认值
    int maxIdlePerKey =
        Integer.parseInt(
            params.getOrDefault(10, String.valueOf(10)));
    int maxTotalPerKey =
        Integer.parseInt(
            params.getOrDefault(20, String.valueOf(20)));
    int maxTotal =
        Integer.parseInt(params.getOrDefault(50, String.valueOf(50)));
    long maxBlockWaitDuration =
        Long.parseLong(
            params.getOrDefault(
                    5L, String.valueOf(5L)));
    long minEvictableIdleDuration =
        Long.parseLong(
            params.getOrDefault(
                    10L, String.valueOf(10L)));

    // 构建池配置
    PoolConfig poolConfig =
        PoolConfig.builder()
            .maxIdlePerKey(maxIdlePerKey)
            .maxTotalPerKey(maxTotalPerKey)
            .maxTotal(maxTotal)
            .maxBlockWaitDuration(Duration.ofSeconds(maxBlockWaitDuration))
            .minEvictableIdleDuration(Duration.ofSeconds(minEvictableIdleDuration))
            .build();

    return poolConfig;
  }
}
