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
package cn.edu.tsinghua.iginx.vectordb.tools;

import static cn.edu.tsinghua.iginx.vectordb.tools.Constants.*;

import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import java.io.Closeable;
import java.io.IOException;

/** Milvus 客户端封装类,创建Milvus连接，实现Closeable 接口，用于在完成操作后释放资源。 */
public class MilvusClient implements Closeable {

  private final MilvusClientV2 client;

  /**
   * 构造函数，创建一个新的 Milvus 连接。
   *
   * @param host Milvus 服务器的主机地址
   * @param port Milvus 服务器的端口
   * @param databaseName 要连接的数据库名称（可选）
   */
  public MilvusClient(String protocol, String host, int port, String databaseName) {
    ConnectConfig config = ConnectConfig.builder().uri(getUrl(protocol, host, port)).build();

    config.setConnectTimeoutMs(DEFAULT_MILVUS_CONNECT_TIMEOUT);
    config.setRpcDeadlineMs(DEFAULT_MILVUS_TIMEOUT);
    client = new MilvusClientV2(config);
    // 设置数据库（如果需要）
    if (databaseName != null && !databaseName.isEmpty()) {
      try {
        client.useDatabase(databaseName);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  protected String getUrl(String protocol, String host, int port) {
    return new StringBuilder(protocol)
        .append("://")
        .append(host)
        .append(":")
        .append(port)
        .toString();
  }

  public MilvusClient(StorageEngineMeta meta) {
    this(
        meta.getExtraParams().getOrDefault(DB_PROTOCOL, DEFAULT_DB_PROTOCOL),
        meta.getIp(),
        meta.getPort(),
        null);
  }

  /**
   * 获取 Milvus 客户端实例。
   *
   * @return MilvusServiceClient 实例
   */
  public MilvusClientV2 getClient() {
    return client;
  }

  /** 关闭连接。 */
  @Override
  public void close() throws IOException {
    if (client != null) {
      client.close();
    }
  }
}
