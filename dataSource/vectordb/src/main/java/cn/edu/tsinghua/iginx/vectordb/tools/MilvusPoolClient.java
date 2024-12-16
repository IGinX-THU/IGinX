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

import cn.edu.tsinghua.iginx.vectordb.pool.MilvusConnectPool;
import io.milvus.v2.client.MilvusClientV2;
import java.io.Closeable;
import java.io.IOException;

/** Milvus 连接池客户端封装类, 通过MilvusConnectPool获取连接，并且支持自动释放连接。 */
public class MilvusPoolClient implements Closeable {

  private MilvusConnectPool milvusConnectPool;
  private MilvusClientV2 client;

  public MilvusPoolClient(MilvusConnectPool milvusConnectPool) {
    this.milvusConnectPool = milvusConnectPool;
  }

  /**
   * 获取 Milvus 客户端实例。
   *
   * @return MilvusServiceClient 实例
   */
  public MilvusClientV2 getClient() throws Exception {
    this.client = this.milvusConnectPool.getMilvusClient();
    return client;
  }

  /** 关闭连接。 */
  @Override
  public void close() throws IOException {
    this.milvusConnectPool.releaseMilvusClient(client);
  }
}
