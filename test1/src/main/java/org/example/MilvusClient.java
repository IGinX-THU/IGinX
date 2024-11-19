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
package org.example;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.database.request.DropDatabaseReq;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;


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

  public static void dropDatabase(MilvusClientV2 client, String databaseName) {
    try {
      client.useDatabase(databaseName);
    } catch (InterruptedException e) {
      return;
    } catch (IllegalArgumentException e) {
      return;
    }
    List<String> collections = client.listCollections().getCollectionNames();
    for (String collectionName : collections) {
      System.out.println("dorp collection : "+collectionName);
      client.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }
    client.dropDatabase(DropDatabaseReq.builder().databaseName(databaseName).build());
  }


  public static void main(String[] args) {
    try(MilvusClient milvusClient = new MilvusClient("grpc","172.25.148.188",19530,null)) {
      MilvusClientV2 client = milvusClient.getClient();
      List<String> databases = client.listDatabases().getDatabaseNames();

      for (String database : databases) {
        if (!database.equals("default")) {
          System.out.println("drop database : "+database);
          dropDatabase(client, database);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println("ok!");
  }


}
