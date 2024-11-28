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

import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.hutool.core.collection.ConcurrentHashSet;
import io.milvus.v2.client.MilvusClientV2;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MilvusBackupThread extends Thread {
  private MilvusBackup backup;

  private static Map<String, Set<String>> collectionBackup;

  public MilvusBackupThread(MilvusBackup backup) {
    this.backup = backup;
    collectionBackup = new ConcurrentHashMap<>();
  }

  @Override
  public void run() {
    collectionBackup.put("unit0000000001", new ConcurrentHashSet<>());
    collectionBackup.put("unit0000000002", new ConcurrentHashSet<>());
    collectionBackup.put("unit0000000003", new ConcurrentHashSet<>());
    collectionBackup.put("unit0000000004", new ConcurrentHashSet<>());
    while (true) {
      check();
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void check() {
    sync();

    for (int i = 1; i < 5; i++) {
      if (!collectionBackup.containsKey("unit000000000" + i)) {
        createBackupDatabase("unit000000000" + i);
      }
    }

    for (Map.Entry<String, Set<String>> entry : this.collectionBackup.entrySet()) {
      if (entry.getValue().size() < 10) {
        new Thread(() -> createBackupCollections(entry.getKey())).start();
      }
    }
  }

  public void sync() {
    try (MilvusPoolClient milvusClient =
        new MilvusPoolClient(this.backup.getStorage().getMilvusConnectPool())) {
      MilvusClientV2 client = milvusClient.getClient();
      List<String> databases = client.listDatabases().getDatabaseNames();

      for (String databaseName : databases) {
        client.useDatabase(databaseName);
        List<String> collections = client.listCollections().getCollectionNames();
        Set<String> currentCollection = new HashSet<>();
        currentCollection.addAll(collections);
        for (Map.Entry<String, Set<String>> entry : collectionBackup.entrySet()) {
          if (!currentCollection.contains(entry.getKey())) {
            collectionBackup.get(databaseName).remove(entry.getKey());
          }
        }
        for (String collectionName : collections) {
          if (collectionName.startsWith("backup")
              && !collectionBackup
                  .computeIfAbsent(databaseName, s -> new ConcurrentHashSet<>())
                  .contains(collectionName)) {
            collectionBackup
                .computeIfAbsent(databaseName, s -> new ConcurrentHashSet<>())
                .add(collectionName);
          }
        }
      }

    } catch (Exception e) {

    }
  }

  public void createBackupCollections(String databaseName) {
    try (MilvusPoolClient milvusClient =
        new MilvusPoolClient(this.backup.getStorage().getMilvusConnectPool())) {
      MilvusClientV2 client = milvusClient.getClient();
      for (int i = 0; i < 10 - this.collectionBackup.get(databaseName).size(); i++) {
        String collectionName = "backup" + UUID.randomUUID().toString().replace("-", "");
        new Thread(
                () -> {
                  try {
                    MilvusClientUtils.doCreateCollection(
                        client, databaseName, collectionName, DataType.LONG);
                    collectionBackup
                        .computeIfAbsent(databaseName, s -> new ConcurrentHashSet<>())
                        .add(collectionName);
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                  }
                })
            .start();
      }
    } catch (Exception e) {

    }
  }

  public void createBackupDatabase(String databaseName) {
    try (MilvusPoolClient milvusClient =
        new MilvusPoolClient(this.backup.getStorage().getMilvusConnectPool())) {
      MilvusClientV2 client = milvusClient.getClient();
      MilvusClientUtils.doCreateDatabase(client, databaseName);
      client.useDatabase(databaseName);
      for (int i = 0; i < 10; i++) {
        String collectionName = "backup" + i;
        new Thread(
                () -> {
                  try {
                    MilvusClientUtils.doCreateCollection(
                        client, databaseName, collectionName, DataType.LONG);
                    collectionBackup
                        .computeIfAbsent(databaseName, s -> new ConcurrentHashSet<>())
                        .add(collectionName);
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                  }
                })
            .start();
      }

    } catch (Exception e) {
    }
  }

  public static synchronized String getBackupCollection(String database) {
    if (collectionBackup.containsKey(database) && !collectionBackup.get(database).isEmpty()) {
      Set<String> set = collectionBackup.get(database);
      String collectionName = set.iterator().next();
      set.remove(collectionName);
      return collectionName;
    }
    return null;
  }
}
