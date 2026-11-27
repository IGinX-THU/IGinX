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
package cn.edu.tsinghua.iginx.filesystem.security;

import static cn.edu.tsinghua.iginx.filesystem.security.SecurityTestEnvironment.*;
import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.thrift.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import org.junit.Test;

/** Complements the unmodified IoTDB12 + FileSystem ClusterIT job in security-test.yml. */
public class ClusterRegressionAcceptanceIT {
  @Test(timeout = 150000)
  public void igt33_storageChangesRestartsAndFourthNode() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment()) {
      ClusterTestFixture.start(env, 0, secure("creator", "required"), 3);
      SecurityTestEnvironment.Node first = env.nodes.get(0);
      StorageTestFixture storage = new StorageTestFixture(first);
      storage.ensure(true);
      first.admin.execute(
          "ALTER STORAGEENGINE "
              + storage.id()
              + " OPTIONS (dummy_dir '"
              + storage.directory.toString().replace('\\', '/')
              + "');");
      storage.ensure(false);
      int value = 0;
      for (SecurityTestEnvironment.Node node : env.nodes) {
        node.stop();
        node.start();
        ClusterTestFixture.roundTrip(env, value++);
      }
      SecurityTestEnvironment.Node extra = env.node(0, secure("creator", "required"));
      extra.start();
      ClusterTestFixture.waitForCluster(first, 4, 4);
      ClusterTestFixture.roundTrip(env, value);
      try (Connection raw = extra.raw()) {
        denied(raw.client.executeSql(new ExecuteSqlReq(0, "SHOW COLUMNS;")).status);
      }
    }
  }

  @Test(timeout = 240000)
  public void igt33_thousandRowsCloseAndRevoke() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment()) {
      ClusterTestFixture.start(env, 1, secure("creator", "required"), 3);
      SecurityTestEnvironment.Node first = env.nodes.get(0);
      first.user("writer", AuthType.Read, AuthType.Write);
      try (Connection c = first.connect("writer", "password")) {
        for (int row = 0; row < 1000; row++)
          c.execute("INSERT INTO durable_close (key,a) VALUES (" + row + "," + row + ");");
        ok(c.client.closeSession(new CloseSessionReq(c.id)));
      }
      CountDownLatch started = new CountDownLatch(1);
      ExecutorService executor = Executors.newSingleThreadExecutor();
      final List<Integer> accepted = Collections.synchronizedList(new ArrayList<>());
      final List<Integer> uncertain = Collections.synchronizedList(new ArrayList<>());
      try {
        Future<?> writing =
            executor.submit(
                () -> {
                  try (Connection c = env.nodes.get(1).connect("writer", "password")) {
                    for (int row = 0; row < 1000; row++) {
                      Status status =
                          c.sql(
                                  "INSERT INTO durable_revoke (key,a) VALUES ("
                                      + row
                                      + ","
                                      + row
                                      + ");")
                              .status;
                      if (status.code == 200) accepted.add(row);
                      else if (status.code == 401) {
                        assertEquals("Session has been closed", status.message);
                        uncertain.add(row);
                      } else denied(status);
                      if (row == 249) started.countDown();
                    }
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                });
        assertTrue(started.await(40, TimeUnit.SECONDS));
        first.admin.execute("ALTER USER writer IDENTIFIED BY changed;");
        writing.get(60, TimeUnit.SECONDS);
      } finally {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      }
      assertTrue(accepted.size() >= 250 && accepted.size() < 1000);
      assertEquals(
          1000, first.admin.execute("SELECT * FROM durable_close;").queryDataSet.valuesList.size());
      ExecuteSqlResp revoked = first.admin.execute("SELECT * FROM durable_revoke;");
      Set<Integer> persisted = new HashSet<>();
      for (long key :
          cn.edu.tsinghua.iginx.utils.ByteUtils.getLongArrayFromByteBuffer(
              revoked.queryDataSet.keys)) persisted.add((int) key);
      assertTrue("An acknowledged write was lost", persisted.containsAll(accepted));
      Set<Integer> possible = new HashSet<>(accepted);
      possible.addAll(uncertain);
      assertTrue("An entry-denied write was stored", possible.containsAll(persisted));
      for (List<Object> row :
          cn.edu.tsinghua.iginx.utils.ByteUtils.getValuesFromBufferAndBitmaps(
              revoked.dataTypeList,
              revoked.queryDataSet.valuesList,
              revoked.queryDataSet.bitmapList))
        assertTrue(persisted.contains(((Number) row.get(0)).intValue()));
      String before = first.admin.execute("SELECT * FROM *;").toString();
      // A barrier read on every node plus physical copy inspection prevents a test from passing
      // merely because the failed node happened to be a replica.
      for (SecurityTestEnvironment.Node node : env.nodes) node.admin.execute("SELECT * FROM *;");
      long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      int copies;
      do {
        copies = 0;
        for (SecurityTestEnvironment.Node node : env.nodes)
          if (ClusterTestFixture.physical(node, SECRET).values().stream()
              .anyMatch(data -> data.rows.size() >= 1000)) copies++;
        if (copies >= 2) break;
        Thread.sleep(100);
      } while (System.nanoTime() < until);
      assertTrue(copies >= 2);
      Files.write(
          env.directory.resolve("igt33-accepted-rows.txt"),
          Arrays.asList(
              "close=1000",
              "revoke-acknowledged=" + accepted.size(),
              "revoke-in-flight=" + uncertain.size(),
              "revoke-persisted=" + persisted.size(),
              "physical-copies=" + copies),
          StandardCharsets.UTF_8);
    }
  }
}
