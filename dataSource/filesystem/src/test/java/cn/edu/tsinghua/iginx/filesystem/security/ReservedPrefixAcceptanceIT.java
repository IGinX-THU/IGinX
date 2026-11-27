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
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.*;

/** IGT-17..21. Every destructive negative is followed by a privileged read-back. */
public class ReservedPrefixAcceptanceIT {
  static SecurityTestEnvironment env;
  static SecurityTestEnvironment.Node node;
  static Connection root, ordinary, service;

  @BeforeClass
  public static void start() throws Exception {
    env = new SecurityTestEnvironment();
    node = env.node(0, secure("creator", "required"));
    node.start();
    root = node.admin;
    node.user("ordinary", AuthType.Read, AuthType.Write);
    node.user("indagent_data_t1", AuthType.Read, AuthType.Write);
    ordinary = node.connect("ordinary", "password");
    service = node.connect("indagent_data_t1", "password");
  }

  @AfterClass
  public static void stop() throws Exception {
    if (env != null) env.close();
  }

  @Before
  public void reset() throws Exception {
    service.execute("CLEAR DATA;");
    root.execute("CLEAR DATA;");
  }

  static void populate(Connection c, String prefix, int count, String stem) throws Exception {
    StringBuilder names = new StringBuilder("key"), values = new StringBuilder("1");
    for (int i = 0; i < count; i++) {
      names.append(',').append(stem).append(i);
      values.append(',').append(i + 1);
    }
    c.execute("INSERT INTO " + prefix + " (" + names + ") VALUES (" + values + ");");
  }

  static void ninety() throws Exception {
    populate(service, "indagent", 50, "c");
    populate(service, "bigfile", 5, "c");
    populate(service, "sys.blob", 3, "c");
    populate(service, "sys.other", 2, "d");
    populate(service, "a", 30, "c");
  }

  static String reserved() throws Exception {
    return service.execute("SELECT * FROM indagent;").toString()
        + service.execute("SELECT * FROM bigfile;").toString()
        + service.execute("SELECT * FROM sys.blob;").toString();
  }

  static void visible(List<String> paths, int size) {
    assertEquals(paths == null ? "null" : paths.toString(), size, paths == null ? 0 : paths.size());
    if (paths != null)
      for (String path : paths)
        assertFalse(
            path,
            path.contains("indagent.") || path.contains("bigfile.") || path.contains("sys.blob."));
  }

  @Test
  public void igt17_explicitSqlAndRpcPaths() throws Exception {
    populate(service, "a", 2, "c");
    for (String prefix : Arrays.asList("indagent.t", "bigfile.t", "sys.blob")) {
      service.execute("INSERT INTO " + prefix + " (key,a) VALUES (1,42);");
      for (Connection user : Arrays.asList(ordinary, root)) {
        for (String sql :
            Arrays.asList(
                "SELECT * FROM " + prefix + ";",
                "INSERT INTO " + prefix + " (key,b) VALUES (2,9);",
                "DELETE FROM " + prefix + " WHERE key<10;",
                "DELETE COLUMNS " + prefix + ".a;",
                "SHOW COLUMNS " + prefix + ".*;",
                "SELECT * FROM " + prefix + ".*;",
                "INSERT INTO " + prefix + " (key,a) VALUES (SELECT c0 FROM a);",
                "INSERT INTO output (key,a) VALUES (SELECT a FROM " + prefix + ");",
                "SELECT * FROM (SELECT * FROM " + prefix + ");",
                "LOAD DATA FROM INFILE \"missing.csv\" AS CSV INTO " + prefix + ";",
                "SELECT * FROM " + prefix + " INTO OUTFILE \"denied.csv\" AS CSV;")) {
          String before = service.execute("SELECT * FROM *;").toString();
          denied(user.sql(sql).status);
          assertEquals(sql, before, service.execute("SELECT * FROM *;").toString());
        }
        String before = reserved();
        denied(
            user.client.queryData(new QueryDataReq(user.id, Arrays.asList(prefix + ".a"), 0, 10))
                .status);
        denied(
            user.client.aggregateQuery(
                    new AggregateQueryReq(
                        user.id, Arrays.asList(prefix + ".a"), 0, 10, AggregateType.COUNT))
                .status);
        denied(
            user.client.deleteColumns(
                new DeleteColumnsReq(user.id, Arrays.asList("a.c0", prefix + ".a"))));
        denied(
            user.client.deleteDataInColumns(
                new DeleteDataInColumnsReq(user.id, Arrays.asList(prefix + ".a"), 0, 10)));
        assertEquals(before, reserved());
        assertEquals(2, ordinary.execute("SELECT * FROM a;").getPathsSize());
      }
      for (String method :
          Arrays.asList(
              "insertColumnRecords",
              "insertNonAlignedColumnRecords",
              "insertRowRecords",
              "insertNonAlignedRowRecords")) {
        java.lang.reflect.Method rpc =
            Arrays.stream(IService.Iface.class.getDeclaredMethods())
                .filter(m -> m.getName().equals(method))
                .findFirst()
                .get();
        for (Connection c : Arrays.asList(ordinary, root, service)) {
          org.apache.thrift.TBase request =
              new PermissionMatrixIT()
                  .insert(
                      (Class<? extends org.apache.thrift.TBase>) rpc.getParameterTypes()[0], c.id);
          request.setFieldValue(
              request.fieldForId(2), Arrays.asList("ordinary.mixed", prefix + ".rpc"));
          boolean rows = method.contains("Row");
          request.setFieldValue(
              request.fieldForId(4),
              rows
                  ? Arrays.asList(java.nio.ByteBuffer.allocate(16).putLong(0, 91).putLong(8, 92))
                  : Arrays.asList(
                      java.nio.ByteBuffer.allocate(8).putLong(0, 91),
                      java.nio.ByteBuffer.allocate(8).putLong(0, 92)));
          request.setFieldValue(
              request.fieldForId(5),
              rows
                  ? Arrays.asList(java.nio.ByteBuffer.wrap(new byte[] {3}))
                  : Arrays.asList(
                      java.nio.ByteBuffer.wrap(new byte[] {1}),
                      java.nio.ByteBuffer.wrap(new byte[] {1})));
          request.setFieldValue(request.fieldForId(6), Arrays.asList(DataType.LONG, DataType.LONG));
          String prior = service.execute("SELECT * FROM *;").toString();
          Status status = (Status) rpc.invoke(c.client, request);
          if (c == service) {
            ok(status);
            assertEquals(
                1,
                service
                    .client
                    .queryData(new QueryDataReq(c.id, Arrays.asList(prefix + ".rpc"), 0, 100))
                    .getPathsSize());
          } else {
            denied(status);
            assertEquals(
                "Atomic mixed insert: " + method,
                prior,
                service.execute("SELECT * FROM *;").toString());
          }
        }
      }
      service.execute("SELECT * FROM " + prefix + ";");
      service.execute("SHOW COLUMNS " + prefix + ".*;");
      service.execute("INSERT INTO " + prefix + " (key,copy) VALUES (SELECT c0 FROM a);");
      service.execute("INSERT INTO output (key,copy) VALUES (SELECT a FROM " + prefix + ");");
      service.execute("SELECT * FROM (SELECT * FROM " + prefix + ");");
      service.execute("SELECT * FROM " + prefix + " INTO OUTFILE \"allowed.csv\" AS CSV;");
      String upload = "allowed-" + UUID.randomUUID() + ".csv";
      SqlSecurityAcceptanceIT.upload(
          service, upload, "key,csv\n4,44\n".getBytes(StandardCharsets.UTF_8));
      ok(
          service.client.loadCSV(
                  new LoadCSVReq(
                      service.id,
                      "LOAD DATA FROM INFILE \"source.csv\" AS CSV INTO " + prefix + ";",
                      upload))
              .status);
      assertEquals(
          1,
          service
              .client
              .queryData(new QueryDataReq(service.id, Arrays.asList(prefix + ".csv"), 0, 10))
              .getPathsSize());
      ok(
          service.client.deleteColumns(
              new DeleteColumnsReq(service.id, Arrays.asList(prefix + ".rpc"))));
      ok(
          service.client.deleteDataInColumns(
              new DeleteDataInColumnsReq(service.id, Arrays.asList(prefix + ".csv"), 0, 10)));
      service.execute("DELETE COLUMNS " + prefix + ".copy;");
      service.execute("DELETE FROM " + prefix + " WHERE key<0;");
    }
    String file = "header-" + UUID.randomUUID() + ".csv";
    SqlSecurityAcceptanceIT.upload(
        ordinary, file, "key,other.c,blob.c\n2,11,22\n".getBytes(StandardCharsets.UTF_8));
    String before = service.execute("SELECT * FROM *;").toString();
    denied(
        ordinary.client.loadCSV(
                new LoadCSVReq(
                    ordinary.id, "LOAD DATA FROM INFILE \"source.csv\" AS CSV INTO sys;", file))
            .status);
    assertEquals(before, service.execute("SELECT * FROM *;").toString());
    root.execute("INSERT INTO a.t (key,a) VALUES (1,5);");
    for (String value : Arrays.asList("indagent.t.a", "t.a", "*")) {
      root.execute("INSERT INTO dyn (key,p) VALUES (1,\"" + value + "\");");
      String query =
          "SELECT VALUE2META(SELECT p FROM dyn) FROM "
              + (value.equals("indagent.t.a") ? "a, b" : "*")
              + ";";
      if (value.equals("indagent.t.a")) denied(ordinary.sql(query).status);
      else
        for (String path : ordinary.execute(query).getPaths())
          assertFalse(
              path,
              path.startsWith("indagent.")
                  || path.startsWith("bigfile.")
                  || path.startsWith("sys.blob."));
      service.execute(query);
    }
  }

  @Test
  public void igt18_wildcardReadCountAndPagination() throws Exception {
    ninety();
    visible(ordinary.execute("SELECT * FROM *;").paths, 32);
    visible(ordinary.execute("SHOW COLUMNS;").paths, 32);
    visible(ordinary.execute("SHOW COLUMNS LIMIT 10 OFFSET 25;").paths, 7);
    visible(ordinary.execute("SHOW COLUMNS LIMIT 10 OFFSET 32;").paths, 0);
    assertEquals(90, service.execute("SHOW COLUMNS;").getPathsSize());
    assertEquals(10, service.execute("SHOW COLUMNS LIMIT 10 OFFSET 25;").getPathsSize());
    assertEquals(32, ordinary.execute("COUNT POINTS;").pointsNum);
    assertEquals(90, service.execute("COUNT POINTS;").pointsNum);
    assertEquals(32, ordinary.execute("SELECT count(*) FROM *;").getPathsSize());
    visible(ordinary.execute("SELECT * FROM sys.*;").paths, 2);
    ExecuteSqlResp nested = ordinary.execute("SELECT * FROM (SHOW COLUMNS);");
    assertFalse(nested.toString().contains("indagent"));
    for (String pattern : Arrays.asList("*", "in*")) {
      int count = pattern.equals("*") ? 32 : 0;
      QueryDataResp query =
          ordinary.client.queryData(new QueryDataReq(ordinary.id, Arrays.asList(pattern), 0, 10));
      ok(query.status);
      visible(query.paths, count);
      AggregateQueryResp aggregate =
          ordinary.client.aggregateQuery(
              new AggregateQueryReq(
                  ordinary.id, Arrays.asList(pattern), 0, 10, AggregateType.COUNT));
      ok(aggregate.status);
      visible(aggregate.paths, count);
      DownsampleQueryResp down =
          ordinary.client.downsampleQuery(
              new DownsampleQueryReq(
                  ordinary.id, Arrays.asList(pattern), 0, 10, AggregateType.COUNT, 5));
      ok(down.status);
      List<String> dataColumns =
          down.paths == null ? new ArrayList<>() : new ArrayList<>(down.paths);
      dataColumns.removeAll(Arrays.asList("window_start", "window_end"));
      visible(dataColumns, count);
      LastQueryResp last =
          ordinary.client.lastQuery(new LastQueryReq(ordinary.id, Arrays.asList(pattern), 0));
      ok(last.status);
      List<String> lastPaths = new ArrayList<>();
      if (last.queryDataSet != null)
        for (List<Object> row :
            new cn.edu.tsinghua.iginx.session.SessionQueryDataSet(last).getValues())
          lastPaths.add(new String((byte[]) row.get(0), StandardCharsets.UTF_8));
      visible(lastPaths, count);
      // Public ShowColumnsReq has no path argument; verify its full visible result.
      ShowColumnsResp cols = ordinary.client.showColumns(new ShowColumnsReq(ordinary.id));
      ok(cols.status);
      visible(cols.paths, 32);
    }
    visible(
        ordinary.client.queryData(new QueryDataReq(ordinary.id, Arrays.asList("*.c0"), 0, 10))
            .paths,
        1);
    // In IGinX FROM denotes a table prefix. The document's leaf-pattern spelling
    // appends another wildcard and is empty even for the allowlisted account.
    visible(ordinary.execute("SELECT c0 FROM *;").paths, 1);
    visible(ordinary.execute("SELECT * FROM *.c0;").paths, 0);
    assertEquals(0, service.execute("SELECT * FROM *.c0;").getPathsSize());
  }

  @Test
  public void igt19_everyWildcardDeletePreservesData() throws Exception {
    for (Connection user : Arrays.asList(ordinary, root))
      for (int operation = 0; operation < 7; operation++) {
        service.execute("CLEAR DATA;");
        ninety();
        String before = reserved();
        switch (operation) {
          case 0:
            user.execute("DELETE FROM * WHERE key<10;");
            break;
          case 1:
            user.execute("DELETE COLUMNS *.c0;");
            break;
          case 2:
            user.execute("DELETE COLUMNS *;");
            break;
          case 3:
            user.execute("CLEAR DATA;");
            break;
          case 4:
            ok(user.client.deleteColumns(new DeleteColumnsReq(user.id, Arrays.asList("in*"))));
            break;
          case 5:
            ok(user.client.deleteColumns(new DeleteColumnsReq(user.id, Arrays.asList("*"))));
            break;
          default:
            ok(
                user.client.deleteDataInColumns(
                    new DeleteDataInColumnsReq(user.id, Arrays.asList("*"), 0, 10)));
        }
        assertEquals("operation=" + operation, before, reserved());
        assertEquals(
            operation == 4 ? 32 : operation == 1 ? 31 : 0, user.execute("COUNT POINTS;").pointsNum);
      }
  }

  @Test
  public void igt20_boundariesQuotesTagsAndCase() throws Exception {
    for (String prefix : Arrays.asList("indagentx", "a.indagent")) {
      ordinary.execute("INSERT INTO " + prefix + " (key,b) VALUES (1,11);");
      assertEquals(1, ordinary.execute("SELECT * FROM " + prefix + ";").getPathsSize());
    }
    service.execute("INSERT INTO indagent.t (key,a) VALUES (1,9876);");
    for (Connection c : Arrays.asList(ordinary, root, service)) {
      InsertNonAlignedRowRecordsReq tagged =
          new InsertNonAlignedRowRecordsReq(
                  c.id,
                  Arrays.asList("indagent.t.tagged"),
                  java.nio.ByteBuffer.allocate(8).putLong(0, 2),
                  Arrays.asList(java.nio.ByteBuffer.allocate(8).putLong(0, 99)),
                  Arrays.asList(java.nio.ByteBuffer.wrap(new byte[] {1})),
                  Arrays.asList(DataType.LONG))
              .setTagsList(Arrays.asList(Collections.singletonMap("site", "one")));
      String before = reserved();
      Status status = c.client.insertNonAlignedRowRecords(tagged);
      if (c == service) {
        ok(status);
        assertEquals(
            1,
            service
                .client
                .queryData(
                    new QueryDataReq(service.id, Arrays.asList("indagent.t.tagged"), 0, 10)
                        .setTagsList(
                            Arrays.asList(Collections.singletonMap("site", Arrays.asList("one")))))
                .getPathsSize());
      } else {
        denied(status);
        assertEquals(before, reserved());
      }
    }
    for (String path : Arrays.asList("indagent.t.a{k=v}", "`indagent`.t.a", "indagent.`t`.a")) {
      denied(
          ordinary.client.queryData(new QueryDataReq(ordinary.id, Arrays.asList(path), 0, 10))
              .status);
      denied(ordinary.client.deleteColumns(new DeleteColumnsReq(ordinary.id, Arrays.asList(path))));
    }
    denied(ordinary.sql("SELECT * FROM `indagent`.t;").status);
    visible(ordinary.execute("SELECT * FROM `in*`.t;").paths, 0);
    ExecuteSqlResp upper = ordinary.execute("SELECT * FROM INDAGENT.t;");
    assertEquals(0, upper.getPathsSize());
    assertEquals(2, service.execute("SELECT * FROM indagent.t;").getPathsSize());
  }

  @Test
  public void igt21_allowlistRotationWithoutRestart() throws Exception {
    node.user("indagent_data_v1", AuthType.Read, AuthType.Write);
    Connection v1 = node.connect("indagent_data_v1", "password");
    v1.execute("INSERT INTO indagent.t (key,a) VALUES (1,17);");
    v1.execute("SELECT * FROM indagent.t;");
    node.user("indagent_data_v2", AuthType.Read, AuthType.Write);
    Connection v2 = node.connect("indagent_data_v2", "password");
    assertEquals(1, v2.execute("SELECT * FROM indagent.t;").getPathsSize());
    v2.execute("INSERT INTO indagent.t (key,a) VALUES (2,19);");
    long start = System.nanoTime();
    ok(root.client.deleteUser(new DeleteUserReq(root.id, "indagent_data_v1")));
    while (v1.sql("SELECT * FROM indagent.t;").status.code != 101
        && System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5)) Thread.sleep(20);
    denied(v1.sql("SELECT * FROM indagent.t;").status);
    assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5));
    node.user("other_admin", AuthType.values());
    Connection other = node.connect("other_admin", "password");
    denied(other.sql("SELECT * FROM indagent.t;").status);
  }
}
