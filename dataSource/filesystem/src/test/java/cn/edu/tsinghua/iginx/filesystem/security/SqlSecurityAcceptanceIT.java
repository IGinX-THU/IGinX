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
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.apache.thrift.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.protocol.TType;
import org.junit.*;

/** IGT-10,12,13,14,15: assertions cross the public Thrift boundary. */
public class SqlSecurityAcceptanceIT {
  static SecurityTestEnvironment env;
  static SecurityTestEnvironment.Node node;
  static Connection root, read, write, admin;

  @BeforeClass
  public static void start() throws Exception {
    env = new SecurityTestEnvironment();
    node = env.node(0, secure("creator", "required"));
    node.start();
    root = node.admin;
    node.user("matrix_read", AuthType.Read);
    node.user("matrix_write", AuthType.Write);
    node.user("matrix_admin", AuthType.Admin);
    read = node.connect("matrix_read", "password");
    write = node.connect("matrix_write", "password");
    admin = node.connect("matrix_admin", "password");
  }

  @AfterClass
  public static void stop() throws Exception {
    if (env != null) env.close();
  }

  @Before
  public void seed() throws Exception {
    root.execute("CLEAR DATA;");
    root.execute("INSERT INTO business (key,a) VALUES (1,11),(2,22),(3,33);");
  }

  static Status status(Object result) throws Exception {
    return result instanceof Status
        ? (Status) result
        : (Status) result.getClass().getMethod("getStatus").invoke(result);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  static TBase minimum(Class<?> type) throws Exception {
    TBase value = (TBase) type.newInstance();
    Map<? extends TFieldIdEnum, FieldMetaData> fields =
        FieldMetaData.getStructMetaDataMap((Class) type);
    for (Map.Entry<? extends TFieldIdEnum, FieldMetaData> field : fields.entrySet()) {
      if (field.getValue().requirementType != TFieldRequirementType.OPTIONAL)
        value.setFieldValue(field.getKey(), defaultValue(field.getValue().valueMetaData));
    }
    return value;
  }

  static Object defaultValue(FieldValueMetaData meta) throws Exception {
    switch (meta.type) {
      case TType.BOOL:
        return false;
      case TType.BYTE:
        return (byte) 0;
      case TType.I16:
        return (short) 0;
      case TType.I32:
        return 0;
      case TType.I64:
        return 0L;
      case TType.DOUBLE:
        return 0.0;
      case TType.STRING:
        return meta.isBinary() ? ByteBuffer.wrap(new byte[0]) : "";
      case TType.LIST:
        return new ArrayList<>();
      case TType.SET:
        return new HashSet<>();
      case TType.MAP:
        return new HashMap<>();
      case TType.STRUCT:
        return minimum(((StructMetaData) meta).structClass);
      case TType.ENUM:
        return ((EnumMetaData) meta).enumClass.getEnumConstants()[0];
      default:
        throw new AssertionError("Unmapped Thrift type " + meta.type);
    }
  }

  static Object request(Method method, long session, String sql) throws Exception {
    Object req = minimum(method.getParameterTypes()[0]);
    if (!method.getName().equals("debugInfo"))
      req.getClass().getMethod("setSessionId", long.class).invoke(req, session);
    if (Arrays.asList("executeSql", "executeStatement", "loadCSV", "loadUDF")
        .contains(method.getName()))
      req.getClass().getMethod("setStatement", String.class).invoke(req, sql);
    return req;
  }

  static String snapshot(Connection owner) throws Exception {
    return owner.execute("SELECT * FROM *;").toString()
        + owner.client.getUser(new GetUserReq(owner.id)).toString()
        + owner.client.getClusterInfo(new GetClusterInfoReq(owner.id)).toString()
        + owner.client.getRegisterTaskInfo(new GetRegisterTaskInfoReq(owner.id)).toString();
  }

  @Test
  public void igt10_unopenedSessionsEveryRpc() throws Exception {
    long random = new java.security.SecureRandom().nextLong();
    Connection closed = node.connect("matrix_read", "password");
    ok(closed.client.closeSession(new CloseSessionReq(closed.id)));
    String before = snapshot(root);
    int methods = 0;
    for (Method method : IService.Iface.class.getMethods()) {
      if (method.getName().equals("openSession")) continue;
      methods++;
      for (long id : new long[] {0, random, closed.id}) {
        for (String sql :
            new String[] {
              "SELECT * FROM business;", "INSERT INTO business (key,denied) VALUES (9,9);"
            }) {
          try (Connection wire = node.raw()) {
            Object result = method.invoke(wire.client, request(method, id, sql));
            assertEquals(method.getName() + " session=" + id, 101, status(result).code);
          }
        }
      }
    }
    assertEquals(40, methods);
    assertEquals(before, snapshot(root));
    OpenSessionResp bad =
        root.client.openSession(new OpenSessionReq().setUsername("missing").setPassword("wrong"));
    assertEquals(0, bad.sessionId);
    assertEquals(
        cn.edu.tsinghua.iginx.utils.RpcUtils.WRONG_USERNAME_OR_PASSWORD.code, bad.status.code);
  }

  static void upload(Connection c, String name, byte[] bytes) throws Exception {
    ok(
        c.client.uploadFileChunk(
            new UploadFileReq(c.id, new FileChunk(name, 0, ByteBuffer.wrap(bytes), bytes.length))));
  }

  @Test
  public void igt12_compositeAndUploadOwnership() throws Exception {
    String before = snapshot(root);
    denied(write.sql("INSERT INTO output (key,a) VALUES (SELECT a FROM business);").status);
    read.execute("EXPLAIN SELECT * FROM business;");
    Path export = node.home.resolve("read-export.csv");
    ExecuteSqlResp exported =
        read.execute(
            "SELECT * FROM business INTO OUTFILE \""
                + export.toString().replace('\\', '/')
                + "\" AS CSV;");
    // Export is materialized by the client; the server returns the authorized data and export type.
    assertEquals(SqlType.ExportCsv, exported.type);
    assertNotNull(exported.queryDataSet);
    denied(read.sql("LOAD DATA FROM INFILE \"never.csv\" AS CSV INTO output;").status);
    denied(
        read.client.uploadFileChunk(
            new UploadFileReq(
                read.id, new FileChunk("read.csv", 0, ByteBuffer.wrap(new byte[] {1}), 1))));
    denied(
        write.client.loadCSV(new LoadCSVReq(write.id, "SELECT * FROM business;", "unused.csv"))
            .status);
    denied(
        read.client.loadUDF(
                new LoadUDFReq()
                    .setSessionId(read.id)
                    .setStatement("INSERT INTO business (key,denied) VALUES (9,9);")
                    .setIsRemote(false))
            .status);
    String filename = "owned-" + UUID.randomUUID() + ".csv";
    byte[] original = "key,a\n10,77\n".getBytes(StandardCharsets.UTF_8);
    upload(write, filename, original);
    Connection other = node.connect("matrix_write", "password");
    denied(
        other.client.uploadFileChunk(
            new UploadFileReq(
                other.id, new FileChunk(filename, 0, ByteBuffer.wrap(new byte[] {99}), 1))));
    byte[] security = Files.readAllBytes(node.home.resolve("security.properties"));
    for (String invalid :
        Arrays.asList(
            "../security.properties",
            "conf/security.properties",
            "conf\\security.properties",
            node.home.resolve("security.properties").toString(),
            filename)) {
      LoadCSVResp r =
          other.client.loadCSV(
              new LoadCSVReq(
                  other.id, "LOAD DATA FROM INFILE \"source.csv\" AS CSV INTO output;", invalid));
      denied(r.status);
      assertFalse(r.toString().contains(SECRET));
    }
    assertArrayEquals(security, Files.readAllBytes(node.home.resolve("security.properties")));
    assertEquals(before, snapshot(root));
    ok(
        write.client.loadCSV(
                new LoadCSVReq(
                    write.id, "LOAD DATA FROM INFILE \"source.csv\" AS CSV INTO output;", filename))
            .status);
    ExecuteSqlResp imported = root.execute("SELECT a FROM output WHERE key=10;");
    assertEquals(1, imported.getPathsSize());
    assertEquals(
        77L,
        ((Number)
                cn.edu.tsinghua.iginx.utils.ByteUtils.getValuesFromBufferAndBitmaps(
                        imported.dataTypeList,
                        imported.queryDataSet.valuesList,
                        imported.queryDataSet.bitmapList)
                    .get(0)
                    .get(0))
            .longValue());
  }

  @Test
  public void igt13_queryOwnershipAndClose() throws Exception {
    Connection a = node.connect("matrix_read", "password"),
        b = node.connect("matrix_read", "password");
    ExecuteStatementResp first =
        a.client.executeStatement(
            new ExecuteStatementReq(a.id, "SELECT * FROM business;").setFetchSize(1));
    ok(first.status);
    denied(b.client.fetchResults(new FetchResultsReq(b.id, first.queryId).setFetchSize(1)).status);
    denied(b.client.closeStatement(new CloseStatementReq(b.id, first.queryId)));
    denied(root.client.closeSession(new CloseSessionReq(0)));
    denied(
        root.client.closeSession(new CloseSessionReq(new java.security.SecureRandom().nextLong())));
    int rows = first.getQueryDataSet().getValuesListSize();
    while (true) {
      FetchResultsResp next =
          a.client.fetchResults(new FetchResultsReq(a.id, first.queryId).setFetchSize(1));
      ok(next.status);
      rows += next.getQueryDataSet().getValuesListSize();
      if (!next.isHasMoreResults()) break;
    }
    assertEquals(3, rows);
    ok(a.client.closeSession(new CloseSessionReq(a.id)));
    denied(a.client.fetchResults(new FetchResultsReq(a.id, first.queryId).setFetchSize(1)).status);
  }

  @Test
  public void igt14_sessionListingRequiresAdmin() throws Exception {
    for (Connection c : Arrays.asList(read, write)) {
      denied(c.sql("SHOW SESSIONID;").status);
      denied(c.client.showSessionID(new ShowSessionIDReq(c.id)).status);
    }
    admin.execute("SHOW SESSIONID;");
    ok(admin.client.showSessionID(new ShowSessionIDReq(admin.id)).status);
  }

  @Test
  public void igt15_configAndSecrets() throws Exception {
    String original = admin.execute("SHOW CONFIG \"UDFTimeout\";").toString();
    for (Connection c : Arrays.asList(read, write))
      for (String sql :
          Arrays.asList(
              "SHOW CONFIG;", "SHOW CONFIG \"password\";", "SET CONFIG \"UDFTimeout\" \"12345\";"))
        denied(c.sql(sql).status);
    assertEquals(original, admin.execute("SHOW CONFIG \"UDFTimeout\";").toString());
    admin.execute("SET CONFIG \"UDFTimeout\" \"12345\";");
    assertTrue(admin.execute("SHOW CONFIG \"UDFTimeout\";").toString().contains("12345"));
    assertFalse(admin.execute("SHOW CONFIG;").toString().contains(SECRET));
    for (String key :
        Arrays.asList(
            "reserved.prefixes", "zookeeper.password", "etcd.password", "internal.secret")) {
      assertNotEquals(200, admin.sql("SET CONFIG \"" + key + "\" \"x\";").status.code);
      for (java.lang.reflect.Field f : cn.edu.tsinghua.iginx.conf.Config.class.getDeclaredFields())
        assertNotEquals(key, f.getName());
    }
    assertFalse(node.logs().contains(SECRET));
  }
}
