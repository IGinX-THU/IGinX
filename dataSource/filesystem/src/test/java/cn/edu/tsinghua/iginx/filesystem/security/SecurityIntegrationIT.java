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

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.auth.SessionManager;
import cn.edu.tsinghua.iginx.engine.ContextBuilder;
import cn.edu.tsinghua.iginx.engine.StatementExecutor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.filesystem.service.rpc.InternalAuth;
import cn.edu.tsinghua.iginx.filesystem.thrift.*;
import cn.edu.tsinghua.iginx.rest.*;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.io.*;
import java.lang.reflect.Method;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.zookeeper.server.*;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.threadpool.ThreadPoolConfig;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.*;

/**
 * Real worker, FileSystem RPC, ZooKeeper and HTTP, in an isolated fork and temporary directories.
 */
public class SecurityIntegrationIT {
  private static ZooKeeperServer zk;
  private static ServerCnxnFactory factory;
  private static HttpServer http;
  private static IginxWorker worker;
  private static long root, service, reader, writer;
  private static int fsPort, httpPort;
  private static final String SECRET = "integration-only-01234567890123456789";

  private static int port() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  @BeforeClass
  public static void start() throws Exception {
    Path home = Files.createTempDirectory("iginx-security-it-");
    zk = new ZooKeeperServer(home.resolve("snap").toFile(), home.resolve("log").toFile(), 2000);
    factory = ServerCnxnFactory.createFactory(new InetSocketAddress("127.0.0.1", 0), 100);
    factory.startup(zk);
    fsPort = port();
    httpPort = port();
    int rpcPort = port();
    Properties config = new Properties();
    config.setProperty("ip", "127.0.0.1");
    config.setProperty("port", String.valueOf(rpcPort));
    config.setProperty("zookeeperConnectionString", "127.0.0.1:" + factory.getLocalPort());
    config.setProperty(
        "storageEngineList",
        "127.0.0.1#"
            + fsPort
            + "#filesystem#iginx_port="
            + rpcPort
            + "#dir="
            + home.resolve("data").toString().replace('\\', '/'));
    config.setProperty("replicaNum", "0");
    config.setProperty("queryOptimizer", "rbo");
    config.setProperty("useStreamExecutor", "false");
    config.setProperty("pythonCMD", "python");
    config.setProperty("needInitBasicUDFFunctions", "false");
    config.setProperty("enableMemoryControl", "false");
    config.setProperty("memoryTaskThreadPoolSize", "4");
    config.setProperty("physicalTaskThreadPoolSizePerStorage", "4");
    config.setProperty("defaultUDFDir", home.resolve("udf").toString());
    config.setProperty("defaultScheduledTransformJobDir", home.resolve("jobs").toString());
    try (OutputStream out = Files.newOutputStream(home.resolve("config.properties"))) {
      config.store(out, "isolated integration test");
    }
    Path security = home.resolve("security.properties");
    Files.write(
        security,
        ("zookeeper.user=meta\nzookeeper.password="
                + SECRET
                + "\nzookeeper.acl=creator\ninternal.secret="
                + SECRET
                + "\ninternal.auth=required\nreserved.prefixes=indagent,bigfile,sys.blob\nreserved.accounts=indagent_data_*\n")
            .getBytes(StandardCharsets.UTF_8));
    if (Files.getFileStore(security).supportsFileAttributeView("posix"))
      Files.setPosixFilePermissions(
          security, EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
    System.setProperty("IGINX_HOME", home.toString());
    System.setProperty("IGINX_CONF", home.resolve("config.properties").toString());
    Files.createDirectories(home.resolve("driver/filesystem"));
    System.setProperty("IGINX_DRIVER", home.resolve("driver").toString());
    worker = IginxWorker.getInstance();
    root = login("root", "root");
    createUser("indagent_data_test", EnumSet.allOf(AuthType.class));
    service = login("indagent_data_test", "password");
    createUser("reader", EnumSet.of(AuthType.Read));
    reader = login("reader", "password");
    createUser("writer", EnumSet.of(AuthType.Write));
    writer = login("writer", "password");
    assertOk(sql(service, "INSERT INTO indagent (key, secret) VALUES (1, 99);"));
    assertOk(sql(service, "INSERT INTO bigfile (key, secret) VALUES (1, 88);"));
    assertOk(sql(service, "INSERT INTO sys.blob (key, secret) VALUES (1, 77);"));
    assertOk(sql(root, "INSERT INTO business (key, value) VALUES (1, 7);"));
    http =
        GrizzlyHttpServerFactory.createHttpServer(
            new URI("http://127.0.0.1:" + httpPort + "/"),
            new ResourceConfig()
                .register(MetricsResource.class)
                .register(RestAuthenticationFilter.class),
            false);
    http.getListeners()
        .forEach(
            listener -> {
              listener.getTransport().setSelectorRunnersCount(2);
              listener
                  .getTransport()
                  .setWorkerThreadPoolConfig(
                      ThreadPoolConfig.defaultConfig().setCorePoolSize(2).setMaxPoolSize(8));
            });
    http.start();
  }

  @AfterClass
  public static void stop() {
    if (http != null) http.shutdownNow();
    if (factory != null) factory.shutdown();
    if (zk != null) zk.shutdown();
  }

  private static void createUser(String name, Set<AuthType> auth) {
    assertOk(
        worker.addUser(
            new AddUserReq()
                .setSessionId(root)
                .setUsername(name)
                .setPassword("password")
                .setAuths(auth)));
  }

  private static long login(String name, String password) {
    OpenSessionResp resp =
        worker.openSession(new OpenSessionReq().setUsername(name).setPassword(password));
    assertOk(resp.getStatus());
    assertNotEquals(0, resp.getSessionId());
    return resp.getSessionId();
  }

  private static Status sql(long id, String sql) {
    return worker.executeSql(new ExecuteSqlReq().setSessionId(id).setStatement(sql)).getStatus();
  }

  private static void assertOk(Status status) {
    assertEquals(status.toString(), RpcUtils.SUCCESS.code, status.code);
  }

  private static void assertDenied(Status status) {
    assertEquals(status.toString(), RpcUtils.ACCESS_DENY.code, status.code);
  }

  private static List<String> columns(long id, String pattern) {
    ShowColumnsResp response = worker.showColumns(new ShowColumnsReq().setSessionId(id));
    assertOk(response.getStatus());
    return response.getPaths();
  }

  @Test
  public void everyRpcRejectsForgedSessionBeforeReadingArguments() throws Exception {
    int count = 0;
    for (Method method : IService.Iface.class.getMethods()) {
      Object request = method.getParameterTypes()[0].newInstance();
      if (method.getName().equals("openSession")) {
        OpenSessionResp response =
            worker.openSession(new OpenSessionReq().setUsername("").setPassword(""));
        assertEquals(0, response.getSessionId());
        assertNotEquals(RpcUtils.SUCCESS.code, response.getStatus().code);
      } else {
        if (!method.getName().equals("debugInfo"))
          request.getClass().getMethod("setSessionId", long.class).invoke(request, 15L);
        Object response = method.invoke(worker, request);
        Status status =
            response instanceof Status
                ? (Status) response
                : (Status) response.getClass().getMethod("getStatus").invoke(response);
        assertEquals(method.getName() + ": " + status, RpcUtils.ACCESS_DENY.code, status.code);
      }
      count++;
    }
    assertEquals(41, count);
  }

  @Test
  public void pathsPermissionsAndRevocation() throws Exception {
    assertDenied(sql(reader, "INSERT INTO business (key, forbidden) VALUES (2, 5);"));
    assertDenied(sql(writer, "SELECT * FROM business;"));
    for (long id : new long[] {root, reader, writer}) {
      assertDenied(sql(id, "SELECT * FROM indagent;"));
      assertDenied(sql(id, "INSERT INTO indagent (key, forbidden) VALUES (2, 5);"));
    }
    assertOk(sql(reader, "SELECT * FROM *;"));
    assertOk(sql(reader, "SELECT count(*) FROM *;"));
    List<String> visible = columns(reader, "*");
    assertTrue(visible.contains("business.value"));
    assertFalse(
        visible.toString(),
        visible.stream()
            .anyMatch(
                p ->
                    p.startsWith("indagent.")
                        || p.startsWith("bigfile.")
                        || p.startsWith("sys.blob.")));
    assertTrue(columns(service, "*").contains("indagent.secret"));
    assertOk(sql(root, "INSERT INTO indagent2 (key, allowed) VALUES (1, 3);"));
    assertTrue(columns(reader, "in*").contains("indagent2.allowed"));
    assertDenied(
        worker.deleteColumns(
            new DeleteColumnsReq()
                .setSessionId(writer)
                .setPaths(Arrays.asList("business.value", "indagent.secret"))));
    assertTrue(columns(reader, "*").contains("business.value"));
    createUser("revoked", EnumSet.of(AuthType.Read));
    long old = login("revoked", "password");
    assertOk(
        worker.updateUser(
            new UpdateUserReq().setSessionId(root).setUsername("revoked").setPassword("changed")));
    assertDenied(worker.getReplicaNum(new GetReplicaNumReq().setSessionId(old)).getStatus());
    assertNotEquals(old, login("revoked", "changed"));
    assertOk(worker.getReplicaNum(new GetReplicaNumReq().setSessionId(reader)).getStatus());
  }

  @Test
  public void queryOwnershipAndUploadTraversal() throws Exception {
    ExecuteStatementResp response =
        worker.executeStatement(
            new ExecuteStatementReq()
                .setSessionId(reader)
                .setStatement("SELECT * FROM business;")
                .setFetchSize(1));
    assertOk(response.getStatus());
    assertDenied(
        worker
            .fetchResults(
                new FetchResultsReq()
                    .setSessionId(root)
                    .setQueryId(response.getQueryId())
                    .setFetchSize(1))
            .getStatus());
    assertDenied(
        worker.closeStatement(
            new CloseStatementReq().setSessionId(root).setQueryId(response.getQueryId())));
    assertOk(
        worker.closeStatement(
            new CloseStatementReq().setSessionId(reader).setQueryId(response.getQueryId())));
    assertDenied(
        worker.uploadFileChunk(
            new UploadFileReq()
                .setSessionId(writer)
                .setFileChunk(
                    new FileChunk()
                        .setFileName("../config.properties")
                        .setOffset(0)
                        .setData(new byte[] {1})
                        .setChunkSize(1))));
  }

  private static int request(String path, String auth, String body) throws Exception {
    HttpURLConnection conn =
        (HttpURLConnection) new URL("http://127.0.0.1:" + httpPort + path).openConnection();
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(10000);
    if (auth != null)
      conn.setRequestProperty(
          "Authorization",
          "Basic " + Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8)));
    if (body != null) {
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/json");
      try (OutputStream out = conn.getOutputStream()) {
        out.write(body.getBytes(StandardCharsets.UTF_8));
      }
    }
    try {
      return conn.getResponseCode();
    } finally {
      conn.disconnect();
    }
  }

  @Test
  public void runtimePathsAndCsvHeadersCannotBypassProtection() {
    assertOk(sql(root, "INSERT INTO dynamic_paths (key, p) VALUES (1, \"indagent.secret\");"));
    assertDenied(
        sql(reader, "SELECT VALUE2META(SELECT p FROM dynamic_paths) FROM business, sys.other;"));
    assertOk(
        sql(service, "SELECT VALUE2META(SELECT p FROM dynamic_paths) FROM business, sys.other;"));
    assertOk(sql(root, "INSERT INTO wildcard_paths (key, p) VALUES (1, \"*\");"));
    ExecuteSqlResp wildcard =
        worker.executeSql(
            new ExecuteSqlReq(reader, "SELECT VALUE2META(SELECT p FROM wildcard_paths) FROM *;"));
    assertOk(wildcard.getStatus());
    assertTrue(
        wildcard.getPaths().stream()
            .noneMatch(
                p ->
                    p.startsWith("indagent.")
                        || p.startsWith("bigfile.")
                        || p.startsWith("sys.blob.")));
    byte[] csv =
        "key,other.csv_allowed,blob.csv_denied\n1,41,42\n".getBytes(StandardCharsets.UTF_8);
    String filename = "reserved-header.csv";
    assertOk(
        worker.uploadFileChunk(
            new UploadFileReq()
                .setSessionId(writer)
                .setFileChunk(
                    new FileChunk()
                        .setFileName(filename)
                        .setOffset(0)
                        .setData(csv)
                        .setChunkSize(csv.length))));
    assertDenied(
        worker
            .loadCSV(
                new LoadCSVReq(
                    writer, "LOAD DATA FROM INFILE \"source.csv\" AS CSV INTO sys;", filename))
            .getStatus());
    assertFalse(columns(service, "*").contains("sys.other_csv_allowed"));
    assertFalse(columns(service, "*").contains("sys.blob_csv_denied"));
    assertDenied(
        worker
            .loadCSV(
                new LoadCSVReq(
                    service, "LOAD DATA FROM INFILE \"source.csv\" AS CSV INTO sys;", filename))
            .getStatus());
    cn.edu.tsinghua.iginx.auth.UploadRegistry.getInstance().releaseSession(writer);
    assertOk(
        worker.uploadFileChunk(
            new UploadFileReq()
                .setSessionId(service)
                .setFileChunk(
                    new FileChunk()
                        .setFileName(filename)
                        .setOffset(0)
                        .setData(csv)
                        .setChunkSize(csv.length))));
    assertOk(
        worker
            .loadCSV(
                new LoadCSVReq(
                    service, "LOAD DATA FROM INFILE \"source.csv\" AS CSV INTO sys;", filename))
            .getStatus());
    assertTrue(columns(service, "*").contains("sys.blob_csv_denied"));
  }

  @Test
  public void internalExecutionKeepsReservedPathsProtected() {
    for (String sql :
        Arrays.asList(
            "INSERT INTO internal_temp (key, a) VALUES (1, 8);",
            "SELECT * FROM internal_temp;",
            "DELETE COLUMNS internal_temp.*;")) {
      RequestContext context = ContextBuilder.getInstance().build(new ExecuteSqlReq(0, sql));
      context.setInternal(true);
      StatementExecutor.getInstance().execute(context);
      assertOk(context.getResult().getStatus());
    }
    for (String sql :
        Arrays.asList(
            "SELECT * FROM indagent;",
            "INSERT INTO indagent (key, forbidden_internal) VALUES (1, 8);",
            "DELETE COLUMNS indagent.*;")) {
      RequestContext context = ContextBuilder.getInstance().build(new ExecuteSqlReq(service, sql));
      context.setInternal(true);
      StatementExecutor.getInstance().execute(context);
      assertDenied(context.getResult().getStatus());
      assertEquals("Reserved path access denied", context.getResult().getStatus().getMessage());
    }
    assertDenied(sql(0, "SELECT * FROM business;"));
    assertFalse(columns(service, "*").contains("indagent.forbidden_internal"));
  }

  @Test
  public void restAuthenticatesFallbacksAndAsyncWrites() throws Exception {
    int before = SessionManager.getInstance().getSessionIds().size();
    for (String path : Arrays.asList("/", "/unknown", "/api/v1/datapoints/query", "/annotations"))
      assertEquals(path, 401, request(path, null, null));
    assertEquals(401, request("/", "reader:wrong", null));
    assertEquals(200, request("/", "reader:password", null));
    String metric = "{\"name\":\"rest.allowed\",\"tags\":{},\"datapoints\":[[1,9]]}";
    assertEquals(403, request("/api/v1/datapoints", "reader:password", metric));
    assertEquals(200, request("/api/v1/datapoints", "writer:password", metric));
    assertEquals(
        403,
        request(
            "/api/v1/datapoints", "root:root", metric.replace("rest.allowed", "indagent.rest")));
    assertEquals(
        200,
        request(
            "/api/v1/datapoints",
            "indagent_data_test:password",
            metric.replace("rest.allowed", "indagent.rest")));
    assertEquals(before, SessionManager.getInstance().getSessionIds().size());
  }

  @Test
  public void filesystemRequiredHmacRejectsAllUnsignedMethods() throws Exception {
    try (TSocket socket = new TSocket("127.0.0.1", fsPort, 5000)) {
      socket.open();
      FileSystemRpc.Client client = new FileSystemRpc.Client(new TBinaryProtocol(socket));
      RawPrefix prefix = new RawPrefix().setPrefix("");
      for (int method = 0; method < 4; method++) {
        try {
          switch (method) {
            case 0:
              client.getUnits(prefix);
              break;
            case 1:
              client.query(new DataUnit(false), new RawDataTarget(), new RawAggregate());
              break;
            case 2:
              client.delete(new DataUnit(false), new RawDataTarget());
              break;
            default:
              client.insert(
                  new DataUnit(false),
                  new RawInserted()
                      .setPatterns(Collections.emptyList())
                      .setTagsList(Collections.emptyList())
                      .setKeys(new byte[0])
                      .setValuesList(Collections.emptyList())
                      .setBitmapList(Collections.emptyList())
                      .setDataTypeList(Collections.emptyList())
                      .setRawDataType("Row"));
          }
          fail("unsigned method " + method + " accepted");
        } catch (RpcException expected) {
          assertTrue(expected.getMessage(), expected.getMessage().contains("authentication"));
        }
      }
      prefix.setCredential(InternalAuth.sign(SECRET, "getUnits", prefix));
      assertNotNull(client.getUnits(prefix));
      prefix.setPrefix("tampered");
      try {
        client.getUnits(prefix);
        fail("tampered arguments accepted");
      } catch (RpcException expected) {
      }
    }
  }

  @Test(timeout = 30000)
  public void mqttUsesCallerAndRejectsSubscriptionsAndRevokedConnections() throws Exception {
    cn.edu.tsinghua.iginx.conf.Config config =
        cn.edu.tsinghua.iginx.conf.ConfigDescriptor.getInstance().getConfig();
    int mqttPort = port();
    config.setMqttHost("127.0.0.1");
    config.setMqttPort(mqttPort);
    int before = SessionManager.getInstance().getSessionIds().size();
    cn.edu.tsinghua.iginx.mqtt.MQTTService broker =
        cn.edu.tsinghua.iginx.mqtt.MQTTService.getInstance();
    broker.start();
    try {
      try (MqttWire denied = new MqttWire(mqttPort, "bad", "reader", "wrong")) {
        assertNotEquals(0, denied.code);
      }
      try (MqttWire read = new MqttWire(mqttPort, "read", "reader", "password");
          MqttWire write = new MqttWire(mqttPort, "write", "writer", "password");
          MqttWire allowed = new MqttWire(mqttPort, "service", "indagent_data_test", "password")) {
        assertEquals(0, read.code);
        assertEquals(0, write.code);
        assertEquals(0, allowed.code);
        assertEquals(128, allowed.subscribe("#"));
        assertEquals(128, read.subscribe("events"));
        read.publish("mqtt.forbidden_read");
        write.publish("mqtt.allowed");
        write.publish("indagent.mqtt_forbidden");
        allowed.publish("indagent.mqtt_allowed");
        awaitColumn("mqtt.allowed");
        awaitColumn("indagent.mqtt_allowed");
        assertFalse(columns(service, "*").contains("mqtt.forbidden_read"));
        assertFalse(columns(service, "*").contains("indagent.mqtt_forbidden"));
        read.disconnect();
      }
      createUser("mqtt_revoked", EnumSet.of(AuthType.Write));
      try (MqttWire connection = new MqttWire(mqttPort, "revoked", "mqtt_revoked", "password")) {
        assertEquals(0, connection.code);
        connection.publish("mqtt.before_revoke");
        awaitColumn("mqtt.before_revoke");
        assertOk(
            worker.updateUser(
                new UpdateUserReq()
                    .setSessionId(root)
                    .setUsername("mqtt_revoked")
                    .setPassword("changed")));
        connection.publish("mqtt.after_revoke");
        Thread.sleep(300);
        assertFalse(columns(service, "*").contains("mqtt.after_revoke"));
      }
      try (MqttWire connection = new MqttWire(mqttPort, "reconnected", "mqtt_revoked", "changed")) {
        assertEquals(0, connection.code);
        connection.publish("mqtt.after_reconnect");
        awaitColumn("mqtt.after_reconnect");
      }
      long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
      while (SessionManager.getInstance().getSessionIds().size() != before
          && System.nanoTime() < deadline) Thread.sleep(20);
      assertEquals(before, SessionManager.getInstance().getSessionIds().size());
    } finally {
      broker.shutdown();
    }
  }

  private static void awaitColumn(String path) throws Exception {
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
    do {
      if (columns(service, "*").contains(path)) return;
      Thread.sleep(20);
    } while (System.nanoTime() < deadline);
    fail("Publication missing: " + path);
  }

  /** Minimal MQTT 3.1.1 wire client keeps protocol tests independent of a new client dependency. */
  private static final class MqttWire implements AutoCloseable {
    final Socket socket;
    final int code;

    MqttWire(int port, String client, String user, String password) throws Exception {
      socket = new Socket("127.0.0.1", port);
      socket.setSoTimeout(5000);
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      DataOutputStream data = new DataOutputStream(buffer);
      data.writeUTF("MQTT");
      data.writeByte(4);
      data.writeByte(0xc2);
      data.writeShort(30);
      data.writeUTF(client);
      data.writeUTF(user);
      data.writeUTF(password);
      send(0x10, buffer.toByteArray());
      byte[] connack = read(0x20);
      code = connack[1] & 255;
    }

    private void send(int header, byte[] body) throws Exception {
      OutputStream out = socket.getOutputStream();
      out.write(header);
      int remaining = body.length;
      do {
        int digit = remaining % 128;
        remaining /= 128;
        out.write(remaining > 0 ? digit | 128 : digit);
      } while (remaining > 0);
      out.write(body);
      out.flush();
    }

    private byte[] read(int expected) throws Exception {
      DataInputStream in = new DataInputStream(socket.getInputStream());
      assertEquals(expected, in.readUnsignedByte());
      int length = 0, multiplier = 1, digit;
      do {
        digit = in.readUnsignedByte();
        length += (digit & 127) * multiplier;
        multiplier *= 128;
      } while ((digit & 128) != 0);
      byte[] body = new byte[length];
      in.readFully(body);
      return body;
    }

    int subscribe(String topic) throws Exception {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      DataOutputStream data = new DataOutputStream(buffer);
      data.writeShort(1);
      data.writeUTF(topic);
      data.writeByte(0);
      send(0x82, buffer.toByteArray());
      return read(0x90)[2] & 255;
    }

    void publish(String path) throws Exception {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      DataOutputStream data = new DataOutputStream(buffer);
      data.writeUTF("events");
      data.write(
          ("[{\"path\":\"" + path + "\",\"timestamp\":1,\"dataType\":\"long\",\"value\":42}]")
              .getBytes(StandardCharsets.UTF_8));
      send(0x30, buffer.toByteArray());
    }

    void disconnect() throws Exception {
      send(0xe0, new byte[0]);
    }

    public void close() throws IOException {
      socket.close();
    }
  }

  @Test(timeout = 30000)
  public void wildcardsFilterBeforeAggregationPaginationAndDelete() throws Exception {
    for (String wildcard : Arrays.asList("*", "in*", "sys.*")) {
      QueryDataResp query =
          worker.queryData(
              new QueryDataReq()
                  .setSessionId(reader)
                  .setPaths(Arrays.asList(wildcard))
                  .setStartKey(0)
                  .setEndKey(10));
      assertOk(query.getStatus());
      checkVisible(query.getPaths());
      AggregateQueryResp aggregate =
          worker.aggregateQuery(
              new AggregateQueryReq()
                  .setSessionId(reader)
                  .setPaths(Arrays.asList(wildcard))
                  .setStartKey(0)
                  .setEndKey(10)
                  .setAggregateType(AggregateType.COUNT));
      assertOk(aggregate.getStatus());
      checkVisible(aggregate.getPaths());
      DownsampleQueryResp downsample =
          worker.downsampleQuery(
              new DownsampleQueryReq()
                  .setSessionId(reader)
                  .setPaths(Arrays.asList(wildcard))
                  .setStartKey(0)
                  .setEndKey(10)
                  .setAggregateType(AggregateType.COUNT)
                  .setPrecision(5));
      assertOk(downsample.getStatus());
      checkVisible(downsample.getPaths());
      LastQueryResp last =
          worker.lastQuery(
              new LastQueryReq()
                  .setSessionId(reader)
                  .setPaths(Arrays.asList(wildcard))
                  .setStartKey(0));
      assertOk(last.getStatus());
      checkVisible(last.getPaths());
    }
    assertOk(sql(root, "INSERT INTO ordinary (key, c0) VALUES (5, 3);"));
    assertOk(sql(service, "INSERT INTO indagent (key, c0) VALUES (5, 9);"));
    int count = columns(reader, "*").size();
    ExecuteSqlResp paged =
        worker.executeSql(
            new ExecuteSqlReq(reader, "SHOW COLUMNS LIMIT 10 OFFSET " + (count - 1) + ";"));
    assertOk(paged.getStatus());
    assertEquals(1, paged.getPathsSize());
    checkVisible(paged.getPaths());
    assertOk(
        worker.deleteColumns(
            new DeleteColumnsReq().setSessionId(writer).setPaths(Arrays.asList("*.c0"))));
    assertFalse(columns(reader, "*").contains("ordinary.c0"));
    assertTrue(columns(service, "*").contains("indagent.c0"));
    assertOk(
        worker.deleteColumns(
            new DeleteColumnsReq().setSessionId(writer).setPaths(Arrays.asList("sys.*"))));
    assertTrue(columns(service, "*").contains("sys.blob.secret"));
  }

  private static void checkVisible(List<String> paths) {
    if (paths != null)
      for (String path : paths)
        assertFalse(
            path,
            path.contains("indagent.") || path.contains("bigfile.") || path.contains("sys.blob."));
  }
}
