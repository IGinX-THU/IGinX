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

import cn.edu.tsinghua.iginx.rest.MetricsResource;
import cn.edu.tsinghua.iginx.thrift.*;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import org.junit.*;

public class RestSecurityAcceptanceIT {
  static SecurityTestEnvironment env;
  static SecurityTestEnvironment.Node node;
  static Connection service;
  static final String INSERT = "[{\"name\":\"business.a\",\"datapoints\":[[1,42]],\"tags\":{}}]";
  static final String QUERY =
      "{\"start_absolute\":0,\"end_absolute\":100,\"metrics\":[{\"name\":\"business.a\"}]}";

  static String auth(String name, String password) {
    return "Basic "
        + Base64.getEncoder()
            .encodeToString((name + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  static final class Response {
    final int code;
    final String body;

    Response(int code, String body) {
      this.code = code;
      this.body = body;
    }
  }

  static Response http(
      SecurityTestEnvironment.Node node,
      String method,
      String path,
      String authorization,
      String body)
      throws Exception {
    HttpURLConnection c =
        (HttpURLConnection) new URL("http://127.0.0.1:" + node.rest + path).openConnection();
    c.setConnectTimeout(3000);
    c.setReadTimeout(5000);
    c.setRequestMethod(method);
    if (authorization != null) c.setRequestProperty("Authorization", authorization);
    if (body != null) {
      c.setDoOutput(true);
      c.setRequestProperty("Content-Type", "application/json");
      try (OutputStream out = c.getOutputStream()) {
        out.write(body.getBytes(StandardCharsets.UTF_8));
      }
    }
    try {
      int code = c.getResponseCode();
      InputStream in = code < 400 ? c.getInputStream() : c.getErrorStream();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      if (in != null)
        try (InputStream input = in) {
          byte[] b = new byte[8192];
          int n;
          while ((n = input.read(b)) != -1) out.write(b, 0, n);
        }
      return new Response(code, new String(out.toByteArray(), StandardCharsets.UTF_8));
    } finally {
      c.disconnect();
    }
  }

  static void awaitMarker(java.nio.file.Path marker) throws Exception {
    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!Files.exists(marker) && System.nanoTime() < until) Thread.sleep(2);
    if (!Files.exists(marker)) fail("Missing query barrier " + marker + "\n" + node.logs());
  }

  static String annotation(String file) throws Exception {
    return new String(
        Files.readAllBytes(projectRoot().resolve("test/src/test/resources/restAnnotation/" + file)),
        StandardCharsets.UTF_8);
  }

  static final class Route {
    final String verb, path, body;
    final Set<AuthType> permissions;
    final int success;

    Route(String verb, String path, String body, int success, AuthType... permissions) {
      this.verb = verb;
      this.path = path;
      this.body = body;
      this.success = success;
      this.permissions = new HashSet<>(Arrays.asList(permissions));
    }
  }

  static List<Route> routes() throws Exception {
    return Arrays.asList(
        new Route("GET", "/", null, 200),
        new Route("GET", "/unknown", null, 404),
        new Route("POST", "/unknown", "{}", 404),
        new Route(
            "POST",
            "/query",
            "{\"range\":{\"from\":\"1970-01-01T00:00:00.000Z\",\"to\":\"1970-01-01T00:00:01.000Z\"},\"targets\":[{\"target\":\"business.a\",\"type\":\"timeserie\"}]}",
            200,
            AuthType.Read),
        new Route(
            "POST",
            "/annotations",
            "{\"range\":{\"from\":\"2013-02-01T00:00:00.000Z\",\"to\":\"2013-02-03T00:00:00.000Z\"},\"annotation\":{\"name\":\"archive_file_tracked.ann\",\"query\":{\"tags\":{},\"annotation\":{}}}}",
            200,
            AuthType.Read),
        new Route("POST", "/api/v1/datapoints", INSERT, 200, AuthType.Write),
        new Route(
            "POST",
            "/api/v1/datapoints/annotations",
            annotation("longType/insert.json"),
            200,
            AuthType.Write),
        new Route(
            "POST",
            "/api/v1/datapoints/annotations/add",
            annotation("common/add.json"),
            200,
            AuthType.Read,
            AuthType.Write),
        new Route(
            "POST",
            "/api/v1/datapoints/annotations/update",
            annotation("common/update.json"),
            200,
            AuthType.Read,
            AuthType.Write),
        new Route(
            "POST",
            "/api/v1/datapoints/annotations/delete",
            annotation("common/delete.json"),
            200,
            AuthType.Read,
            AuthType.Write),
        new Route("POST", "/api/v1/datapoints/query", QUERY, 200, AuthType.Read),
        new Route(
            "POST",
            "/api/v1/datapoints/query/annotations",
            annotation("common/queryAnno.json"),
            200,
            AuthType.Read),
        new Route(
            "POST",
            "/api/v1/datapoints/query/annotations/data",
            annotation("common/queryData.json"),
            200,
            AuthType.Read),
        new Route("POST", "/api/v1/datapoints/delete", QUERY, 200, AuthType.Write),
        new Route("DELETE", "/api/v1/metric/business.a", null, 200, AuthType.Write));
  }

  @BeforeClass
  public static void start() throws Exception {
    env = new SecurityTestEnvironment();
    node = env.node(0, secure("creator", "required"));
    node.mainClass = RestRevocationNode.class.getName();
    node.config.setProperty("enableRestService", "true");
    node.start();
    awaitMarker(node.home.resolve("rest-revocation-ready"));
    node.user("r", AuthType.Read);
    node.user("w", AuthType.Write);
    node.user("rw", AuthType.Read, AuthType.Write);
    node.user("a", AuthType.Admin);
    node.user("c", AuthType.Cluster);
    node.user("indagent_data_rest", AuthType.Read, AuthType.Write);
    service = node.connect("indagent_data_rest", "password");
  }

  @AfterClass
  public static void stop() throws Exception {
    if (env != null) env.close();
  }

  static void seed() throws Exception {
    service.execute("CLEAR DATA;");
    service.execute("INSERT INTO business (key,a) VALUES (1,42);");
    Response r =
        http(
            node,
            "POST",
            "/api/v1/datapoints/annotations",
            auth("rw", "password"),
            annotation("longType/insert.json"));
    assertEquals(r.body, 200, r.code);
  }

  @Test
  public void igt23_allDeclaredRoutesRejectMalformedCredentials() throws Exception {
    Set<String> actual = new HashSet<>(), expected = new HashSet<>();
    for (java.lang.reflect.Method m : MetricsResource.class.getDeclaredMethods()) {
      javax.ws.rs.Path path = m.getAnnotation(javax.ws.rs.Path.class);
      if (path == null) continue;
      String value = "/" + path.value();
      if (value.contains("metricName")) value = "/api/v1/metric/business.a";
      if (value.contains("string :")) value = "/unknown";
      String verb =
          m.isAnnotationPresent(javax.ws.rs.GET.class)
              ? "GET"
              : m.isAnnotationPresent(javax.ws.rs.DELETE.class) ? "DELETE" : "POST";
      actual.add(verb + " " + value);
    }
    for (Route route : routes()) expected.add(route.verb + " " + route.path);
    assertEquals(expected, actual);
    seed();
    String before = SqlSecurityAcceptanceIT.snapshot(node.admin);
    for (Route route : routes()) {
      String denial = null;
      for (String header :
          Arrays.asList(
              null,
              auth("missing", "password"),
              auth("r", "wrong"),
              "Bearer ignored",
              "Basic !!!",
              "Basic "
                  + Base64.getEncoder()
                      .encodeToString("missing-colon".getBytes(StandardCharsets.UTF_8)))) {
        Response r = http(node, route.verb, route.path, header, route.body);
        assertEquals(route.path + " " + r.body, 401, r.code);
        if (denial == null) denial = r.body;
        else assertEquals(denial, r.body);
      }
    }
    assertEquals(before, SqlSecurityAcceptanceIT.snapshot(node.admin));
  }

  @Test
  public void igt25_fiveRolesEveryRestOperation() throws Exception {
    Map<String, Set<AuthType>> accounts = new LinkedHashMap<>();
    accounts.put("r", EnumSet.of(AuthType.Read));
    accounts.put("w", EnumSet.of(AuthType.Write));
    accounts.put("rw", EnumSet.of(AuthType.Read, AuthType.Write));
    accounts.put("a", EnumSet.of(AuthType.Admin));
    accounts.put("c", EnumSet.of(AuthType.Cluster));
    List<Route> operations = new ArrayList<>(routes());
    operations.add(new Route("POST", "/api/v1/datapoints", "[]", 200, AuthType.Write));
    operations.add(
        new Route(
            "POST",
            "/api/v1/datapoints/query",
            "{\"start_absolute\":0,\"end_absolute\":100,\"metrics\":[]}",
            200,
            AuthType.Read));
    for (String type : Arrays.asList("doubleType", "binaryType"))
      operations.add(
          new Route(
              "POST",
              "/api/v1/datapoints/annotations",
              annotation(type + "/insert.json").replace("archive_file_tracked", "typed_" + type),
              200,
              AuthType.Write));
    operations.add(
        new Route(
            "POST",
            "/api/v1/datapoints/query",
            QUERY.replace(
                "\"business.a\"",
                "\"business.a\",\"aggregators\":[{\"name\":\"save_as\",\"metric_name\":\"saved.a\"}]"),
            200,
            AuthType.Read,
            AuthType.Write));
    for (Map.Entry<String, Set<AuthType>> user : accounts.entrySet())
      for (Route route : operations) {
        seed();
        String before = SqlSecurityAcceptanceIT.snapshot(node.admin);
        Response response =
            http(node, route.verb, route.path, auth(user.getKey(), "password"), route.body);
        boolean permitted = user.getValue().containsAll(route.permissions);
        assertEquals(
            user.getKey() + " " + route.path + " " + response.body,
            permitted ? route.success : 403,
            response.code);
        if (!permitted) assertEquals(before, SqlSecurityAcceptanceIT.snapshot(node.admin));
        if (permitted && route.body != null && route.body.contains("typed_")) {
          String type = route.body.contains("typed_doubleType") ? "doubleType" : "binaryType";
          Response roundTrip =
              http(
                  node,
                  "POST",
                  "/api/v1/datapoints/query/annotations",
                  auth("rw", "password"),
                  annotation("common/queryAnno.json")
                      .replace("archive_file_tracked", "typed_" + type));
          assertEquals(roundTrip.body, 200, roundTrip.code);
          assertTrue(roundTrip.body, roundTrip.body.contains("title1"));
          assertTrue(roundTrip.body, roundTrip.body.contains("dsp1"));
        }
        try (Connection c = node.connect(user.getKey(), "password")) {
          if (route.permissions.contains(AuthType.Read)) {
            Status s =
                c.client.queryData(new QueryDataReq(c.id, Arrays.asList("business.a"), 0, 100))
                    .status;
            if (user.getValue().contains(AuthType.Read)) ok(s);
            else denied(s);
          }
          if (route.permissions.contains(AuthType.Write)) {
            Status s =
                c.client.deleteDataInColumns(
                    new DeleteDataInColumnsReq(c.id, Arrays.asList("business.a"), 99, 100));
            if (user.getValue().contains(AuthType.Write)) ok(s);
            else denied(s);
          }
        }
      }
  }

  @Test(timeout = 180000)
  public void igt26_reservedPathsAndHundredRevocations() throws Exception {
    seed();
    service.execute("INSERT INTO indagent.t (key,a) VALUES (1,777);");
    String before = service.execute("SELECT * FROM indagent.t;").toString();
    for (String user : Arrays.asList("rw", "indagent_data_rest"))
      for (Route route :
          Arrays.asList(
              new Route(
                  "POST", "/api/v1/datapoints", INSERT.replace("business.a", "indagent.t.a"), 200),
              new Route(
                  "POST",
                  "/api/v1/datapoints/query",
                  QUERY.replace("business.a", "indagent.t.a"),
                  200),
              new Route(
                  "POST",
                  "/api/v1/datapoints/delete",
                  QUERY.replace("business.a", "indagent.t.a"),
                  200))) {
        Response r = http(node, route.verb, route.path, auth(user, "password"), route.body);
        assertEquals(r.body, user.equals("rw") ? 403 : 200, r.code);
        if (user.equals("rw"))
          assertEquals(before, service.execute("SELECT * FROM indagent.t;").toString());
      }
    service.execute("INSERT INTO indagent.t (key,a) VALUES (1,777);");
    Response wildcard =
        http(
            node,
            "POST",
            "/api/v1/datapoints/query",
            auth("rw", "password"),
            QUERY.replace("business.a", "*"));
    assertEquals(wildcard.body, 200, wildcard.code);
    assertFalse(wildcard.body.contains("indagent"));
    node.user("revoked_rest", AuthType.Read);
    ok(
        node.admin.client.updateUser(
            new UpdateUserReq()
                .setSessionId(node.admin.id)
                .setUsername("revoked_rest")
                .setPassword("changed")));
    for (int i = 0; i < 10; i++)
      assertEquals(
          401,
          http(node, "POST", "/api/v1/datapoints/query", auth("revoked_rest", "password"), QUERY)
              .code);
    ok(node.admin.client.deleteUser(new DeleteUserReq(node.admin.id, "revoked_rest")));
    StringBuilder insert = new StringBuilder("INSERT INTO longquery (key,a) VALUES ");
    for (int i = 1; i <= 10000; i++) {
      if (i > 1) insert.append(',');
      insert.append('(').append(i).append(",42)");
    }
    service.execute(insert.append(';').toString());
    String longQuery = QUERY.replace("business.a", "longquery.a").replace("100,", "20000,");
    for (int i = 0; i < 10; i++)
      assertEquals(
          200,
          http(node, "POST", "/api/v1/datapoints/query", auth("rw", "password"), longQuery).code);
    int threads = node.workerThreads(),
        sessions =
            node.admin
                .client
                .showSessionID(new ShowSessionIDReq(node.admin.id))
                .getSessionIDListSize();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      for (int i = 0; i < 100; i++) {
        node.user("revoked_rest", AuthType.Read);
        java.nio.file.Path barrier = Files.createDirectory(node.home.resolve("rest-barrier-" + i));
        java.nio.file.Path command = node.home.resolve("rest-revocation-next");
        Files.write(command, barrier.getFileName().toString().getBytes(StandardCharsets.UTF_8));
        Files.move(
            command,
            node.home.resolve("rest-revocation-command"),
            java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        awaitMarker(barrier.resolve("armed"));
        Future<Response> query =
            executor.submit(
                () ->
                    http(
                        node,
                        "POST",
                        "/api/v1/datapoints/query",
                        auth("revoked_rest", "password"),
                        longQuery));
        long begin;
        try {
          awaitMarker(barrier.resolve("entered"));
          assertFalse("Query completed before revocation", query.isDone());
          begin = System.nanoTime();
          ok(node.admin.client.deleteUser(new DeleteUserReq(node.admin.id, "revoked_rest")));
        } finally {
          Files.write(barrier.resolve("release"), new byte[0]);
        }
        Response completed = query.get(5, TimeUnit.SECONDS);
        assertTrue(
            "Queued revoked query unexpectedly succeeded: " + completed.code,
            Arrays.asList(400, 401, 403).contains(completed.code));
        assertTrue(System.nanoTime() - begin < TimeUnit.SECONDS.toNanos(5));
        // IGT-26 allows at most five seconds for deletion to converge. Once denied,
        // subsequent requests must remain denied, including delayed watch events.
        long deadline = begin + TimeUnit.SECONDS.toNanos(5);
        int status;
        do {
          status =
              http(
                      node,
                      "POST",
                      "/api/v1/datapoints/query",
                      auth("revoked_rest", "password"),
                      longQuery)
                  .code;
          if (status == 401) break;
          assertTrue(
              "Deletion did not converge in five seconds: " + status, System.nanoTime() < deadline);
          Thread.sleep(10);
        } while (true);
        assertTrue("Revocation exceeded five seconds", System.nanoTime() < deadline);
        awaitMarker(barrier.resolve("finished"));
        for (int check = 0; check < 3; check++)
          assertEquals(
              "Revoked account was restored by a delayed event",
              401,
              http(
                      node,
                      "POST",
                      "/api/v1/datapoints/query",
                      auth("revoked_rest", "password"),
                      longQuery)
                  .code);
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
    assertEquals(
        sessions,
        node.admin
            .client
            .showSessionID(new ShowSessionIDReq(node.admin.id))
            .getSessionIDListSize());
    assertTrue("REST worker leak", node.workerThreads() <= threads);
  }
}
