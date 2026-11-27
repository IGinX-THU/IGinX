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

import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.jar.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.zookeeper.server.*;

/** Independent JVMs, real public protocols, retained logs, no changes to a developer deployment. */
final class SecurityTestEnvironment implements AutoCloseable {
  static final String SECRET = "acceptance-test-only-01234567890123456789";
  private static final Set<Integer> allocatedPorts = new HashSet<>();
  final Path directory;
  final ZooKeeperServer zk;
  final ServerCnxnFactory factory;
  final List<Node> nodes = new ArrayList<>();
  final List<AutoCloseable> resources = new ArrayList<>();

  SecurityTestEnvironment() throws Exception {
    Path reports = Paths.get("target", "security-acceptance").toAbsolutePath();
    Files.createDirectories(reports);
    directory = Files.createTempDirectory(reports, "run-");
    zk =
        new ZooKeeperServer(
            directory.resolve("snap").toFile(), directory.resolve("log").toFile(), 2000);
    factory = ServerCnxnFactory.createFactory(new InetSocketAddress("127.0.0.1", 0), 100);
    factory.startup(zk);
  }

  Node node(int replicas, String security) throws Exception {
    Node node = new Node(directory.resolve("node-" + nodes.size()), replicas, security);
    nodes.add(node);
    return node;
  }

  static synchronized int port() throws IOException {
    // CI's Linux and Windows ephemeral ranges start above 30000. A port selected with
    // bind(0) can be reused by an outgoing connection or JMX before the child JVM binds it.
    // Keep fixture listeners below that range and never reuse a selection in this JVM.
    int first = java.util.concurrent.ThreadLocalRandom.current().nextInt(20000);
    for (int i = 0; i < 20000; i++) {
      int candidate = 10000 + (first + i) % 20000;
      if (allocatedPorts.contains(candidate)) continue;
      try (ServerSocket socket = new ServerSocket(candidate)) {
        allocatedPorts.add(candidate);
        return socket.getLocalPort();
      } catch (BindException occupied) {
        // An independently started listener may already own this port.
      }
    }
    throw new IOException("No free acceptance listener ports in 10000..29999");
  }

  static boolean windows() {
    return System.getProperty("os.name").startsWith("Windows");
  }

  static String java() {
    return Paths.get(System.getProperty("java.home"), "bin", windows() ? "java.exe" : "java")
        .toString();
  }

  static String classpath() {
    String file = System.getProperty("iginx.acceptance.server.classpathFile");
    if (file != null)
      try {
        return new String(Files.readAllBytes(Paths.get(file)), StandardCharsets.UTF_8).trim();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    String effective =
        System.getProperty(
            "iginx.acceptance.server.classpath",
            System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
    String baseline = System.getProperty("iginx.acceptance.baselineRoot");
    if (baseline != null) {
      StringBuilder prefix = new StringBuilder();
      Set<Path> patched = new HashSet<>();
      for (String module :
          Arrays.asList("dataSource/filesystem", "core", "shared", "thrift", "session", "antlr")) {
        Path classes = Paths.get(baseline).resolve(module).resolve("target/classes");
        if (!Files.isDirectory(classes))
          throw new IllegalStateException("Missing compiled baseline classes: " + classes);
        prefix.append(classes).append(File.pathSeparator);
        patched.add(
            projectRoot().resolve(module).resolve("target/classes").toAbsolutePath().normalize());
      }
      // Exclude patched classes completely. Merely prepending original classes lets Jersey
      // discover new authentication filters that did not exist in the baseline revision.
      for (String entry : effective.split(java.util.regex.Pattern.quote(File.pathSeparator))) {
        if (!patched.contains(Paths.get(entry).toAbsolutePath().normalize()))
          prefix.append(entry).append(File.pathSeparator);
      }
      effective = prefix.toString();
    }
    return effective;
  }

  static void ok(Status status) {
    assertEquals(status.toString(), RpcUtils.SUCCESS.code, status.code);
  }

  static void denied(Status status) {
    assertEquals(status.toString(), RpcUtils.ACCESS_DENY.code, status.code);
  }

  static String secure(String acl, String internal) {
    return "zookeeper.user=meta\nzookeeper.password="
        + SECRET
        + "\nzookeeper.acl="
        + acl
        + "\ninternal.secret="
        + SECRET
        + "\ninternal.auth="
        + internal
        + "\nreserved.prefixes=indagent,bigfile,sys.blob\nreserved.accounts=indagent_data_*\n";
  }

  static void privateFile(Path path, String value) throws IOException {
    Files.write(path, value.getBytes(StandardCharsets.UTF_8));
    if (Files.getFileStore(path).supportsFileAttributeView("posix"))
      Files.setPosixFilePermissions(
          path, EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
  }

  static Path projectRoot() {
    Path p = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
    while (p != null && !Files.isDirectory(p.resolve("core/src/assembly/resources/sbin")))
      p = p.getParent();
    if (p == null) throw new IllegalStateException("Cannot find IGinX repository");
    return p;
  }

  final class Node implements AutoCloseable {
    final Path home;
    final int rpc = port(), filesystem = port(), rest = port(), mqtt = port(), management = port();
    final Properties config = new Properties();
    final Map<String, String> environment = new HashMap<>();
    final List<String> jvmOptions = new ArrayList<>();
    final List<Connection> connections = new ArrayList<>();
    Process process;
    Connection admin;
    int generation;
    Path log;
    String scriptJavaPid;

    Node(Path home, int replicas, String security) throws Exception {
      this.home = home;
      Files.createDirectories(home.resolve("driver/filesystem"));
      Files.createDirectories(home.resolve("udf/python_scripts"));
      config.setProperty("ip", "127.0.0.1");
      config.setProperty("port", "" + rpc);
      config.setProperty("zookeeperConnectionString", "127.0.0.1:" + factory.getLocalPort());
      config.setProperty(
          "storageEngineList",
          "127.0.0.1#"
              + filesystem
              + "#filesystem#iginx_port="
              + rpc
              + "#dir="
              + home.resolve("data").toString().replace('\\', '/'));
      config.setProperty("replicaNum", "" + replicas);
      config.setProperty("queryOptimizer", "rbo");
      config.setProperty("useStreamExecutor", "false");
      config.setProperty("enableMemoryControl", "false");
      config.setProperty("enableRestService", "false");
      config.setProperty("restIp", "127.0.0.1");
      config.setProperty("restPort", "" + rest);
      config.setProperty("enableMqtt", "false");
      config.setProperty("mqttHost", "127.0.0.1");
      config.setProperty("mqttPort", "" + mqtt);
      config.setProperty(
          "pythonCMD",
          System.getProperty(
              "iginx.test.python", System.getenv().getOrDefault("IGINX_TEST_PYTHON", "python")));
      config.setProperty("needInitBasicUDFFunctions", "false");
      config.setProperty("memoryTaskThreadPoolSize", "4");
      config.setProperty("physicalTaskThreadPoolSizePerStorage", "4");
      config.setProperty("defaultUDFDir", home.resolve("udf").toString());
      config.setProperty("defaultScheduledTransformJobDir", home.resolve("jobs").toString());
      security(security);
    }

    void security(String text) throws IOException {
      if (text == null) Files.deleteIfExists(home.resolve("security.properties"));
      else privateFile(home.resolve("security.properties"), text);
    }

    void writeConfig() throws IOException {
      try (OutputStream out = Files.newOutputStream(home.resolve("config.properties"))) {
        config.store(out, "isolated acceptance fixture");
      }
    }

    void start() throws Exception {
      start("root");
    }

    void start(String password) throws Exception {
      writeConfig();
      List<String> cmd =
          new ArrayList<>(
              Arrays.asList(
                  java(),
                  "-Xms32m",
                  "-Xmx512m",
                  "-XX:ActiveProcessorCount=2",
                  "-Dfile.encoding=UTF-8",
                  "-DIGINX_HOME=" + home,
                  "-DIGINX_CONF=" + home.resolve("config.properties"),
                  "-DIGINX_DRIVER=" + home.resolve("driver"),
                  "-cp",
                  classpath(),
                  "cn.edu.tsinghua.iginx.Iginx"));
      cmd.addAll(
          1,
          Arrays.asList(
              "-Dcom.sun.management.jmxremote.port=" + management,
              "-Dcom.sun.management.jmxremote.rmi.port=" + management,
              "-Dcom.sun.management.jmxremote.authenticate=false",
              "-Dcom.sun.management.jmxremote.ssl=false",
              "-Dcom.sun.management.jmxremote.host=127.0.0.1",
              "-Djava.rmi.server.hostname=127.0.0.1"));
      cmd.addAll(1, jvmOptions);
      process = launch(cmd, "node");
      awaitReady(password);
    }

    Process launch(List<String> command, String name) throws IOException {
      log = home.resolve(name + "-" + generation++ + ".log");
      ProcessBuilder builder =
          new ProcessBuilder(command)
              .directory(projectRoot().toFile())
              .redirectErrorStream(true)
              .redirectOutput(log.toFile());
      builder.environment().put("IGINX_HOME", home.toString());
      builder.environment().put("IGINX_CONF", home.resolve("config.properties").toString());
      builder.environment().put("IGINX_CONF_DIR", home.toString());
      builder.environment().put("IGINX_DRIVER", home.resolve("driver").toString());
      builder
          .environment()
          .put(
              "IGINX_JAVA_OPTS",
              "-Xms32m -Xmx512m -XX:ActiveProcessorCount=2 -Dcom.sun.management.jmxremote.port="
                  + management
                  + " -Dcom.sun.management.jmxremote.rmi.port="
                  + management
                  + " -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.host=127.0.0.1 -Djava.rmi.server.hostname=127.0.0.1");
      builder.environment().putAll(environment);
      return builder.start();
    }

    void rememberScriptProcess() throws Exception {
      javax.management.remote.JMXServiceURL url =
          new javax.management.remote.JMXServiceURL(
              "service:jmx:rmi:///jndi/rmi://127.0.0.1:" + management + "/jmxrmi");
      try (javax.management.remote.JMXConnector connector =
          javax.management.remote.JMXConnectorFactory.connect(url)) {
        java.lang.management.RuntimeMXBean runtime =
            java.lang.management.ManagementFactory.newPlatformMXBeanProxy(
                connector.getMBeanServerConnection(),
                java.lang.management.ManagementFactory.RUNTIME_MXBEAN_NAME,
                java.lang.management.RuntimeMXBean.class);
        scriptJavaPid = runtime.getName().split("@")[0];
        assertTrue(scriptJavaPid.matches("[0-9]+"));
      }
    }

    void awaitReady(String password) throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
      Exception last = null;
      while (System.nanoTime() < deadline && process.isAlive()) {
        try {
          // REST/MQTT are started asynchronously after the public RPC listener.
          if (Boolean.parseBoolean(config.getProperty("enableMqtt", "false")))
            try (Socket probe = new Socket("127.0.0.1", mqtt)) {}
          if (Boolean.parseBoolean(config.getProperty("enableRestService", "false")))
            try (Socket probe = new Socket("127.0.0.1", rest)) {}
          admin = connect("root", password);
          return;
        } catch (Exception e) {
          last = e;
          Thread.sleep(100);
        }
      }
      throw new AssertionError("Node not ready; log=" + log + "\n" + logs(), last);
    }

    Connection connect(String user, String password) throws Exception {
      Connection connection = new Connection(rpc);
      OpenSessionResp response =
          connection.client.openSession(
              new OpenSessionReq().setUsername(user).setPassword(password));
      if (response.status.code != RpcUtils.SUCCESS.code) {
        connection.close();
        throw new IOException(response.status.toString());
      }
      assertNotEquals(0, response.sessionId);
      connection.id = response.sessionId;
      connections.add(connection);
      return connection;
    }

    Connection raw() throws Exception {
      Connection c = new Connection(rpc);
      connections.add(c);
      return c;
    }

    void user(String name, AuthType... auths) throws Exception {
      ok(
          admin.client.addUser(
              new AddUserReq()
                  .setSessionId(admin.id)
                  .setUsername(name)
                  .setPassword("password")
                  .setAuths(new HashSet<>(Arrays.asList(auths)))));
    }

    String logs() throws IOException {
      return log == null ? "" : new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
    }

    int workerThreads() throws Exception {
      return workerThreadNames().size();
    }

    Set<String> workerThreadNames() throws Exception {
      javax.management.remote.JMXServiceURL url =
          new javax.management.remote.JMXServiceURL(
              "service:jmx:rmi:///jndi/rmi://127.0.0.1:" + management + "/jmxrmi");
      try (javax.management.remote.JMXConnector connector =
          javax.management.remote.JMXConnectorFactory.connect(url)) {
        java.lang.management.ThreadMXBean bean =
            java.lang.management.ManagementFactory.newPlatformMXBeanProxy(
                connector.getMBeanServerConnection(),
                java.lang.management.ManagementFactory.THREAD_MXBEAN_NAME,
                java.lang.management.ThreadMXBean.class);
        Set<String> names = new TreeSet<>();
        List<String> stacks = new ArrayList<>();
        for (java.lang.management.ThreadInfo info : bean.dumpAllThreads(false, false)) {
          String name = info.getThreadName();
          if (name.startsWith("Memory")
              || name.startsWith("pool-")
              || name.contains("WorkerProcess")
              || name.startsWith("transform")
              || name.startsWith("iginx")) {
            names.add(name);
            stacks.add(name + " " + info.getThreadState());
            for (StackTraceElement frame : info.getStackTrace()) stacks.add("  at " + frame);
          }
        }
        Files.write(home.resolve("worker-threads.txt"), stacks, StandardCharsets.UTF_8);
        return names;
      }
    }

    void packageScripts() throws IOException {
      Files.createDirectories(home.resolve("lib"));
      Files.createDirectories(home.resolve("sbin"));
      Manifest manifest = new Manifest();
      manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
      StringBuilder entries = new StringBuilder();
      for (String entry : classpath().split(java.util.regex.Pattern.quote(File.pathSeparator)))
        entries.append(Paths.get(entry).toUri().toASCIIString()).append(' ');
      manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, entries.toString());
      try (JarOutputStream jar =
          new JarOutputStream(
              Files.newOutputStream(home.resolve("lib/acceptance-runtime.jar")), manifest)) {}
      try (DirectoryStream<Path> stream =
          Files.newDirectoryStream(projectRoot().resolve("core/src/assembly/resources/sbin"))) {
        for (Path script : stream)
          if (Files.isRegularFile(script))
            Files.copy(
                script,
                home.resolve("sbin").resolve(script.getFileName()),
                StandardCopyOption.REPLACE_EXISTING);
      }
    }

    List<String> script(String name, String... args) {
      List<String> cmd = new ArrayList<>();
      if (windows()) {
        cmd.add("cmd.exe");
        cmd.add("/c");
        cmd.add(home.resolve("sbin/" + name + ".bat").toString());
      } else {
        cmd.add("bash");
        cmd.add(home.resolve("sbin/" + name + ".sh").toString());
      }
      Collections.addAll(cmd, args);
      return cmd;
    }

    void stop() throws Exception {
      for (Connection c : connections) c.close();
      connections.clear();
      admin = null;
      if (scriptJavaPid != null && windows() && process != null && process.isAlive()) {
        Process killer =
            new ProcessBuilder("taskkill", "/PID", scriptJavaPid, "/T", "/F")
                .redirectErrorStream(true)
                .redirectOutput(home.resolve("script-stop.log").toFile())
                .start();
        assertTrue(killer.waitFor(10, TimeUnit.SECONDS));
        scriptJavaPid = null;
      }
      if (process != null) {
        process.destroy();
        if (!process.waitFor(5, TimeUnit.SECONDS)) {
          process.destroyForcibly();
          assertTrue(process.waitFor(5, TimeUnit.SECONDS));
        }
        process = null;
      }
    }

    public void close() throws Exception {
      stop();
    }
  }

  static final class Connection implements AutoCloseable {
    final TSocket socket;
    final IService.Client client;
    long id;

    Connection(int port) throws Exception {
      socket = new TSocket("127.0.0.1", port, 10000);
      socket.open();
      client = new IService.Client(new TBinaryProtocol(socket));
    }

    ExecuteSqlResp sql(String sql) throws Exception {
      return client.executeSql(new ExecuteSqlReq(id, sql));
    }

    ExecuteSqlResp execute(String sql) throws Exception {
      ExecuteSqlResp r = sql(sql);
      assertEquals(sql + ": " + r.status, 200, r.status.code);
      return r;
    }

    public void close() {
      socket.close();
    }
  }

  public void close() throws Exception {
    Exception failure = null;
    for (Node node : nodes)
      try {
        node.close();
      } catch (Exception e) {
        failure = e;
      }
    for (AutoCloseable resource : resources)
      try {
        resource.close();
      } catch (Exception e) {
        failure = e;
      }
    factory.shutdown();
    zk.shutdown();
    if (failure != null) throw failure;
  }
}
