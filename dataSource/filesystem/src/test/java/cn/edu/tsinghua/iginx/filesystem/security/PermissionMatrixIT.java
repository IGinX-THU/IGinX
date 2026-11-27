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

import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.thrift.*;
import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.apache.thrift.TBase;
import org.junit.*;

/** Independent Appendix A fixtures: every cell is a real, semantically valid public request. */
public class PermissionMatrixIT {
  static SecurityTestEnvironment env;
  static SecurityTestEnvironment.Node node;
  static CodeTestFixture code;
  static StorageTestFixture storage;
  static final Map<String, Set<AuthType>> ROLES = new LinkedHashMap<>();

  static {
    ROLES.put("r", EnumSet.of(AuthType.Read));
    ROLES.put("w", EnumSet.of(AuthType.Write));
    ROLES.put("rw", EnumSet.of(AuthType.Read, AuthType.Write));
    ROLES.put("a", EnumSet.of(AuthType.Admin));
    ROLES.put("c", EnumSet.of(AuthType.Cluster));
  }

  static Map<StatementType, Set<AuthType>> sqlPermissions() {
    Map<StatementType, Set<AuthType>> result = new EnumMap<>(StatementType.class);
    addSql(
        result,
        AuthType.Read,
        "SELECT SHOW_COLUMNS COUNT_POINTS EXPORT_CSV_FROM_SELECT EXPORT_STREAM_FROM_SELECT SHOW_REPLICATION SHOW_USER SHOW_REGISTER_TASK SHOW_JOB_STATUS SHOW_ELIGIBLE_JOB");
    addSql(result, AuthType.Write, "INSERT DELETE DELETE_COLUMNS CLEAR_DATA INSERT_FROM_CSV");
    addSql(
        result,
        AuthType.Admin,
        "CREATE_USER GRANT_USER CHANGE_USER_PASSWORD DROP_USER SHOW_CONFIG SET_CONFIG SHOW_SESSION_ID SHOW_RULES SET_RULES REGISTER_TASK DROP_TASK COMMIT_TRANSFORM_JOB CANCEL_JOB");
    addSql(
        result,
        AuthType.Cluster,
        "ADD_STORAGE_ENGINE ALTER_STORAGE_ENGINE REMOVE_HISTORY_DATA_SOURCE SHOW_CLUSTER_INFO COMPACT");
    result.put(StatementType.INSERT_FROM_SELECT, EnumSet.of(AuthType.Read, AuthType.Write));
    return result;
  }

  static void addSql(Map<StatementType, Set<AuthType>> m, AuthType a, String names) {
    for (String n : names.split(" ")) assertNull(m.put(StatementType.valueOf(n), EnumSet.of(a)));
  }

  static Map<String, Set<AuthType>> rpcPermissions() {
    Map<String, Set<AuthType>> result = new LinkedHashMap<>();
    addRpc(
        result,
        AuthType.Read,
        "queryData aggregateQuery downsampleQuery lastQuery showColumns getReplicaNum getUser curveMatch getRegisterTaskInfo queryTransformJobStatus showEligibleJob executeSql executeStatement");
    addRpc(
        result,
        AuthType.Write,
        "insertColumnRecords insertNonAlignedColumnRecords insertRowRecords insertNonAlignedRowRecords deleteColumns deleteDataInColumns uploadFileChunk loadCSV");
    addRpc(
        result,
        AuthType.Admin,
        "addUser updateUser deleteUser showSessionID showRules setRules registerTask dropTask commitTransformJob cancelTransformJob loadUDF");
    addRpc(
        result,
        AuthType.Cluster,
        "addStorageEngines alterStorageEngine removeStorageEngine getClusterInfo");
    for (String n : Arrays.asList("closeSession", "closeStatement", "fetchResults"))
      result.put(n, Collections.emptySet());
    return result;
  }

  static void addRpc(Map<String, Set<AuthType>> m, AuthType a, String names) {
    for (String n : names.split(" ")) assertNull(m.put(n, EnumSet.of(a)));
  }

  @BeforeClass
  public static void start() throws Exception {
    env = new SecurityTestEnvironment();
    node = env.node(0, secure("creator", "required"));
    node.start();
    for (Map.Entry<String, Set<AuthType>> role : ROLES.entrySet())
      node.user(role.getKey(), role.getValue().toArray(new AuthType[0]));
    code = new CodeTestFixture(node);
    storage = new StorageTestFixture(node);
  }

  @AfterClass
  public static void stop() throws Exception {
    if (env != null) env.close();
  }

  long job;

  void prepare(String operation) throws Exception {
    storage.ensure(false);
    node.admin.execute("CLEAR DATA;");
    node.admin.execute("INSERT INTO business (key,a) VALUES (1,11),(2,22),(3,33),(4,44),(5,55);");
    List<String> users = node.admin.client.getUser(new GetUserReq(node.admin.id)).usernames;
    if (users.contains("target"))
      ok(node.admin.client.deleteUser(new DeleteUserReq(node.admin.id, "target")));
    if (!operation.equals("CREATE_USER") && !operation.equals("addUser"))
      node.user("target", AuthType.Read);
    for (RegisterTaskInfo info :
        node.admin.client.getRegisterTaskInfo(new GetRegisterTaskInfoReq(node.admin.id))
            .registerTaskInfoList)
      if (info.name.equals("matrix_function"))
        ok(node.admin.client.dropTask(new DropTaskReq(node.admin.id, info.name)));
    if (operation.equals("DROP_TASK") || operation.equals("dropTask"))
      node.admin.execute(code.create("matrix_function"));
    storage.ensure(
        operation.equals("ALTER_STORAGE_ENGINE")
            || operation.equals("REMOVE_HISTORY_DATA_SOURCE")
            || operation.equals("alterStorageEngine")
            || operation.equals("removeStorageEngine"));
    if (operation.contains("JOB") || operation.contains("Job"))
      job = CodeTestFixture.submit(node.admin, code.scheduled(node.admin.id));
  }

  String sql(StatementType type) throws Exception {
    switch (type) {
      case SELECT:
        return "SELECT * FROM business;";
      case SHOW_COLUMNS:
        return "SHOW COLUMNS;";
      case COUNT_POINTS:
        return "COUNT POINTS;";
      case EXPORT_CSV_FROM_SELECT:
        return "SELECT * FROM business INTO OUTFILE \"matrix.csv\" AS CSV;";
      case EXPORT_STREAM_FROM_SELECT:
        return "SELECT * FROM business INTO OUTFILE \"matrix.stream\" AS STREAM;";
      case SHOW_REPLICATION:
        return "SHOW REPLICA NUMBER;";
      case SHOW_USER:
        return "SHOW USER;";
      case SHOW_REGISTER_TASK:
        return "SHOW FUNCTIONS;";
      case SHOW_JOB_STATUS:
        return "SHOW TRANSFORM JOB STATUS " + job + ";";
      case SHOW_ELIGIBLE_JOB:
        return "SHOW TRANSFORM JOBS;";
      case INSERT:
        return "INSERT INTO written (key,a) VALUES (99,99);";
      case DELETE:
        return "DELETE FROM business WHERE key<3;";
      case DELETE_COLUMNS:
        return "DELETE COLUMNS business.a;";
      case CLEAR_DATA:
        return "CLEAR DATA;";
      case INSERT_FROM_CSV:
        Path csv = node.home.resolve("matrix.csv");
        Files.write(csv, "key,a\n99,99\n".getBytes(StandardCharsets.UTF_8));
        return "LOAD DATA FROM INFILE \""
            + csv.toString().replace('\\', '/')
            + "\" AS CSV INTO imported;";
      case INSERT_FROM_SELECT:
        return "INSERT INTO copied (key,a) VALUES (SELECT a FROM business);";
      case CREATE_USER:
        return "CREATE USER target IDENTIFIED BY password;";
      case GRANT_USER:
        return "GRANT WRITE TO USER target;";
      case CHANGE_USER_PASSWORD:
        return "ALTER USER target IDENTIFIED BY changed;";
      case DROP_USER:
        return "DROP USER target;";
      case SHOW_CONFIG:
        return "SHOW CONFIG;";
      case SET_CONFIG:
        return "SET CONFIG \"UDFTimeout\" \"10101\";";
      case SHOW_SESSION_ID:
        return "SHOW SESSIONID;";
      case SHOW_RULES:
        return "SHOW RULES;";
      case SET_RULES:
        return "SET RULES AllowNullColumnRule=on;";
      case REGISTER_TASK:
        return code.create("matrix_function");
      case DROP_TASK:
        return "DROP FUNCTION \"matrix_function\";";
      case COMMIT_TRANSFORM_JOB:
        return "COMMIT TRANSFORM JOB \"" + code.yaml.toString().replace('\\', '/') + "\";";
      case CANCEL_JOB:
        return "CANCEL TRANSFORM JOB " + job + ";";
      case ADD_STORAGE_ENGINE:
        return storage.addSql();
      case ALTER_STORAGE_ENGINE:
        return "ALTER STORAGEENGINE "
            + storage.id()
            + " OPTIONS (dummy_dir '"
            + storage.directory.toString().replace('\\', '/')
            + "');";
      case REMOVE_HISTORY_DATA_SOURCE:
        return storage.removeSql();
      case SHOW_CLUSTER_INFO:
        return "SHOW CLUSTER INFO;";
      case COMPACT:
        return "COMPACT;";
      default:
        throw new AssertionError(type);
    }
  }

  String snapshot() throws Exception {
    return SqlSecurityAcceptanceIT.snapshot(node.admin)
        + node.admin.execute("SHOW CONFIG;").configs
        + node.admin.execute("SHOW RULES;").rules;
  }

  @Test(timeout = 240000)
  public void igt11_fiveRolesEverySqlStatement() throws Exception {
    Map<StatementType, Set<AuthType>> operations = sqlPermissions();
    assertEquals(34, operations.size());
    Set<StatementType> all = EnumSet.allOf(StatementType.class);
    all.remove(StatementType.NULL);
    assertEquals(all, operations.keySet());
    for (Map.Entry<String, Set<AuthType>> role : ROLES.entrySet())
      try (Connection c = node.connect(role.getKey(), "password")) {
        for (Map.Entry<StatementType, Set<AuthType>> operation : operations.entrySet()) {
          prepare(operation.getKey().name());
          String statement = sql(operation.getKey());
          String before = snapshot();
          Status status = c.sql(statement).status;
          boolean allowed = role.getValue().containsAll(operation.getValue());
          assertEquals(
              role.getKey() + " " + operation.getKey() + " " + statement + " " + status,
              allowed ? 200 : 101,
              status.code);
          if (!allowed) assertEquals(before, snapshot());
        }
      }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  TBase insert(Class<? extends TBase> type, long session) throws Exception {
    TBase request = SqlSecurityAcceptanceIT.minimum(type);
    request.setFieldValue(request.fieldForId(1), session);
    request.setFieldValue(request.fieldForId(2), Arrays.asList("written.a"));
    request.setFieldValue(request.fieldForId(3), ByteBuffer.allocate(8).putLong(0, 99));
    request.setFieldValue(
        request.fieldForId(4), Arrays.asList(ByteBuffer.allocate(8).putLong(0, 99)));
    request.setFieldValue(request.fieldForId(5), Arrays.asList(ByteBuffer.wrap(new byte[] {1})));
    request.setFieldValue(request.fieldForId(6), Arrays.asList(DataType.LONG));
    return request;
  }

  Object request(String method, Class<?> type, Connection c, Set<AuthType> auth) throws Exception {
    long id = c.id;
    List<String> paths = Arrays.asList("business.a");
    switch (method) {
      case "queryData":
        return new QueryDataReq(id, paths, 0, 100);
      case "aggregateQuery":
        return new AggregateQueryReq(id, paths, 0, 100, AggregateType.COUNT);
      case "downsampleQuery":
        return new DownsampleQueryReq(id, paths, 0, 100, AggregateType.COUNT, 10);
      case "lastQuery":
        return new LastQueryReq(id, paths, 0);
      case "showColumns":
        return new ShowColumnsReq(id);
      case "getReplicaNum":
        return new GetReplicaNumReq(id);
      case "getUser":
        return new GetUserReq(id);
      case "curveMatch":
        return new CurveMatchReq(id, paths, 0, 100, Arrays.asList(11.0, 22.0, 33.0), 1);
      case "getRegisterTaskInfo":
        return new GetRegisterTaskInfoReq(id);
      case "queryTransformJobStatus":
        return new QueryTransformJobStatusReq(id, job);
      case "showEligibleJob":
        return new ShowEligibleJobReq(id);
      case "insertColumnRecords":
      case "insertNonAlignedColumnRecords":
      case "insertRowRecords":
      case "insertNonAlignedRowRecords":
        return insert((Class<? extends TBase>) type, id);
      case "deleteColumns":
        return new DeleteColumnsReq(id, paths);
      case "deleteDataInColumns":
        return new DeleteDataInColumnsReq(id, paths, 0, 3);
      case "uploadFileChunk":
        return new UploadFileReq(
            id,
            new FileChunk(
                "matrix-" + UUID.randomUUID() + ".csv", 0, ByteBuffer.wrap(new byte[] {7}), 1));
      case "addUser":
        return new AddUserReq(id, "target", "password", EnumSet.of(AuthType.Read));
      case "updateUser":
        return new UpdateUserReq(id, "target").setPassword("changed");
      case "deleteUser":
        return new DeleteUserReq(id, "target");
      case "showSessionID":
        return new ShowSessionIDReq(id);
      case "showRules":
        return new ShowRulesReq(id);
      case "setRules":
        return new SetRulesReq(id, Collections.singletonMap("AllowNullColumnRule", true));
      case "registerTask":
        return code.register(id, "matrix_function", true);
      case "dropTask":
        return new DropTaskReq(id, "matrix_function");
      case "commitTransformJob":
        return code.scheduled(id);
      case "cancelTransformJob":
        return new CancelTransformJobReq(id, job);
      case "addStorageEngines":
        return new AddStorageEnginesReq(id, Arrays.asList(storage.engine()));
      case "alterStorageEngine":
        return new AlterStorageEngineReq(
            id, storage.id(), Collections.singletonMap("dummy_dir", storage.directory.toString()));
      case "removeStorageEngine":
        return storage.remove(id);
      case "getClusterInfo":
        return new GetClusterInfoReq(id);
      case "executeSql":
        return new ExecuteSqlReq(id, "SELECT * FROM business;");
      case "executeStatement":
        return new ExecuteStatementReq(id, "SELECT * FROM business;").setFetchSize(2);
      case "loadCSV":
        String file = "owned-" + UUID.randomUUID() + ".csv";
        if (auth.contains(AuthType.Write))
          SqlSecurityAcceptanceIT.upload(
              c, file, "key,a\n99,99\n".getBytes(StandardCharsets.UTF_8));
        return new LoadCSVReq(
            id, "LOAD DATA FROM INFILE \"source.csv\" AS CSV INTO imported;", file);
      case "loadUDF":
        return new LoadUDFReq(id, code.create("matrix_function"), true)
            .setUdfFile(ByteBuffer.wrap(code.archive));
      case "closeSession":
        return new CloseSessionReq(id);
      case "fetchResults":
      case "closeStatement":
        String statement =
            auth.contains(AuthType.Read)
                ? "SELECT * FROM business;"
                : auth.contains(AuthType.Admin)
                    ? "SHOW CONFIG;"
                    : auth.contains(AuthType.Cluster)
                        ? "SHOW CLUSTER INFO;"
                        : "INSERT INTO cursor (key,a) VALUES (1,1);";
        ExecuteStatementResp cursor =
            c.client.executeStatement(new ExecuteStatementReq(id, statement).setFetchSize(1));
        ok(cursor.status);
        return method.equals("fetchResults")
            ? new FetchResultsReq(id, cursor.queryId).setFetchSize(2)
            : new CloseStatementReq(id, cursor.queryId);
      default:
        throw new AssertionError("Unmapped RPC " + method);
    }
  }

  @Test(timeout = 240000)
  public void igt11_fiveRolesEveryRpc() throws Exception {
    Map<String, Set<AuthType>> operations = rpcPermissions();
    Map<String, java.lang.reflect.Method> methods = new TreeMap<>();
    for (java.lang.reflect.Method m : IService.Iface.class.getDeclaredMethods())
      if (!m.getName().equals("openSession") && !m.getName().equals("debugInfo"))
        methods.put(m.getName(), m);
    assertEquals(39, operations.size());
    assertEquals(methods.keySet(), operations.keySet());
    for (Map.Entry<String, Set<AuthType>> role : ROLES.entrySet())
      for (java.lang.reflect.Method method : methods.values())
        try (Connection c = node.connect(role.getKey(), "password")) {
          prepare(method.getName());
          Object request =
              request(method.getName(), method.getParameterTypes()[0], c, role.getValue());
          String before = snapshot();
          Object response = method.invoke(c.client, request);
          Status status = SqlSecurityAcceptanceIT.status(response);
          boolean allowed = role.getValue().containsAll(operations.get(method.getName()));
          assertEquals(
              role.getKey() + " " + method.getName() + " " + status,
              allowed ? 200 : 101,
              status.code);
          if (!allowed) assertEquals(before, snapshot());
        }
  }
}
