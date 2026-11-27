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
package cn.edu.tsinghua.iginx.auth;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.thrift.*;
import java.lang.reflect.Method;
import java.util.*;
import org.junit.Test;

public class StatementAuthTableTest {
  @Test
  public void everyTypeMapped() {
    Map<StatementType, Set<AuthType>> appendix = new EnumMap<>(StatementType.class);
    expected(
        appendix,
        AuthType.Read,
        "SELECT SHOW_COLUMNS COUNT_POINTS EXPORT_CSV_FROM_SELECT EXPORT_STREAM_FROM_SELECT SHOW_REPLICATION SHOW_USER SHOW_REGISTER_TASK SHOW_JOB_STATUS SHOW_ELIGIBLE_JOB");
    expected(appendix, AuthType.Write, "INSERT DELETE DELETE_COLUMNS CLEAR_DATA INSERT_FROM_CSV");
    expected(
        appendix,
        AuthType.Admin,
        "CREATE_USER GRANT_USER CHANGE_USER_PASSWORD DROP_USER SHOW_CONFIG SET_CONFIG SHOW_SESSION_ID SHOW_RULES SET_RULES REGISTER_TASK DROP_TASK COMMIT_TRANSFORM_JOB CANCEL_JOB");
    expected(
        appendix,
        AuthType.Cluster,
        "ADD_STORAGE_ENGINE ALTER_STORAGE_ENGINE REMOVE_HISTORY_DATA_SOURCE SHOW_CLUSTER_INFO COMPACT");
    appendix.put(StatementType.INSERT_FROM_SELECT, EnumSet.of(AuthType.Read, AuthType.Write));
    assertEquals(34, appendix.size());
    for (StatementType type : StatementType.values()) {
      if (type == StatementType.NULL) assertNull(StatementPermissions.required(type));
      else assertEquals(type.name(), appendix.get(type), StatementPermissions.required(type));
    }
    assertEquals(
        EnumSet.of(AuthType.Read, AuthType.Write),
        StatementPermissions.required(StatementType.INSERT_FROM_SELECT));
    assertEquals(
        Collections.singleton(AuthType.Admin),
        StatementPermissions.required(StatementType.SHOW_CONFIG));
    assertEquals(
        Collections.singleton(AuthType.Read),
        StatementPermissions.required(StatementType.SHOW_JOB_STATUS));
    assertEquals(
        Collections.singleton(AuthType.Cluster),
        StatementPermissions.required(StatementType.COMPACT));
    Set<String> expected =
        new HashSet<>(
            Arrays.asList(
                "openSession",
                "closeSession",
                "closeStatement",
                "executeSql",
                "executeStatement",
                "loadCSV",
                "loadUDF",
                "fetchResults",
                "queryData",
                "aggregateQuery",
                "downsampleQuery",
                "lastQuery",
                "showColumns",
                "getReplicaNum",
                "getUser",
                "curveMatch",
                "getRegisterTaskInfo",
                "queryTransformJobStatus",
                "showEligibleJob",
                "insertColumnRecords",
                "insertNonAlignedColumnRecords",
                "insertRowRecords",
                "insertNonAlignedRowRecords",
                "deleteColumns",
                "deleteDataInColumns",
                "uploadFileChunk",
                "addUser",
                "updateUser",
                "deleteUser",
                "showSessionID",
                "showRules",
                "setRules",
                "registerTask",
                "dropTask",
                "commitTransformJob",
                "cancelTransformJob",
                "addStorageEngines",
                "alterStorageEngine",
                "removeStorageEngine",
                "getClusterInfo",
                "debugInfo"));
    Set<String> actual = new HashSet<>();
    for (Method method : IService.Iface.class.getDeclaredMethods()) actual.add(method.getName());
    assertEquals(expected, actual);
    Map<String, Set<AuthType>> rpc = new HashMap<>();
    expectedRpc(
        rpc,
        AuthType.Read,
        "queryData aggregateQuery downsampleQuery lastQuery showColumns getReplicaNum getUser curveMatch getRegisterTaskInfo queryTransformJobStatus showEligibleJob");
    expectedRpc(
        rpc,
        AuthType.Write,
        "insertColumnRecords insertNonAlignedColumnRecords insertRowRecords insertNonAlignedRowRecords deleteColumns deleteDataInColumns uploadFileChunk");
    expectedRpc(
        rpc,
        AuthType.Admin,
        "addUser updateUser deleteUser showSessionID showRules setRules registerTask dropTask commitTransformJob cancelTransformJob");
    expectedRpc(
        rpc,
        AuthType.Cluster,
        "addStorageEngines alterStorageEngine removeStorageEngine getClusterInfo");
    for (String method :
        Arrays.asList(
            "openSession",
            "closeSession",
            "closeStatement",
            "executeSql",
            "executeStatement",
            "loadCSV",
            "loadUDF",
            "fetchResults")) rpc.put(method, Collections.emptySet());
    rpc.put("debugInfo", null);
    assertEquals(actual, rpc.keySet());
    for (String method : actual)
      assertEquals(method, rpc.get(method), StatementPermissions.requiredRpc(method));
    assertNull(StatementPermissions.required(null));
    assertNull(StatementPermissions.requiredRpc("newUnreviewedEndpoint"));
  }

  private static void expectedRpc(Map<String, Set<AuthType>> table, AuthType auth, String names) {
    for (String name : names.split(" ")) assertNull(table.put(name, EnumSet.of(auth)));
  }

  private static void expected(
      Map<StatementType, Set<AuthType>> table, AuthType auth, String names) {
    for (String name : names.split(" "))
      assertNull(table.put(StatementType.valueOf(name), EnumSet.of(auth)));
  }
}
