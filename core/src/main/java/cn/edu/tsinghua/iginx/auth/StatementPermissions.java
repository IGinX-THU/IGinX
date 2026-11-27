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

import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import java.util.*;

/** Exhaustive SQL permission matrix. Unlisted statement types fail closed. */
public final class StatementPermissions {
  private static final Map<StatementType, Set<AuthType>> PERMISSIONS =
      new EnumMap<>(StatementType.class);
  private static final Map<String, Set<AuthType>> RPC_PERMISSIONS = new HashMap<>();

  static {
    add(
        AuthType.Read,
        StatementType.SELECT,
        StatementType.SHOW_COLUMNS,
        StatementType.COUNT_POINTS,
        StatementType.EXPORT_CSV_FROM_SELECT,
        StatementType.EXPORT_STREAM_FROM_SELECT,
        StatementType.SHOW_REPLICATION,
        StatementType.SHOW_USER,
        StatementType.SHOW_REGISTER_TASK,
        StatementType.SHOW_JOB_STATUS,
        StatementType.SHOW_ELIGIBLE_JOB);
    add(
        AuthType.Write,
        StatementType.INSERT,
        StatementType.DELETE,
        StatementType.DELETE_COLUMNS,
        StatementType.CLEAR_DATA,
        StatementType.INSERT_FROM_CSV);
    add(
        AuthType.Admin,
        StatementType.CREATE_USER,
        StatementType.GRANT_USER,
        StatementType.CHANGE_USER_PASSWORD,
        StatementType.DROP_USER,
        StatementType.SHOW_CONFIG,
        StatementType.SET_CONFIG,
        StatementType.SHOW_SESSION_ID,
        StatementType.SHOW_RULES,
        StatementType.SET_RULES,
        StatementType.REGISTER_TASK,
        StatementType.DROP_TASK,
        StatementType.COMMIT_TRANSFORM_JOB,
        StatementType.CANCEL_JOB);
    add(
        AuthType.Cluster,
        StatementType.ADD_STORAGE_ENGINE,
        StatementType.ALTER_STORAGE_ENGINE,
        StatementType.REMOVE_HISTORY_DATA_SOURCE,
        StatementType.SHOW_CLUSTER_INFO,
        StatementType.COMPACT);
    PERMISSIONS.put(
        StatementType.INSERT_FROM_SELECT,
        Collections.unmodifiableSet(EnumSet.of(AuthType.Read, AuthType.Write)));
    rpc(
        AuthType.Read,
        "queryData aggregateQuery downsampleQuery lastQuery showColumns getReplicaNum getUser curveMatch getRegisterTaskInfo queryTransformJobStatus showEligibleJob");
    rpc(
        AuthType.Write,
        "insertColumnRecords insertNonAlignedColumnRecords insertRowRecords insertNonAlignedRowRecords deleteColumns deleteDataInColumns uploadFileChunk");
    rpc(
        AuthType.Admin,
        "addUser updateUser deleteUser showSessionID showRules setRules registerTask dropTask commitTransformJob cancelTransformJob");
    rpc(
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
            "fetchResults")) RPC_PERMISSIONS.put(method, Collections.emptySet());
  }

  private static void rpc(AuthType auth, String methods) {
    for (String method : methods.split(" "))
      RPC_PERMISSIONS.put(method, Collections.singleton(auth));
  }

  /** Empty sets require the entry's session/ownership or parsed SQL check; null fails closed. */
  public static Set<AuthType> requiredRpc(String method) {
    return RPC_PERMISSIONS.get(method);
  }

  public static boolean permitsRpc(long sessionId, String method) {
    Set<AuthType> required = requiredRpc(method);
    if (required == null) return false;
    for (AuthType auth : required)
      if (!SessionManager.getInstance().checkSession(sessionId, auth)) return false;
    return true;
  }

  private static void add(AuthType auth, StatementType... types) {
    for (StatementType type : types) PERMISSIONS.put(type, Collections.singleton(auth));
  }

  public static Set<AuthType> required(StatementType type) {
    return PERMISSIONS.get(type);
  }

  public static boolean permits(long sessionId, StatementType type) {
    Set<AuthType> required = required(type);
    if (required == null) return false;
    for (AuthType auth : required) {
      if (!SessionManager.getInstance().checkSession(sessionId, auth)) return false;
    }
    return true;
  }

  private StatementPermissions() {}
}
