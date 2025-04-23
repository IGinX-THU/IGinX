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
package cn.edu.tsinghua.iginx.integration.expansion.dameng;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DamengHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(
          cn.edu.tsinghua.iginx.integration.expansion.dameng.DamengHistoryDataGenerator.class);

  private static final char SEPARATOR = '.';

  private static final String PREFIX = "DAMENG"; // 密码前缀（达梦密码强度要求）

  public static final String QUERY_DATABASES_STATEMENT =
      "SELECT username  as DATNAME FROM DBA_USERS WHERE CREATED > TO_DATE('2025-01-01', 'YYYY-MM-DD')";

  public static final String CREATE_DATABASE_STATEMENT = "CREATE USER %s IDENTIFIED BY %s";

  private static final String GRANT_DATABASE_STATEMENT =
      "GRANT CREATE SESSION,CREATE TABLE,UNLIMITED TABLESPACE TO %s";

  private static final String GRANT_ROLE_STATEMENT = "GRANT RESOURCE TO %s";

  public static final String CREATE_TABLE_STATEMENT = "CREATE TABLE IF NOT EXISTS %s.%s (%s)";

  public static final String INSERT_STATEMENT = "INSERT INTO %s.%s VALUES %s";

  public static final String DROP_DATABASE_STATEMENT = "DROP USER %s CASCADE";

  public DamengHistoryDataGenerator() {
    Constant.oriPort = 5236;
    Constant.expPort = 5237;
    Constant.readOnlyPort = 5238;
  }

  public static Connection connect(int port) {
    try {
      String url;
      url = String.format("jdbc:dm://127.0.0.1:%d/SYSDBA?user=SYSDBA&password=SYSDBA001", port);
      Class.forName("dm.jdbc.driver.DmDriver");
      return DriverManager.getConnection(url);
    } catch (SQLException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeHistoryData(
      int port,
      List<String> pathList,
      List<DataType> dataTypeList,
      List<Long> keyList,
      List<List<Object>> valuesList) {
    Connection connection = null;
    try {
      connection = connect(port);
      if (connection == null) {
        LOGGER.error("cannot connect to 127.0.0.1:{}!", port);
        return;
      }
      LOGGER.info("pathList: {}", pathList);

      Map<String, Map<String, List<Integer>>> databaseToTablesToColumnIndexes = new HashMap<>();
      for (int i = 0; i < pathList.size(); i++) {
        String path = pathList.get(i);
        String databaseName = path.substring(0, path.indexOf(SEPARATOR));
        String tableName = path.substring(path.indexOf(SEPARATOR) + 1, path.lastIndexOf(SEPARATOR));

        Map<String, List<Integer>> tablesToColumnIndexes =
            databaseToTablesToColumnIndexes.computeIfAbsent(databaseName, x -> new HashMap<>());
        List<Integer> columnIndexes =
            tablesToColumnIndexes.computeIfAbsent(tableName, x -> new ArrayList<>());
        columnIndexes.add(i);
        tablesToColumnIndexes.put(tableName, columnIndexes);
      }

      for (Map.Entry<String, Map<String, List<Integer>>> entry :
          databaseToTablesToColumnIndexes.entrySet()) {
        String databaseName = entry.getKey();
        Statement stmt = connection.createStatement();
        String createDatabaseSql =
            String.format(
                CREATE_DATABASE_STATEMENT, getQuotName(databaseName), toDamengPassword(port));
        String grantDatabaseSql =
            String.format(GRANT_DATABASE_STATEMENT, getQuotName(databaseName));
        String grantRoleSql = String.format(GRANT_ROLE_STATEMENT, getQuotName(databaseName));
        try {
          LOGGER.info("create database with stmt: {}", createDatabaseSql);
          stmt.execute(createDatabaseSql);
          stmt.execute(grantDatabaseSql);
          stmt.execute(grantRoleSql);
        } catch (SQLException e) {
          LOGGER.info("database {} exists!", databaseName);
        }
        stmt.close();

        stmt = connection.createStatement();
        for (Map.Entry<String, List<Integer>> item : entry.getValue().entrySet()) {
          String tableName = item.getKey();
          StringBuilder createTableStr = new StringBuilder();
          for (Integer index : item.getValue()) {
            String path = pathList.get(index);
            String columnName = path.substring(path.lastIndexOf(SEPARATOR) + 1);
            DataType dataType = dataTypeList.get(index);
            createTableStr.append(getQuotName(columnName));
            createTableStr.append(" ");
            createTableStr.append(toDamengSQL(dataType));
            createTableStr.append(", ");
          }
          stmt.execute(
              String.format(
                  CREATE_TABLE_STATEMENT,
                  getQuotName(databaseName),
                  getQuotName(tableName),
                  createTableStr.substring(0, createTableStr.length() - 2)));

          StringBuilder insertStr = new StringBuilder();
          for (List<Object> values : valuesList) {
            insertStr.append("(");
            for (Integer index : item.getValue()) {
              if (dataTypeList.get(index) == DataType.BINARY) {
                insertStr.append("'").append(new String((byte[]) values.get(index))).append("'");
              } else if (dataTypeList.get(index) == DataType.BOOLEAN) {
                // 达梦的布尔类型需要特殊处理，转换为1或0
                insertStr.append(((Boolean) values.get(index)) ? "1" : "0");
              } else {
                insertStr.append(values.get(index));
              }
              insertStr.append(", ");
            }
            insertStr = new StringBuilder(insertStr.substring(0, insertStr.length() - 2));
            insertStr.append("), ");
          }
          stmt.execute(
              String.format(
                  INSERT_STATEMENT,
                  getQuotName(databaseName),
                  getQuotName(tableName),
                  insertStr.substring(0, insertStr.length() - 2)));
        }
        stmt.close();
      }
      connection.close();

      LOGGER.info("write data to 127.0.0.1:{} success!", port);
    } catch (RuntimeException | SQLException e) {
      LOGGER.error("write data to 127.0.0.1:{} failure: ", port, e);
    } finally {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        LOGGER.error("close connection failure: ", e);
      }
    }
  }

  //  @Override
  //  public void writeHistoryData(
  //      int port,
  //      List<String> pathList,
  //      List<DataType> dataTypeList,
  //      List<Long> keyList,
  //      List<List<Object>> valuesList) {
  //    Connection connection = null;
  //    try {
  //      connection = connect(port);
  //      if (connection == null) {
  //        LOGGER.error("cannot connect to 127.0.0.1:{}!", port);
  //        return;
  //      }
  //
  //      Map<String, Map<String, List<Integer>>> databaseToTablesToColumnIndexes = new HashMap<>();
  //      for (int i = 0; i < pathList.size(); i++) {
  //        String path = pathList.get(i);
  //        String databaseName = path.substring(0, path.indexOf(SEPARATOR));
  //        String tableName = path.substring(path.indexOf(SEPARATOR) + 1,
  // path.lastIndexOf(SEPARATOR));
  //
  //        Map<String, List<Integer>> tablesToColumnIndexes =
  //            databaseToTablesToColumnIndexes.computeIfAbsent(databaseName, x -> new HashMap<>());
  //        List<Integer> columnIndexes =
  //            tablesToColumnIndexes.computeIfAbsent(tableName, x -> new ArrayList<>());
  //        columnIndexes.add(i);
  //        tablesToColumnIndexes.put(tableName, columnIndexes);
  //      }
  //
  //      for (Map.Entry<String, Map<String, List<Integer>>> entry :
  //          databaseToTablesToColumnIndexes.entrySet()) {
  //        String databaseName = entry.getKey();
  //        Statement stmt = connection.createStatement();
  //        // 达梦的用户名始终会转为大写
  //        String createDatabaseSql =
  //            String.format(
  //                CREATE_DATABASE_STATEMENT, getQuotName(databaseName), toDamengPassword(port));
  //        String grantDatabaseSql =
  //            String.format(GRANT_DATABASE_STATEMENT, getQuotName(databaseName));
  //        String grantRoleSql = String.format(GRANT_ROLE_STATEMENT, getQuotName(databaseName));
  //        try {
  //          LOGGER.info("create database with stmt: {} {}", port, createDatabaseSql);
  //          stmt.execute(createDatabaseSql);
  //          stmt.execute(grantDatabaseSql);
  //          stmt.execute(grantRoleSql);
  //        } catch (SQLException e) {
  //          LOGGER.info("database {} {} exists!", port, databaseName);
  //        }
  //        stmt.close();
  //
  //        stmt = connection.createStatement();
  //        for (Map.Entry<String, List<Integer>> item : entry.getValue().entrySet()) {
  //          String tableName = item.getKey();
  //          StringBuilder columnStr = new StringBuilder();
  //          StringBuilder createTableStr = new StringBuilder();
  //          for (Integer index : item.getValue()) {
  //            String path = pathList.get(index);
  //            String columnName = path.substring(path.lastIndexOf(SEPARATOR) + 1);
  //            DataType dataType = dataTypeList.get(index);
  //            createTableStr.append(getQuotName(columnName));
  //            createTableStr.append(" ");
  //            createTableStr.append(toDamengSQL(dataType));
  //            createTableStr.append(", ");
  //            columnStr.append(getQuotName(columnName)).append(",");
  //          }
  //          stmt.execute(
  //              String.format(
  //                  CREATE_TABLE_STATEMENT,
  //                  getQuotName(databaseName),
  //                  getQuotName(tableName),
  //                  createTableStr.substring(0, createTableStr.length() - 2)));
  //
  //          columnStr = new StringBuilder(columnStr.substring(0, columnStr.length() - 1));
  //          int start = 0, end = 0, step = 0;
  //          while (end < valuesList.size()) {
  //            step = Math.min(valuesList.size() - end, 1000);
  //            end += step;
  //            StringBuilder insertAllSql = new StringBuilder("INSERT ALL ");
  //            StringBuilder insertStr = new StringBuilder();
  //            List<List<Object>> lists = valuesList.subList(start, end);
  //            for (List<Object> values : lists) {
  //              insertStr
  //                  .append("INTO ")
  //                  .append(getQuotName(databaseName))
  //                  .append(SEPARATOR)
  //                  .append(getQuotName(tableName))
  //                  .append(" (")
  //                  .append(columnStr)
  //                  .append(") VALUES ");
  //              insertStr.append("(");
  //              for (Integer index : item.getValue()) {
  //                if (dataTypeList.get(index) == DataType.BINARY) {
  //                  insertStr.append("'").append(new String((byte[])
  // values.get(index))).append("'");
  //                } else {
  //                  insertStr.append(values.get(index));
  //                }
  //                insertStr.append(", ");
  //              }
  //              insertStr = new StringBuilder(insertStr.substring(0, insertStr.length() - 2));
  //              insertStr.append(") ");
  //            }
  //            insertAllSql.append(insertStr).append("SELECT * FROM dual");
  //            stmt.execute(insertAllSql.toString());
  //            start = end;
  //          }
  //        }
  //        stmt.close();
  //      }
  //      connection.close();
  //
  //      LOGGER.info("write data to 127.0.0.1:{} success!", port);
  //    } catch (RuntimeException | SQLException e) {
  //      LOGGER.error("write data to 127.0.0.1:{} failure: ", port, e);
  //    } finally {
  //      try {
  //        if (connection != null) {
  //          connection.close();
  //        }
  //      } catch (SQLException e) {
  //        LOGGER.error("close connection failure: ", e);
  //      }
  //    }
  //  }

  @Override
  public void writeHistoryData(
      int port, List<String> pathList, List<DataType> dataTypeList, List<List<Object>> valuesList) {
    writeHistoryData(port, pathList, dataTypeList, new ArrayList<>(), valuesList);
  }

  @Override
  public void writeSpecialHistoryData() {
    // write float value
    writeHistoryData(
        Constant.readOnlyPort,
        Constant.READ_ONLY_FLOAT_PATH_LIST,
        new ArrayList<>(Collections.singletonList(DataType.FLOAT)),
        Constant.READ_ONLY_FLOAT_VALUES_LIST);
    // create another user who can read data of tm user
    try (Connection connection = connect(Constant.readOnlyPort);
        Statement stmt = connection.createStatement()) {
      String createDatabaseSql =
          String.format(
              CREATE_DATABASE_STATEMENT,
              getQuotName("observer"),
              toDamengPassword(Constant.readOnlyPort));
      LOGGER.info(
          "create another user in {} with stmt: {}", Constant.readOnlyPort, createDatabaseSql);
      stmt.execute(createDatabaseSql);

      String grantDatabaseSql = String.format(GRANT_DATABASE_STATEMENT, getQuotName("observer"));
      String grantRoleSql = String.format(GRANT_ROLE_STATEMENT, getQuotName("observer"));
      LOGGER.info("grant permission to observer with stmt: {}", grantDatabaseSql);
      stmt.execute(grantDatabaseSql);
      stmt.execute(grantRoleSql);

      String grantTableSql =
          String.format(
              "GRANT SELECT ON %s.%s TO %s",
              getQuotName("tm"), getQuotName("wf05.wt01"), getQuotName("observer"));
      LOGGER.info("grant select permission to observer with stmt: {}", grantTableSql);
      stmt.execute(grantTableSql);

      String grantTable2Sql =
          String.format(
              "GRANT SELECT ON %s.%s TO %s",
              getQuotName("tm"), getQuotName("wf05.wt02"), getQuotName("observer"));
      LOGGER.info("grant select permission to observer with stmt: {}", grantTable2Sql);
      stmt.execute(grantTable2Sql);
    } catch (SQLException e) {
      LOGGER.error("write special history data failure: ", e);
    }
  }

  @Override
  public void clearHistoryDataForGivenPort(int port) {
    Connection conn = null;
    try {
      LOGGER.info("port: {}", port);
      conn = connect(port);
      Statement stmt = conn.createStatement();
      ResultSet databaseSet = stmt.executeQuery(QUERY_DATABASES_STATEMENT);
      Statement dropDatabaseStatement = conn.createStatement();

      while (databaseSet.next()) {
        String databaseName = databaseSet.getString("DATNAME");

        // 过滤系统数据库
        if (databaseName.equals("SYS")
            || databaseName.equals("SYSDBA")
            || databaseName.equals("SYSSSO")
            || databaseName.equals("SYSAUDITOR")) {
          continue;
        }

        dropDatabaseStatement.addBatch(
            String.format(DROP_DATABASE_STATEMENT, getQuotName(databaseName)));
        LOGGER.info("drop database {} on 127.0.0.1:{}: ", databaseName, port);
      }
      dropDatabaseStatement.executeBatch();

      dropDatabaseStatement.close();
      databaseSet.close();
      stmt.close();
      conn.close();
      LOGGER.info("clear data on 127.0.0.1:{} success!", port);
    } catch (SQLException e) {
      LOGGER.warn("clear data on 127.0.0.1:{} failure: ", port, e);
    } finally {
      try {
        if (conn != null) {
          conn.close();
        }
      } catch (SQLException e) {
        LOGGER.error("close connection failure: ", e);
      }
    }
  }

  private static String toDamengSQL(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return "TINYINT";
      case INTEGER:
        return "INT";
      case LONG:
        return "BIGINT";
      case FLOAT:
        return "FLOAT";
      case DOUBLE:
        return "DOUBLE";
      case BINARY:
      default:
        return "VARCHAR(4000)";
    }
  }

  private static String getQuotName(String name) {
    return String.format("\"%s\"", name);
  }

  private static String toDamengPassword(int port) {
    return PREFIX + port;
  }
}
