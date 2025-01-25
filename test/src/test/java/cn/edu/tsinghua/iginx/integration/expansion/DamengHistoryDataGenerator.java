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
package cn.edu.tsinghua.iginx.integration.expansion;

import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.sql.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DamengHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(DamengHistoryDataGenerator.class);

  private static final char SEPARATOR = '.';

  private static final String QUERY_DATABASES_STATEMENT =
      "select distinct object_name TABLE_SCHEMA from all_objects where object_type='SCH' AND OWNER='SYSDBA';";

  private static final String CREATE_DATABASE_STATEMENT = "CREATE SCHEMA %s";

  private static final String CREATE_TABLE_STATEMENT = "CREATE TABLE %s.%s (%s)";

  private static final String INSERT_STATEMENT = "INSERT INTO %s.%s VALUES %s";

  private static final String DROP_DATABASE_STATEMENT = "DROP SCHEMA %s CASCADE";

  public DamengHistoryDataGenerator() {
    Constant.oriPort = 5236;
    Constant.expPort = 5237;
    Constant.readOnlyPort = 5238;
  }

  private Connection connect(int port, boolean useSystemDatabase, String databaseName) {
    try {
      String url = String.format("jdbc:dm://172.17.0.2:%d", port);
      Class.forName("dm.jdbc.driver.DmDriver");
      return DriverManager.getConnection(url, "SYSDBA", "SYSDBA001");
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
      connection = connect(port, true, null);
      if (connection == null) {
        LOGGER.error("cannot connect to 127.0.0.1:{}!", port);
        return;
      }

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
            String.format(CREATE_DATABASE_STATEMENT, getQuotName(databaseName));
        try {
          LOGGER.info("create database with stmt: {}", createDatabaseSql);
          stmt.execute(createDatabaseSql);
        } catch (SQLException e) {
          LOGGER.info("database {} exists!", databaseName);
        }
        stmt.close();

        stmt = connection.createStatement();
        for (Map.Entry<String, List<Integer>> item : entry.getValue().entrySet()) {
          String tableName = item.getKey();
          StringBuilder columnStr = new StringBuilder();
          StringBuilder createTableStr = new StringBuilder();
          for (Integer index : item.getValue()) {
            String path = pathList.get(index);
            String columnName = path.substring(path.lastIndexOf(SEPARATOR) + 1);
            DataType dataType = dataTypeList.get(index);
            createTableStr
                .append(getQuotName(columnName))
                .append(" ")
                .append(toDamengSQL(dataType))
                .append(", ");
            columnStr.append(getQuotName(columnName)).append(",");
          }
          stmt.execute(
              String.format(
                  CREATE_TABLE_STATEMENT,
                  getQuotName(databaseName),
                  getQuotName(tableName),
                  createTableStr.substring(0, createTableStr.length() - 2)));
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
  }

  @Override
  public void clearHistoryDataForGivenPort(int port) {
    Connection conn = null;
    try {
      conn = connect(port, true, null);
      Statement stmt = conn.createStatement();

      // 查询达梦数据库的所有数据库
      ResultSet databaseSet = stmt.executeQuery(QUERY_DATABASES_STATEMENT);
      Statement dropDatabaseStatement = conn.createStatement();

      while (databaseSet.next()) {
        String databaseName = databaseSet.getString("TABLE_SCHEMA");

        // 过滤系统数据库
        if (databaseName.equals("SYSDBA")) {
          continue;
        }

        // 获取所有连接并终止它们
        terminateConnectionsForDatabase(databaseName, port);

        // 先终止所有连接，才能删除数据库
        dropDatabaseStatement.addBatch(
            String.format(DROP_DATABASE_STATEMENT, getQuotName(databaseName)));

        LOGGER.info("Dropping database {} on 127.0.0.1:{}...", databaseName, port);
      }

      //      dropDatabaseStatement.executeBatch();

      dropDatabaseStatement.close();
      databaseSet.close();
      stmt.close();
      conn.close();
      LOGGER.info("Successfully cleared history data on 127.0.0.1:{}!", port);

    } catch (SQLException e) {
      LOGGER.warn("Failed to clear history data on 127.0.0.1:{}: ", port, e);
    } finally {
      try {
        if (conn != null) {
          conn.close();
        }
      } catch (SQLException e) {
        LOGGER.error("Failed to close connection: ", e);
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
    return "\"" + name + "\"";
  }

  // 终止所有连接的逻辑
  private void terminateConnectionsForDatabase(String databaseName, int port) throws SQLException {
    String query = String.format("SELECT sess_id FROM v$sessions;");
    Connection conn = null;
    conn = connect(port, true, null);
    if (conn == null) {
      LOGGER.error("cannot connect to 127.0.0.1:{}!", port);
      return;
    }
    try (Statement stmt = conn.createStatement();
        ResultSet resultSet = stmt.executeQuery(query)) {
      while (resultSet.next()) {
        int sid = resultSet.getInt("sess_id");
        LOGGER.info("Killing session with SID: {}", sid);
        String killQuery = String.format("sp_close_session(%d)", sid);
        try (Statement killStmt = conn.createStatement()) {
          killStmt.execute(killQuery);
          LOGGER.info("Killed session with SID: {}", sid);
        }
      }
    }
  }
}
