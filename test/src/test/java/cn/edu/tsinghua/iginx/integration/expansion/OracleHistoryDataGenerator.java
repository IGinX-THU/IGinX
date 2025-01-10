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
package cn.edu.tsinghua.iginx.integration.expansion.oracle;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.sql.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(OracleHistoryDataGenerator.class);

  private static final char SEPARATOR = '.';

  private static final String QUERY_DATABASES_STATEMENT =
      "select distinct s.owner as DATNAME from dba_segments s where s.tablespace_name ='USERS'";

  private static final String CREATE_DATABASE_STATEMENT =
      "CREATE USER %s DEFAULT TABLESPACE users TEMPORARY TABLESPACE temp";

  private static final String GRANT_DATABASE_STATEMENT =
      "GRANT CREATE SESSION,CREATE TABLE,RESOURCE,UNLIMITED TABLESPACE TO %s";

  private static final String CREATE_TABLE_STATEMENT =
      "CREATE TABLE %s.%s (%s)"; // "CREATE TABLE %s (%s);";

  private static final String INSERT_STATEMENT =
      "INSERT INTO %s.%s VALUES %s"; // "INSERT INTO %s VALUES %s;";

  private static final String DROP_DATABASE_STATEMENT =
      "DROP USER %s CASCADE"; // "DROP DATABASE IF EXISTS `%s`;";

  public OracleHistoryDataGenerator() {
    Constant.oriPort = 1521;
    Constant.expPort = 1522;
    Constant.readOnlyPort = 1523;
  }

  private Connection connect(int port, boolean useSystemDatabase, String databaseName) {
    try {
      String url;

      // TODO 获取docker container 虚拟IP 172.17.0.2
      url =
          String.format(
              "jdbc:oracle:thin:system/Oracle123@172.17.0.2:%d/%s",
              port, databaseName == null ? "ORCLPDB" : databaseName);
      Class.forName("oracle.jdbc.driver.OracleDriver");
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
        String grantDatabaseSql =
            String.format(GRANT_DATABASE_STATEMENT, getQuotName(databaseName));
        try {
          LOGGER.info("create database with stmt: {}", createDatabaseSql);
          stmt.execute(createDatabaseSql);
          stmt.execute(grantDatabaseSql);
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
            createTableStr.append(getQuotName(columnName));
            createTableStr.append(" ");
            createTableStr.append(toOracleSQL(dataType));
            createTableStr.append(", ");
            columnStr.append(getQuotName(columnName)).append(",");
          }
          stmt.execute(
              String.format(
                  CREATE_TABLE_STATEMENT,
                  getQuotName(databaseName),
                  getQuotName(tableName),
                  createTableStr.substring(0, createTableStr.length() - 2)));

          columnStr = new StringBuilder(columnStr.substring(0, columnStr.length() - 1));
          int start = 0, end = 0, step = 0;
          while (end < valuesList.size()) {
            step = Math.min(valuesList.size() - end, 1000);
            end += step;
            StringBuilder insertAllSql = new StringBuilder("INSERT ALL ");
            StringBuilder insertStr = new StringBuilder();
            List<List<Object>> lists = valuesList.subList(start, end);
            for (List<Object> values : lists) {
              insertStr
                  .append("INTO ")
                  .append(getQuotName(databaseName))
                  .append(SEPARATOR)
                  .append(getQuotName(tableName))
                  .append(" (")
                  .append(columnStr)
                  .append(") VALUES ");
              insertStr.append("(");
              for (Integer index : item.getValue()) {
                if (dataTypeList.get(index) == DataType.BINARY) {
                  insertStr.append("'").append(new String((byte[]) values.get(index))).append("'");
                } else {
                  insertStr.append(values.get(index));
                }
                insertStr.append(", ");
              }
              insertStr = new StringBuilder(insertStr.substring(0, insertStr.length() - 2));
              insertStr.append(") ");
            }
            insertAllSql.append(insertStr).append("SELECT * FROM dual");
            stmt.execute(insertAllSql.toString());
            start = end;
          }
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
      ResultSet databaseSet = stmt.executeQuery(QUERY_DATABASES_STATEMENT);
      Statement dropDatabaseStatement = conn.createStatement();

      while (databaseSet.next()) {
        String databaseName = databaseSet.getString("DATNAME");
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

  private static String toOracleSQL(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return "NUMBER(1)";
      case INTEGER:
        return "NUMBER(10)";
      case LONG:
        return "NUMBER(19)";
      case FLOAT:
        return "FLOAT";
      case DOUBLE:
        return "FLOAT";
      case BINARY:
      default:
        return "VARCHAR2(4000)";
    }
  }

  private static String getQuotName(String name) {
    return "\"" + name + "\"";
  }
}
