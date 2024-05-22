package cn.edu.tsinghua.iginx.integration.expansion.mysql;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(MySQLHistoryDataGenerator.class);

  private static final char SEPARATOR = '.';

  private static final String QUERY_DATABASES_STATEMENT =
      "SELECT SCHEMA_NAME AS datname FROM information_schema.schemata;";

  private static final String CREATE_DATABASE_STATEMENT = "CREATE DATABASE `%s`;";

  private static final String CREATE_TABLE_STATEMENT = "CREATE TABLE %s (%s);";

  private static final String INSERT_STATEMENT = "INSERT INTO %s VALUES %s;";

  private static final String DROP_DATABASE_STATEMENT = "DROP DATABASE IF EXISTS `%s`;";

  public MySQLHistoryDataGenerator() {
    Constant.oriPort = 3306;
    Constant.expPort = 3307;
    Constant.readOnlyPort = 3308;
  }

  private Connection connect(int port, boolean useSystemDatabase, String databaseName) {
    try {
      String url;
      if (useSystemDatabase) {
        url = String.format("jdbc:mysql://127.0.0.1:%d/", port);
      } else {
        url = String.format("jdbc:mysql://127.0.0.1:%d/%s", port, databaseName);
      }
      Class.forName("com.mysql.cj.jdbc.Driver");
      return DriverManager.getConnection(url, "root", null);
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
        String createDatabaseSql = String.format(CREATE_DATABASE_STATEMENT, databaseName);
        try {
          LOGGER.info("create database with stmt: {}", createDatabaseSql);
          stmt.execute(createDatabaseSql);
        } catch (SQLException e) {
          LOGGER.info("database {} exists!", databaseName);
        }
        stmt.close();

        Connection conn = connect(port, false, databaseName);
        stmt = conn.createStatement();
        for (Map.Entry<String, List<Integer>> item : entry.getValue().entrySet()) {
          String tableName = item.getKey();
          StringBuilder createTableStr = new StringBuilder();
          for (Integer index : item.getValue()) {
            String path = pathList.get(index);
            String columnName = path.substring(path.lastIndexOf(SEPARATOR) + 1);
            DataType dataType = dataTypeList.get(index);
            createTableStr.append(getQuotName(columnName));
            createTableStr.append(" ");
            createTableStr.append(toPostgreSQL(dataType));
            createTableStr.append(", ");
          }
          stmt.execute(
              String.format(
                  CREATE_TABLE_STATEMENT,
                  getQuotName(tableName),
                  createTableStr.substring(0, createTableStr.length() - 2)));

          StringBuilder insertStr = new StringBuilder();
          for (List<Object> values : valuesList) {
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
            insertStr.append("), ");
          }
          stmt.execute(
              String.format(
                  INSERT_STATEMENT,
                  getQuotName(tableName),
                  insertStr.substring(0, insertStr.length() - 2)));
        }
        stmt.close();
        conn.close();
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
  public void clearHistoryDataForGivenPort(int port) {
    Connection conn = null;
    try {
      conn = connect(port, true, null);
      Statement stmt = conn.createStatement();
      ResultSet databaseSet = stmt.executeQuery(QUERY_DATABASES_STATEMENT);
      Statement dropDatabaseStatement = conn.createStatement();

      while (databaseSet.next()) {
        String databaseName = databaseSet.getString("DATNAME");
        if (databaseName.equalsIgnoreCase("performance_schema")
            || databaseName.equalsIgnoreCase("information_schema")
            || databaseName.equalsIgnoreCase("mysql")
            || databaseName.equalsIgnoreCase("sys")) {
          continue;
        }
        dropDatabaseStatement.addBatch(String.format(DROP_DATABASE_STATEMENT, databaseName));
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

  private static String toPostgreSQL(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return "BOOLEAN";
      case INTEGER:
        return "INTEGER";
      case LONG:
        return "BIGINT";
      case FLOAT:
        return "REAL";
      case DOUBLE:
        return "DOUBLE PRECISION";
      case BINARY:
      default:
        return "TEXT";
    }
  }

  private static String getQuotName(String name) {
    return "`" + name + "`";
  }
}
