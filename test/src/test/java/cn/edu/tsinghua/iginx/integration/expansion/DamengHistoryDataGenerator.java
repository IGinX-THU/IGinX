package cn.edu.tsinghua.iginx.integration.expansion;

import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class DamengHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(DamengHistoryDataGenerator.class);

  private static final char SEPARATOR = '.';

  private static final String QUERY_DATABASES_STATEMENT =
          "SELECT SCHEMA_NAME FROM DBA_SCHEMAS WHERE SCHEMA_NAME NOT IN ('SYSDBA', 'SYSAUDITOR')";

  private static final String CREATE_DATABASE_STATEMENT =
          "CREATE SCHEMA %s";

  private static final String GRANT_DATABASE_STATEMENT =
          "GRANT CONNECT, RESOURCE TO %s";

  private static final String CREATE_TABLE_STATEMENT =
          "CREATE TABLE %s.%s (%s)";

  private static final String INSERT_STATEMENT =
          "INSERT INTO %s.%s VALUES %s";

  private static final String DROP_DATABASE_STATEMENT =
          "DROP SCHEMA %s CASCADE";

  public DamengHistoryDataGenerator() {
    Constant.oriPort = 5236;
    Constant.expPort = 5237;
    Constant.readOnlyPort = 5238;
  }

  private Connection connect(int port, boolean useSystemDatabase, String databaseName) {
    try {
      String url = String.format(
              "jdbc:dm://172.17.0.2:%d/%s",
              port, databaseName == null ? "DMDB" : databaseName);
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
            createTableStr.append(getQuotName(columnName)).append(" ").append(toDamengSQL(dataType)).append(", ");
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
}