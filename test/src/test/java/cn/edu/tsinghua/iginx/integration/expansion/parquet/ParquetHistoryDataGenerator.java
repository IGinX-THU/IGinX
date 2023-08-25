package cn.edu.tsinghua.iginx.integration.expansion.parquet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetHistoryDataGenerator extends BaseHistoryDataGenerator {

  private static final Logger logger = LoggerFactory.getLogger(ParquetHistoryDataGenerator.class);

  private static final char IGINX_SEPARATOR = '.';

  private static final char PARQUET_SEPARATOR = '$';

  private static final HashMap<Integer, List<String>> parquetParams = new ParquetParams().getParams();

  private static Connection getConnection() {
    try {
      Class.forName("org.duckdb.DuckDBDriver");
      return DriverManager.getConnection("jdbc:duckdb:");
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

//  @Test
//  public void Test() {
//
//    int oriPort = 6669;
//
//    List<String> ORI_PATH_LIST =
//            Arrays.asList("mn.wf05.wt01.status", "mn.wf05.wt01.temperature");
//
//    List<DataType> ORI_DATA_TYPE_LIST =
//            Arrays.asList(DataType.BOOLEAN, DataType.DOUBLE);
//
//    List<List<Object>> ORI_VALUES_LIST =
//            Arrays.asList(Arrays.asList(false, 100.01), Arrays.asList(true, 99.99));
//
//    writeHistoryData(oriPort, ORI_PATH_LIST, ORI_DATA_TYPE_LIST, ORI_VALUES_LIST);
//  }
//
//  @Test
//  public void clear() {
//    int port = 6667;
//    clearHistoryDataForGivenPort(port);
//  }

  @Override
  public void writeHistoryData(int port, List<String> pathList, List<DataType> dataTypeList, List<List<Object>> valuesList) {
    if (!parquetParams.containsKey(port)) {
      logger.error(String.format("writing to unknown port %d.", port));
      return;
    }
    Connection conn = getConnection();
    if (conn == null) {
      logger.error("can't get DuckDB connection.");
      return;
    }
    Statement stmt = null;
    try {
      stmt = conn.createStatement();
      if (stmt == null) {
        logger.error("can't create statement.");
        return;
      }
    } catch (SQLException e) {
      logger.error("statement creation error.");
      return;
    }

    String dir = parquetParams.get(port).get(0);
    String filename = parquetParams.get(port).get(1);
    Path dirPath = Paths.get("../"+dir);
    if (Files.notExists(dirPath)) {
      try {
        Files.createDirectories(dirPath);
      } catch (IOException e) {
        logger.error("can't create data file path {}.", dir);
        return;
      }
    }

    // <columnName, dataType>
    List<Pair<String, String>> columnList = new ArrayList<>();
    try {
      int columnCount = pathList.size();
      // table name does not affect query
      String tableName = dir;
      String columnName;
      String dataType;
      for (int i = 0; i < columnCount; i++) {
        columnName = pathList.get(i).replace(IGINX_SEPARATOR, PARQUET_SEPARATOR);
        dataType = dataTypeList.get(i).toString();

        columnList.add(new Pair(columnName, dataType));
      }

      // create table
      StringBuilder typeListStr = new StringBuilder();
      StringBuilder insertStr;
      for (Pair p :
              columnList) {
        typeListStr.append(p.k).append(" ").append(p.v).append(", ");
      }

      System.out.println(tableName);
      System.out.println(typeListStr);
      stmt.execute(
              String.format(
                      "CREATE TABLE %s (time LONG, %s);",
                      tableName,
                      typeListStr.substring(0, typeListStr.length() - 2))
      );

      //insert value
      insertStr = new StringBuilder();
      int timeCnt = 0;
      for (List<Object> values :
              valuesList) {
        insertStr.append("(").append(timeCnt).append(", ");
        for (int i = 0; i < columnCount; i++) {
          insertStr.append(values.get(i)).append(", ");
        }
        insertStr = new StringBuilder(insertStr.substring(0, insertStr.length() - 2));
        insertStr.append("), ");
        timeCnt++;
      }

      System.out.println(tableName);
      System.out.println(insertStr);
      stmt.execute(
              String.format(
                      "INSERT INTO %s VALUES %s;",
                      tableName,
                      insertStr.substring(0, insertStr.length() - 2))
      );


      Path parquetPath = Paths.get("../" + dir, filename);
      stmt.execute(
              String.format(
                      "COPY (SELECT * FROM %s) TO '%s' (FORMAT 'parquet');",
                      tableName, parquetPath));

    } catch (SQLException e) {
      logger.error("write history data failed.");
      e.printStackTrace();
    }

  }

  @Override
  public void clearHistoryDataForGivenPort(int port) {
    if (!parquetParams.containsKey(port)) {
      logger.error(String.format("delete from unknown port %d.", port));
      return;
    }

    String dir = parquetParams.get(port).get(0);
    String filename = parquetParams.get(port).get(1);
    Path parquetPath = Paths.get("../" + dir, filename);
    File file = new File(parquetPath.toString());

    if (file.exists() && file.isFile()) {
      file.delete();
    } else {
      logger.error("Delete "+dir+"/"+filename+" error: does not exist or is not a file.");
    }
  }
}
