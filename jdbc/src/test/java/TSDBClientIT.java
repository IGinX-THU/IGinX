/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.jdbc.tsdb.IginxConnection;
import java.sql.*;
import java.util.HashMap;
import java.util.Properties;
import java.util.Vector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TSDBClientIT {

  private Connection connection;

  @Before
  public void before() throws SQLException {
    String url =
        "jdbc:iginx:tsdb:;database=benchmark;lindorm.tsdb.driver.socket.timeout=30000;lindorm.tsdb.driver.connect.timeout=30000;lindorm.tsdb.driver.http.compression=false";

    Properties info = new Properties();
    info.put("iginx.authorities", "192.168.100.101:6888#192.168.100.101:7888#192.168.100.101:6888");

    connection = DriverManager.getConnection(url, info);
    Assert.assertNotNull(connection);
  }

  @After
  public void after() throws SQLException {
    try {
      ((IginxConnection) connection).getSessionPool().executeSql("clear data;");
    } catch (SessionException e) {
      throw new SQLException(e);
    }
    connection.close();
  }

  @Test
  public void testConnect() {}

  private static String getInsertSql(String table, String columns, String variables) {
    return "insert into " + table + "(" + columns + ") values (" + variables + ")";
  }

  private static final String INSERT_SQL =
      "insert into sensor(device_id,time,field0) values (?,?,?)";

  @Test
  public void testDecribe() throws SQLException {
    StringBuilder preparedVariables = new StringBuilder();
    StringBuilder preparedColumns = new StringBuilder();

    String tableName = "sensor";
    String describeTable = "describe table " + tableName;
    try (Statement describeStmt = connection.createStatement();
        ResultSet tableSchema = describeStmt.executeQuery(describeTable)) {
      int columns = 0;
      while (tableSchema.next()) {
        columns++;
        if (columns == 1) {
          preparedColumns.append(tableSchema.getString(1));
          preparedVariables.append("?");
        } else {
          preparedColumns.append(",");
          preparedColumns.append(tableSchema.getString(1));
          preparedVariables.append(",?");
        }
      }
    }

    Assert.assertEquals("device_id,time,field0", preparedColumns.toString());
    Assert.assertEquals("?,?,?", preparedVariables.toString());

    String insertSql =
        getInsertSql(tableName, preparedColumns.toString(), preparedVariables.toString());
    Assert.assertEquals(INSERT_SQL, insertSql);
  }

  @Test
  public void testInsert() throws SQLException {
    try (PreparedStatement preparedInsertStmt = connection.prepareStatement(INSERT_SQL)) {
      for (int timestamp = 0; timestamp < 20; timestamp++) {
        int field = timestamp % 4;
        String deviceId = String.format("tpc11:prekey%d", field);
        String value = String.format("value%d", timestamp);
        preparedInsertStmt.setString(1, deviceId);
        preparedInsertStmt.setLong(2, timestamp);
        preparedInsertStmt.setString(3, value);
        preparedInsertStmt.addBatch();
      }
      preparedInsertStmt.executeBatch();
    }
  }

  @Test
  public void testScan() throws SQLException {
    testInsert();
    String deviceId = "tpc11:prekey1";
    long timestamp = 10;
    String sqlQueryStr =
        "SELECT * FROM sensor WHERE device_id = '"
            + deviceId
            + "' and time "
            + " between "
            + timestamp
            + " and "
            + (timestamp + 5000L);
    Vector<HashMap<String, String>> result = new Vector<>();
    try (Statement queryStmt = connection.createStatement();
        ResultSet resultSet = queryStmt.executeQuery(sqlQueryStr)) {
      ResultSetMetaData metaData = resultSet.getMetaData();
      while (resultSet.next()) {
        HashMap<String, String> tuple = new HashMap<>();
        for (int columnIndex = 1; columnIndex <= metaData.getColumnCount(); columnIndex++) {
          tuple.put(
              metaData.getColumnName(columnIndex).toLowerCase(), resultSet.getString(columnIndex));
        }
        result.add(tuple);
      }
    }
    Vector<HashMap<String, String>> expected = new Vector<>();
    for (int key = 10; key < 20; key++) {
      int field = key % 4;
      if (field != 1) {
        continue;
      }
      HashMap<String, String> tuple = new HashMap<>();
      tuple.put("device_id", String.format("tpc11:prekey%d", field));
      tuple.put("time", String.valueOf(key));
      tuple.put("field0", String.format("value%d", key));
      expected.add(tuple);
    }
    Assert.assertEquals(expected, result);
  }
}
