/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.relational.meta;

import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.tools.IDataTypeTransformer;
import cn.edu.tsinghua.iginx.relational.tools.PostgreSQLDataTypeTransformer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.postgresql.ds.PGConnectionPoolDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgreSQLMeta extends AbstractRelationalMeta {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLMeta.class);

  private final Map<String, PGConnectionPoolDataSource> connectionPoolMap =
      new ConcurrentHashMap<>();

  private static final String TIMEOUT_SETTING_SQL =
      "alter system set idle_in_transaction_session_timeout='1min'";

  private static final String DRIVER_CLASS = "org.postgresql.Driver";

  private static final String DEFAULT_DATABASE_NAME = "postgres";

  public static final String QUERY_DATABASES_STATEMENT = "SELECT datname FROM pg_database;";

  private static final PostgreSQLDataTypeTransformer dataTypeTransformer =
      PostgreSQLDataTypeTransformer.getInstance();

  public PostgreSQLMeta() {
    super();
  }

  @Override
  public String getDefaultDatabaseName() {
    return DEFAULT_DATABASE_NAME;
  }

  @Override
  public String getDriverClass() {
    return DRIVER_CLASS;
  }

  @Override
  public void setConnectionTimeout(Statement statement) throws SQLException {
    statement.executeUpdate(TIMEOUT_SETTING_SQL);
  }

  @Override
  public IDataTypeTransformer getDataTypeTransformer() {
    return dataTypeTransformer;
  }

  @Override
  public Connection getConnectionFromPool(String databaseName, StorageEngineMeta meta) {
    try {
      if (connectionPoolMap.containsKey(databaseName)) {
        return connectionPoolMap.get(databaseName).getConnection();
      }
      PGConnectionPoolDataSource connectionPool = new PGConnectionPoolDataSource();
      connectionPool.setUrl(getUrl(databaseName, meta));
      connectionPoolMap.put(databaseName, connectionPool);
      return connectionPool.getConnection();
    } catch (SQLException e) {
      LOGGER.error(String.format("cannot get connection for database {}: %s", databaseName));
      return null;
    }
  }

  @Override
  public List<String> getDatabaseNames(StorageEngineMeta meta, Connection connection) {
    List<String> databaseNames = new ArrayList<>();
    try {
      Statement stmt = connection.createStatement();
      ResultSet databaseSet = stmt.executeQuery(QUERY_DATABASES_STATEMENT);
      while (databaseSet.next()) {
        databaseNames.add(databaseSet.getString("DATNAME"));
      }
    } catch (SQLException e) {
      LOGGER.error(String.format("failed to get databases of %s", meta));
    }

    return databaseNames;
  }
}
