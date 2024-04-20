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

import static cn.edu.tsinghua.iginx.relational.tools.Constants.KEY_NAME;

import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.tools.IDataTypeTransformer;
import cn.edu.tsinghua.iginx.relational.tools.PostgreSQLDataTypeTransformer;
import java.util.ArrayList;
import java.util.Arrays;
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

  private static final PostgreSQLDataTypeTransformer dataTypeTransformer =
      PostgreSQLDataTypeTransformer.getInstance();

  private static final List<String> SYSTEM_DATABASE_NAME =
      new ArrayList<>(Arrays.asList("template0", "template1"));

  public PostgreSQLMeta(StorageEngineMeta meta) {
    super(meta);
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
  public IDataTypeTransformer getDataTypeTransformer() {
    return dataTypeTransformer;
  }

  @Override
  public List<String> getSystemDatabaseName() {
    return SYSTEM_DATABASE_NAME;
  }

  @Override
  public String getDatabaseQuerySql() {
    return "SELECT datname FROM pg_database;";
  }

  @Override
  public char getQuote() {
    return '"';
  }

  public String getDropDatabaseStatement() {
    return "DROP DATABASE IF EXISTS %s WITH FORCE;";
  }

  @Override
  public boolean jdbcNeedQuote() {
    return false;
  }

  @Override
  public String getSchemaPattern() {
    return "public";
  }

  @Override
  public String getUpsertStatement() {
    return " ON CONFLICT (" + getQuote() + KEY_NAME + getQuote() + ") DO UPDATE SET ";
  }

  @Override
  public String getUpsertConflictStatement() {
    return "%s = EXCLUDED.%s";
  }
}
