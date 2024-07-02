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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.relational.meta;

import static cn.edu.tsinghua.iginx.relational.tools.Constants.KEY_NAME;

import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.datatype.transformer.IDataTypeTransformer;
import cn.edu.tsinghua.iginx.relational.datatype.transformer.PostgreSQLDataTypeTransformer;
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
      new ArrayList<>(Arrays.asList("template0", "template1", "readme_to_recover"));

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
    return "DROP DATABASE IF EXISTS %s WITH (FORCE);";
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

  @Override
  public boolean isSupportFullJoin() {
    return true;
  }

  @Override
  public String getRegexpOp() {
    return "~";
  }

  @Override
  public boolean jdbcSupportSpecialChar() {
    return true;
  }
}
