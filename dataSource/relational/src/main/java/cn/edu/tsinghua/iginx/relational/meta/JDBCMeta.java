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
package cn.edu.tsinghua.iginx.relational.meta;

import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.datatype.transformer.IDataTypeTransformer;
import cn.edu.tsinghua.iginx.relational.datatype.transformer.JDBCDataTypeTransformer;
import cn.edu.tsinghua.iginx.relational.datatype.transformer.OracleLDataTypeTransformer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class JDBCMeta extends AbstractRelationalMeta {
  private final char quote;
  private final String defaultDatabaseName;

  private final String driverClass;

  private final IDataTypeTransformer dataTypeTransformer;

  private final List<String> systemDatabaseName;

  private final String databaseQuerySql;

  private final String databaseDropStatement;

  private final String databaseCreateStatement;

  private final String grantPrivilegesStatement;

  private final String createTableStatement;

  private final String alterTableAddColumnStatement;

  private final String alterTableDropColumnStatement;

  private final String queryTableStatement;

  private final String queryTableWithoutKeyStatement;

  private final String insertTableStatement;

  private final String updateTableStatement;

  private final String deleteTableStatement;

  private final boolean needQuote;

  private final String schemaPattern;

  private final String upsertStatement;

  private final String upsertConflictStatement;

  private final boolean isSupportFullJoin;

  private final String regexpOp;

  private final String notRegexOp;

  private final boolean jdbcSupportBackslash;

  public JDBCMeta(StorageEngineMeta meta, Properties properties) {
    super(meta);
    quote = properties.getProperty("quote").charAt(0);
    driverClass = properties.getProperty("driver_class");
    defaultDatabaseName = properties.getProperty("default_database");
    if (meta.getExtraParams().get("engine").equalsIgnoreCase("oracle")) {
      dataTypeTransformer = OracleLDataTypeTransformer.getInstance();
    } else {
      dataTypeTransformer = new JDBCDataTypeTransformer(properties);
    }
    systemDatabaseName = Arrays.asList(properties.getProperty("system_databases").split(","));
    databaseQuerySql = properties.getProperty("database_query_sql");
    databaseDropStatement = properties.getProperty("drop_database_statement");
    databaseCreateStatement = properties.getProperty("create_database_statement");
    grantPrivilegesStatement = properties.getProperty("grant_privileges_statement");
    createTableStatement = properties.getProperty("create_table_statement");
    alterTableAddColumnStatement = properties.getProperty("alter_table_add_column_statement");
    alterTableDropColumnStatement = properties.getProperty("alter_table_drop_column_statement");
    queryTableStatement = properties.getProperty("query_table_statement");
    queryTableWithoutKeyStatement = properties.getProperty("query_table_without_key_statement");
    insertTableStatement = properties.getProperty("insert_table_statement");
    updateTableStatement = properties.getProperty("update_table_statement");
    deleteTableStatement = properties.getProperty("delete_table_statement");
    needQuote = Boolean.parseBoolean(properties.getProperty("jdbc_need_quote"));
    schemaPattern = properties.getProperty("schema_pattern");
    upsertStatement = properties.getProperty("upsert_statement");
    upsertConflictStatement = properties.getProperty("upsert_conflict_statement");
    isSupportFullJoin = Boolean.parseBoolean(properties.getProperty("is_support_full_join"));
    regexpOp = properties.getProperty("regex_like_symbol");
    notRegexOp = properties.getProperty("not_regex_like_symbol");
    jdbcSupportBackslash =
        Boolean.parseBoolean(properties.getProperty("jdbc_support_special_char"));
  }

  @Override
  public char getQuote() {
    return quote;
  }

  @Override
  public String getDefaultDatabaseName() {
    return defaultDatabaseName;
  }

  @Override
  public String getDriverClass() {
    return driverClass;
  }

  @Override
  public IDataTypeTransformer getDataTypeTransformer() {
    return dataTypeTransformer;
  }

  @Override
  public List<String> getSystemDatabaseName() {
    return systemDatabaseName;
  }

  @Override
  public String getDatabaseQuerySql() {
    return databaseQuerySql;
  }

  @Override
  public String getDropDatabaseStatement() {
    return databaseDropStatement;
  }

  @Override
  public String getCreateDatabaseStatement() {
    return databaseCreateStatement;
  }

  @Override
  public String getGrantPrivilegesStatement() {
    return grantPrivilegesStatement;
  }

  @Override
  public String getCreateTableStatement() {
    return createTableStatement;
  }

  @Override
  public String getAlterTableDropColumnStatement() {
    return alterTableDropColumnStatement;
  }

  @Override
  public String getAlterTableAddColumnStatement() {
    return alterTableAddColumnStatement;
  }

  @Override
  public String getQueryTableStatement() {
    return queryTableStatement;
  }

  @Override
  public String getQueryTableWithoutKeyStatement() {
    return queryTableWithoutKeyStatement;
  }

  @Override
  public String getInsertTableStatement() {
    return insertTableStatement;
  }

  @Override
  public String getUpdateTableStatement() {
    return updateTableStatement;
  }

  @Override
  public String getDeleteTableStatement() {
    return deleteTableStatement;
  }

  @Override
  public boolean jdbcNeedQuote() {
    return needQuote;
  }

  @Override
  public String getSchemaPattern() {
    return schemaPattern;
  }

  @Override
  public String getUpsertStatement() {
    return upsertStatement;
  }

  @Override
  public String getUpsertConflictStatement() {
    return upsertConflictStatement;
  }

  @Override
  public boolean isSupportFullJoin() {
    return isSupportFullJoin;
  }

  @Override
  public String getRegexpOp() {
    return regexpOp;
  }

  @Override
  public String getNotRegexpOp() {
    return notRegexOp;
  }

  @Override
  public boolean jdbcSupportSpecialChar() {
    return jdbcSupportBackslash;
  }

  public StorageEngineMeta getStorageEngineMeta() {
    return meta;
  }
}
