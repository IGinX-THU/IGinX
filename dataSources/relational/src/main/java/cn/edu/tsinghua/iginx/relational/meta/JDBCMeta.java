package cn.edu.tsinghua.iginx.relational.meta;

import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.tools.IDataTypeTransformer;
import cn.edu.tsinghua.iginx.relational.tools.JDBCDataTypeTransformer;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class JDBCMeta extends AbstractRelationalMeta {

  private final char quote;
  private final String defaultDatabaseName;

  private final String driverClass;

  private final JDBCDataTypeTransformer dataTypeTransformer;

  private final List<String> systemDatabaseName;

  private final String databaseQuerySql;

  private final String databaseDropStatement;

  private final boolean needQuote;

  private final String schemaPattern;

  private final String upsertStatement;

  private final String upsertConflictStatement;

  private final String useDatabaseStatement;

  public JDBCMeta(StorageEngineMeta meta, String propertiesPath) throws IOException {
    super(meta);
    Properties properties = new Properties();
    InputStream in = Files.newInputStream(Paths.get(propertiesPath));
    properties.load(in);

    quote = properties.getProperty("quote").charAt(0);
    driverClass = properties.getProperty("driver_class");
    defaultDatabaseName = properties.getProperty("default_database");
    dataTypeTransformer = new JDBCDataTypeTransformer(properties);
    systemDatabaseName = Arrays.asList(properties.getProperty("system_databases").split(","));
    databaseQuerySql = properties.getProperty("database_query_sql");
    databaseDropStatement = properties.getProperty("drop_database_statement");
    needQuote = Boolean.parseBoolean(properties.getProperty("jdbc_need_quote"));
    schemaPattern = properties.getProperty("schema_pattern");
    upsertStatement = properties.getProperty("upsert_statement");
    upsertConflictStatement = properties.getProperty("upsert_conflict_statement");
    useDatabaseStatement = properties.getProperty("use_database_statement");
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
  public String getUseDatabaseStatement() {
    return useDatabaseStatement;
  }
}
