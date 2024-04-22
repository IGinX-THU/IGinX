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

  private final Properties properties;

  private final char quote;
  private String defaultDatabaseName;

  private String driverClass;

  private JDBCDataTypeTransformer dataTypeTransformer;

  private List<String> systemDatabaseName;

  private String databaseQuerySql;

  private String databaseDropStatement;

  private boolean needQuote;

  private String schemaPattern;

  private String upsertStatement;

  private String upsertConflictStatement;

  public JDBCMeta(StorageEngineMeta meta, String propertiesPath) throws IOException {
    super(meta);
    properties = new Properties();
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
}
