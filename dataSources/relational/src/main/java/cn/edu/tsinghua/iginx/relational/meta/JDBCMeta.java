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

  public JDBCMeta(StorageEngineMeta meta, String propertiesPath) throws IOException {
    super(meta);
    properties = new Properties();
    InputStream in = Files.newInputStream(Paths.get(propertiesPath));
    properties.load(in);
  }

  @Override
  public String getDefaultDatabaseName() {
    return properties.getProperty("default_database");
  }

  @Override
  public String getDriverClass() {
    return properties.getProperty("driver_class");
  }

  @Override
  public IDataTypeTransformer getDataTypeTransformer() {
    return new JDBCDataTypeTransformer(properties);
  }

  @Override
  public List<String> getSystemDatabaseName() {
    return Arrays.asList(properties.getProperty("system_databases").split(","));
  }

  @Override
  public String getDatabaseQuerySql() {
    return properties.getProperty("database_query_sql");
  }
}
