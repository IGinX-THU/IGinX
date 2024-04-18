package cn.edu.tsinghua.iginx.relational.meta;

import static cn.edu.tsinghua.iginx.relational.tools.Constants.PASSWORD;
import static cn.edu.tsinghua.iginx.relational.tools.Constants.USERNAME;

import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.tools.IDataTypeTransformer;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public abstract class AbstractRelationalMeta {

  public AbstractRelationalMeta() {}

  /**
   * 获取ENGINE的默认数据库名称
   *
   * @return ENGINE的默认数据库名称
   */
  public abstract String getDefaultDatabaseName();

  /**
   * 获取ENGINE的驱动类
   *
   * @return ENGINE的驱动类
   */
  public abstract String getDriverClass();

  /** 对ENGINE设置连接超时 */
  public abstract void setConnectionTimeout(Statement stmt) throws SQLException;

  /**
   * 获取ENGINE的数据类型转换器
   *
   * @return ENGINE的数据类型转换器
   */
  public abstract IDataTypeTransformer getDataTypeTransformer();

  /**
   * 该函数要求子类维护一个数据库连接池，根据数据库名称获取一个数据库连接
   *
   * @param databaseName 数据库名称
   * @return 数据库连接
   */
  public abstract Connection getConnectionFromPool(String databaseName, StorageEngineMeta meta);

  /**
   * 使用JDBC获取该ENGINE的所有数据库名称
   *
   * @return 数据库名称列表
   */
  public abstract List<String> getDatabaseNames(StorageEngineMeta meta, Connection connection);

  protected String getUrl(String databaseName, StorageEngineMeta meta) {
    Map<String, String> extraParams = meta.getExtraParams();
    String username = extraParams.getOrDefault(USERNAME, "");
    String password = extraParams.getOrDefault(PASSWORD, "");
    return String.format(
        "jdbc:postgresql://%s:%s/%s?user=%s&password=%s",
        meta.getIp(), meta.getPort(), databaseName, username, password);
  }
}
