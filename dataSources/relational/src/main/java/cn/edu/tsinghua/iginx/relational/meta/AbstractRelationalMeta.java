package cn.edu.tsinghua.iginx.relational.meta;

import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.tools.IDataTypeTransformer;
import java.util.List;

public abstract class AbstractRelationalMeta {

  protected StorageEngineMeta meta;

  public AbstractRelationalMeta(StorageEngineMeta meta) {
    this.meta = meta;
  }

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

  /**
   * 获取ENGINE的数据类型转换器
   *
   * @return ENGINE的数据类型转换器
   */
  public abstract IDataTypeTransformer getDataTypeTransformer();

  /**
   * 获取系统数据库名称，用于忽略，如pg的template0,template1
   *
   * @return 系统数据库名称
   */
  public abstract List<String> getSystemDatabaseName();
}
