package cn.edu.tsinghua.iginx.relational.meta;

import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.tools.IDataTypeTransformer;
import java.util.List;

public class JDBCMeta extends AbstractRelationalMeta {

  public JDBCMeta(StorageEngineMeta meta) {
    super(meta);
  }

  @Override
  public String getDefaultDatabaseName() {
    return meta.getExtraParams().get("default_database");
  }

  @Override
  public String getDriverClass() {
    return meta.getExtraParams().get("driver_class");
  }

  @Override
  public IDataTypeTransformer getDataTypeTransformer() {
    return null;
  }

  @Override
  public List<String> getSystemDatabaseName() {
    return null;
  }
}
