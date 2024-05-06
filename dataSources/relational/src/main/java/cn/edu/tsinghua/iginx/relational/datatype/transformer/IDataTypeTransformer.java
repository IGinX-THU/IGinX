package cn.edu.tsinghua.iginx.relational.datatype.transformer;

import cn.edu.tsinghua.iginx.thrift.DataType;

public interface IDataTypeTransformer {

  public DataType fromEngineType(String dataType);

  public String toEngineType(DataType dataType);
}
