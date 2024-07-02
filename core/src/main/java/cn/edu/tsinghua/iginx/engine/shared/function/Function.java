package cn.edu.tsinghua.iginx.engine.shared.function;

public interface Function {

  FunctionType getFunctionType();

  MappingType getMappingType();

  String getIdentifier();
}
