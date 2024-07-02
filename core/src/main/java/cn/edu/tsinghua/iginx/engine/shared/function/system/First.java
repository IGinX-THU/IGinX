package cn.edu.tsinghua.iginx.engine.shared.function.system;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.*;

public class First implements MappingFunction {

  public static final String FIRST = "first";

  private static final First INSTANCE = new First();

  private First() {}

  public static First getInstance() {
    return INSTANCE;
  }

  @Override
  public FunctionType getFunctionType() {
    return FunctionType.System;
  }

  @Override
  public MappingType getMappingType() {
    return MappingType.Mapping;
  }

  @Override
  public String getIdentifier() {
    return FIRST;
  }

  @Override
  public RowStream transform(Table table, FunctionParams params) throws Exception {
    return FunctionUtils.firstOrLastTransform(table, params, this);
  }
}
