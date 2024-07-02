package cn.edu.tsinghua.iginx.engine.shared.function;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;

public interface RowMappingFunction extends Function {

  Row transform(Row row, FunctionParams params) throws Exception;
}
