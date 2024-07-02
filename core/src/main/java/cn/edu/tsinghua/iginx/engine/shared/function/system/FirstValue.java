package cn.edu.tsinghua.iginx.engine.shared.function.system;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.*;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.List;

public class FirstValue implements SetMappingFunction {

  public static final String FIRST_VALUE = "first_value";

  private static final FirstValue INSTANCE = new FirstValue();

  private FirstValue() {}

  public static FirstValue getInstance() {
    return INSTANCE;
  }

  @Override
  public FunctionType getFunctionType() {
    return FunctionType.System;
  }

  @Override
  public MappingType getMappingType() {
    return MappingType.SetMapping;
  }

  @Override
  public String getIdentifier() {
    return FIRST_VALUE;
  }

  @Override
  public Row transform(Table table, FunctionParams params) throws Exception {
    Pair<List<Field>, List<Integer>> pair = FunctionUtils.getFieldAndIndices(table, params, this);
    List<Field> targetFields = pair.k;
    List<Integer> indices = pair.v;

    Object[] targetValues = new Object[targetFields.size()];
    for (Row row : table.getRows()) {
      for (int i = 0; i < indices.size(); i++) {
        Object value = row.getValue(indices.get(i));
        if (targetValues[i] != null) { // 找到第一个非空值之后，后续不再找了
          continue;
        }
        targetValues[i] = value;
      }
    }
    return new Row(new Header(targetFields), targetValues);
  }
}
