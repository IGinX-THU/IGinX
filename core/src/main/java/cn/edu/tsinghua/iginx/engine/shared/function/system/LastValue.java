package cn.edu.tsinghua.iginx.engine.shared.function.system;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.*;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.List;

public class LastValue implements SetMappingFunction {

  public static final String LAST_VALUE = "last_value";

  private static final LastValue INSTANCE = new LastValue();

  private LastValue() {}

  public static LastValue getInstance() {
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
    return LAST_VALUE;
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
        if (value == null) {
          continue;
        }
        targetValues[i] = value;
      }
    }
    return new Row(new Header(targetFields), targetValues);
  }
}
