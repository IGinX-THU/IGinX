package cn.edu.tsinghua.iginx.engine.shared.function.system;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.*;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.List;

public class Min implements SetMappingFunction {

  public static final String MIN = "min";

  private static final Min INSTANCE = new Min();

  private Min() {}

  public static Min getInstance() {
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
    return MIN;
  }

  @Override
  public Row transform(Table table, FunctionParams params) throws Exception {
    Pair<List<Field>, List<Integer>> pair = FunctionUtils.getFieldAndIndices(table, params, this);
    List<Field> targetFields = pair.k;
    List<Integer> indices = pair.v;
    Object[] targetValues = new Object[targetFields.size()];
    for (Row row : table.getRows()) {
      Object[] values = row.getValues();
      for (int i = 0; i < indices.size(); i++) {
        Object value = values[indices.get(i)];
        if (targetValues[i] == null) {
          targetValues[i] = value;
        } else {
          if (value != null
              && ValueUtils.compare(targetValues[i], value, targetFields.get(i).getType()) > 0) {
            targetValues[i] = value;
          }
        }
      }
    }
    return new Row(new Header(targetFields), targetValues);
  }
}
