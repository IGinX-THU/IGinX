/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.engine.shared.function.system;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.*;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.List;

public class Avg implements SetMappingFunction {

  public static final String AVG = "avg";

  private static final Avg INSTANCE = new Avg();

  private Avg() {}

  public static Avg getInstance() {
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
    return AVG;
  }

  @Override
  public Row transform(Table table, FunctionParams params) throws Exception {
    Pair<List<Field>, List<Integer>> pair = FunctionUtils.getFieldAndIndices(table, params, this);
    List<Field> fields = table.getHeader().getFields();
    List<Field> targetFields = pair.k;
    List<Integer> indices = pair.v;

    for (Field field : targetFields) {
      if (!DataTypeUtils.isNumber(field.getType())) {
        throw new IllegalArgumentException("only number can calculate average");
      }
    }

    double[] targetSums = new double[targetFields.size()];
    long[] counts = new long[targetFields.size()];
    for (Row row : table.getRows()) {
      for (int i = 0; i < indices.size(); i++) {
        int index = indices.get(i);
        Object value = row.getValue(index);
        if (value == null) {
          continue;
        }
        switch (fields.get(index).getType()) {
          case INTEGER:
            targetSums[i] += (int) value;
            break;
          case LONG:
            targetSums[i] += (long) value;
            break;
          case FLOAT:
            targetSums[i] += (float) value;
            break;
          case DOUBLE:
            targetSums[i] += (double) value;
            break;
          default:
            throw new IllegalStateException(
                "Unexpected field type: " + fields.get(index).getType().toString());
        }
        counts[i]++;
      }
    }
    Object[] targetValues = new Object[targetFields.size()];
    for (int i = 0; i < targetValues.length; i++) {
      targetValues[i] = targetSums[i] / counts[i];
    }
    return new Row(new Header(targetFields), targetValues);
  }
}
