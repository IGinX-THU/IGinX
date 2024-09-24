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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.engine.shared.function.system;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.*;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Count implements SetMappingFunction {

  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(Count.class);

  public static final String COUNT = "count";

  private static final Count INSTANCE = new Count();

  private Count() {}

  public static Count getInstance() {
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
    return COUNT;
  }

  @Override
  public Row transform(Table table, FunctionParams params) throws Exception {
    Pair<List<Field>, List<Integer>> pair = FunctionUtils.getFieldAndIndices(table, params, this);
    List<Field> targetFields = pair.k;
    List<Integer> indices = pair.v;

    long[] counts = new long[targetFields.size()];
    for (Row row : table.getRows()) {
      Object[] values = row.getValues();
      for (int i = 0; i < indices.size(); i++) {
        int index = indices.get(i);
        if (values[index] != null) {
          counts[i]++;
        }
      }
    }
    Object[] targetValues = new Object[targetFields.size()];
    for (int i = 0; i < counts.length; i++) {
      targetValues[i] = counts[i];
    }
    return new Row(new Header(targetFields), targetValues);
  }
}
