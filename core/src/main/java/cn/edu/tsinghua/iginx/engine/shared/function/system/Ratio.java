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

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.RowMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Collections;

public class Ratio implements RowMappingFunction {

  public static final String RATIO = "ratio";

  private static final Ratio INSTANCE = new Ratio();

  private Ratio() {}

  public static Ratio getInstance() {
    return INSTANCE;
  }

  @Override
  public FunctionType getFunctionType() {
    return FunctionType.System;
  }

  @Override
  public MappingType getMappingType() {
    return MappingType.RowMapping;
  }

  @Override
  public String getIdentifier() {
    return RATIO;
  }

  @Override
  public Row transform(Row row, FunctionParams params) throws Exception {
    if (params.getPaths() == null || params.getPaths().size() != 2) {
      throw new IllegalArgumentException("unexpected params for ratio.");
    }

    String pathA = params.getPaths().get(0);
    String pathB = params.getPaths().get(1);

    Value valueA = row.getAsValue(pathA);
    Value valueB = row.getAsValue(pathB);
    if (valueA == null || valueB == null) {
      return Row.EMPTY_ROW;
    }

    valueA = ValueUtils.transformToDouble(valueA);
    valueB = ValueUtils.transformToDouble(valueB);
    if (valueB.getDoubleV() == 0.0) {
      return Row.EMPTY_ROW;
    }

    double ret = valueA.getDoubleV() / valueB.getDoubleV();

    Header newHeader =
        row.getHeader().hasKey()
            ? new Header(
                Field.KEY,
                Collections.singletonList(
                    new Field("ratio(" + pathA + ", " + pathB + ")", DataType.DOUBLE)))
            : new Header(
                Collections.singletonList(
                    new Field("ratio(" + pathA + ", " + pathB + ")", DataType.DOUBLE)));

    return new Row(newHeader, row.getKey(), new Object[] {ret});
  }
}
