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
package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class RowTransform extends AbstractUnaryOperator {

  private final List<FunctionCall> functionCallList;

  public RowTransform(Source source, List<FunctionCall> functionCallList) {
    super(OperatorType.RowTransform, source);
    this.functionCallList = new ArrayList<>();
    functionCallList.forEach(
        functionCall -> {
          if (functionCall == null || functionCall.getFunction() == null) {
            throw new IllegalArgumentException("function shouldn't be null");
          }
          if (functionCall.getFunction().getMappingType() != MappingType.RowMapping) {
            throw new IllegalArgumentException("function should be set mapping function");
          }
          this.functionCallList.add(functionCall);
        });
  }

  public List<FunctionCall> getFunctionCallList() {
    return functionCallList;
  }

  @Override
  public Operator copy() {
    return new RowTransform(getSource().copy(), new ArrayList<>(functionCallList));
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new RowTransform(source, new ArrayList<>(functionCallList));
  }

  public String getInfo() {
    StringBuilder sb = new StringBuilder();
    sb.append("FuncList(Name, FuncType): ");
    for (FunctionCall functionCall : functionCallList) {
      sb.append(functionCall.getNameAndFuncTypeStr());
      sb.append(", ");
    }

    sb.append("MappingType: RowMapping");

    return sb.toString();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    RowTransform that = (RowTransform) object;
    return functionCallList.equals(that.functionCallList);
  }
}
