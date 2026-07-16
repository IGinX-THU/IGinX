/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.engine.shared.operator;

import static cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils.isCanUseSetQuantifierFunction;

import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SetTransform extends AbstractUnaryOperator {

  private final List<FunctionCall> functionCallList;

  public SetTransform(Source source, List<FunctionCall> functionCallList) {
    super(OperatorType.SetTransform, source);
    this.functionCallList = new ArrayList<>();

    if (functionCallList == null) {
      throw new IllegalArgumentException("functionCallList shouldn't be null or empty");
    }

    for (FunctionCall functionCall : functionCallList) {
      if (functionCall.getParams().isDistinct()
          != functionCallList.get(0).getParams().isDistinct()) {
        throw new IllegalArgumentException(
            "functionCallList should have the same distinct property");
      }

      if (functionCall == null || functionCall.getFunction() == null) {
        throw new IllegalArgumentException("function shouldn't be null");
      }
      if (functionCall.getFunction().getMappingType() != MappingType.SetMapping) {
        throw new IllegalArgumentException("function should be set mapping function");
      }
      this.functionCallList.add(functionCall);
      if (isDistinct()
          && !isCanUseSetQuantifierFunction(functionCall.getFunction().getIdentifier())) {
        throw new IllegalArgumentException(
            "function " + functionCall.getFunction().getIdentifier() + " can't use DISTINCT");
      }
    }
  }

  public List<FunctionCall> getFunctionCallList() {
    return functionCallList;
  }

  public boolean isDistinct() {
    // 所有的functionCall的distinct属性都相同，所以只需要看第一个就可以了
    if (functionCallList.size() > 0) {
      return functionCallList.get(0).getParams().isDistinct();
    }
    return false;
  }

  @Override
  public Operator copy() {
    return new SetTransform(
        getSource().copy(),
        functionCallList.stream().map(FunctionCall::copy).collect(Collectors.toList()));
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new SetTransform(source, new ArrayList<>(functionCallList));
  }

  @Override
  public String getInfo() {
    StringBuilder sb = new StringBuilder();
    sb.append("FuncList(Name, FuncType): ");
    for (FunctionCall functionCall : functionCallList) {
      sb.append(functionCall.getNameAndFuncTypeStr());
      sb.append(", ");
    }

    sb.append("MappingType: SetMapping, ");

    if (isDistinct()) {
      sb.append("isDistinct: true");
    } else {
      sb.append("isDistinct: false");
    }

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
    SetTransform that = (SetTransform) object;
    return functionCallList.equals(that.functionCallList); // todo
  }
}
