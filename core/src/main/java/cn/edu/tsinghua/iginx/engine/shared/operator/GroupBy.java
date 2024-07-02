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
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class GroupBy extends AbstractUnaryOperator {

  private final List<String> groupByCols;

  private final List<FunctionCall> functionCallList;

  public GroupBy(Source source, List<String> groupByCols, List<FunctionCall> functionCallList) {
    super(OperatorType.GroupBy, source);
    if (groupByCols == null || groupByCols.isEmpty()) {
      throw new IllegalArgumentException("groupByCols shouldn't be null");
    }
    this.groupByCols = groupByCols;
    this.functionCallList = functionCallList;
  }

  public List<String> getGroupByCols() {
    return groupByCols;
  }

  public List<FunctionCall> getFunctionCallList() {
    return functionCallList;
  }

  @Override
  public Operator copy() {
    return new GroupBy(
        getSource().copy(), new ArrayList<>(groupByCols), new ArrayList<>(functionCallList));
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new GroupBy(source, new ArrayList<>(groupByCols), new ArrayList<>(functionCallList));
  }

  public boolean isDistinct() {
    // 所有的functionCall的distinct属性都相同，所以只需要看第一个就可以了
    if (functionCallList.size() > 0) {
      return functionCallList.get(0).getParams().isDistinct();
    }
    return false;
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    builder.append("GroupByCols: ").append(String.join(",", groupByCols));
    if (functionCallList != null && !functionCallList.isEmpty()) {
      builder.append(", FuncList(Name, FuncType): ");
      for (FunctionCall functionCall : functionCallList) {
        builder.append(functionCall.getNameAndFuncTypeStr()).append(",");
      }
      builder.append(" MappingType: ");
      builder.append(functionCallList.get(0).getFunction().getMappingType());
      if (isDistinct()) {
        builder.append(" isDistinct: true");
      } else {
        builder.append(" isDistinct: false");
      }
    }
    return builder.toString();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    GroupBy that = (GroupBy) object;
    return groupByCols.equals(that.groupByCols) && functionCallList.equals(that.functionCallList);
  }
}
