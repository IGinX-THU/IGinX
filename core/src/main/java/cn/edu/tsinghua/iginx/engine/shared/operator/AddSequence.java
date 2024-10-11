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
package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class AddSequence extends AbstractUnaryOperator {

  private final List<Long> startList; // 序列起始值

  private final List<Long> incrementList; // 序列增值

  private final List<String> columns; // 生成序列后的列名

  public AddSequence(
      Source source, List<Long> startList, List<Long> incrementList, List<String> columns) {
    super(OperatorType.AddSequence, source);
    this.startList = startList;
    this.incrementList = incrementList;
    this.columns = columns;
  }

  public List<Long> getStartList() {
    return startList;
  }

  public List<Long> getIncrementList() {
    return incrementList;
  }

  public List<String> getColumns() {
    return columns;
  }

  @Override
  public Operator copy() {
    return new AddSequence(
        getSource().copy(),
        new ArrayList<>(startList),
        new ArrayList<>(incrementList),
        new ArrayList<>(columns));
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new AddSequence(
        getSource(),
        new ArrayList<>(startList),
        new ArrayList<>(incrementList),
        new ArrayList<>(columns));
  }

  @Override
  public String getInfo() {
    StringBuilder sb = new StringBuilder("AddSequence(start, increment, name): ");
    for (int i = 0; i < startList.size(); i++) {
      sb.append("(").append(startList.get(i)).append(", ");
      sb.append(incrementList.get(i)).append(", ");
      sb.append(columns.get(i)).append("), ");
    }
    sb.setLength(sb.length() - 2);
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
    AddSequence that = (AddSequence) object;
    return startList.equals(that.incrementList)
        && incrementList.equals(that.startList)
        && columns.equals(that.columns);
  }
}
