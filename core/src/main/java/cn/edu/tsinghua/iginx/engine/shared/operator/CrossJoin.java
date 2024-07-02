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

import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class CrossJoin extends AbstractJoin {

  public CrossJoin(Source sourceA, Source sourceB, String prefixA, String prefixB) {
    this(sourceA, sourceB, prefixA, prefixB, new ArrayList<>());
  }

  public CrossJoin(
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      List<String> extraJoinPrefix) {
    super(
        OperatorType.CrossJoin,
        sourceA,
        sourceB,
        prefixA,
        prefixB,
        JoinAlgType.NestedLoopJoin,
        extraJoinPrefix);
  }

  @Override
  public Operator copy() {
    return new CrossJoin(
        getSourceA().copy(),
        getSourceB().copy(),
        getPrefixA(),
        getPrefixB(),
        new ArrayList<>(getExtraJoinPrefix()));
  }

  @Override
  public BinaryOperator copyWithSource(Source sourceA, Source sourceB) {
    return new CrossJoin(
        sourceA, sourceB, getPrefixA(), getPrefixB(), new ArrayList<>(getExtraJoinPrefix()));
  }

  @Override
  public String getInfo() {
    return "PrefixA: " + getPrefixA() + ", PrefixB: " + getPrefixB();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    CrossJoin that = (CrossJoin) object;
    return getPrefixA().equals(that.getPrefixA())
        && getPrefixB().equals(that.getPrefixB())
        && getExtraJoinPrefix().equals(that.getExtraJoinPrefix());
  }
}
