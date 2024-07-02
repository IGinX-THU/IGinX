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
import java.util.List;

public abstract class AbstractJoin extends AbstractBinaryOperator {

  private String prefixA;

  private String prefixB;

  private JoinAlgType joinAlgType;

  private List<String> extraJoinPrefix; // 连接时需要额外进行比较的列名

  public AbstractJoin(
      OperatorType type,
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      JoinAlgType joinAlgType,
      List<String> extraJoinPrefix) {
    super(type, sourceA, sourceB);
    this.prefixA = prefixA;
    this.prefixB = prefixB;
    this.joinAlgType = joinAlgType;
    this.extraJoinPrefix = extraJoinPrefix;
  }

  public String getPrefixA() {
    return prefixA;
  }

  public String getPrefixB() {
    return prefixB;
  }

  public JoinAlgType getJoinAlgType() {
    return joinAlgType;
  }

  public List<String> getExtraJoinPrefix() {
    return extraJoinPrefix;
  }

  public void setPrefixA(String prefixA) {
    this.prefixA = prefixA;
  }

  public void setPrefixB(String prefixB) {
    this.prefixB = prefixB;
  }

  public void setJoinAlgType(JoinAlgType joinAlgType) {
    this.joinAlgType = joinAlgType;
  }

  public void setExtraJoinPrefix(List<String> extraJoinPrefix) {
    this.extraJoinPrefix = extraJoinPrefix;
  }
}
