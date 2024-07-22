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
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.List;

public class Rename extends AbstractUnaryOperator {

  private final List<Pair<String, String>> aliasMap;

  private final List<String> ignorePatterns; // 不进行重命名的列

  public Rename(Source source, List<Pair<String, String>> aliasMap) {
    this(source, aliasMap, new ArrayList<>());
  }

  public Rename(Source source, List<Pair<String, String>> aliasMap, List<String> ignorePatterns) {
    super(OperatorType.Rename, source);
    if (aliasMap == null) {
      throw new IllegalArgumentException("aliasMap shouldn't be null");
    }
    this.aliasMap = aliasMap;
    this.ignorePatterns = ignorePatterns;
  }

  public List<Pair<String, String>> getAliasMap() {
    return aliasMap;
  }

  public List<String> getIgnorePatterns() {
    return ignorePatterns;
  }

  @Override
  public Operator copy() {
    return new Rename(getSource().copy(), new ArrayList<>(aliasMap));
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new Rename(source, new ArrayList<>(aliasMap));
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    builder.append("AliasMap: ");
    aliasMap.forEach(
        p -> builder.append("(").append(p.k).append(", ").append(p.v).append(")").append(","));
    builder.deleteCharAt(builder.length() - 1);
    if (!ignorePatterns.isEmpty()) {
      builder.append(", IgnorePatterns: ").append(ignorePatterns);
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
    Rename rename = (Rename) object;
    return aliasMap.equals(rename.aliasMap) && ignorePatterns.equals(rename.ignorePatterns);
  }
}
