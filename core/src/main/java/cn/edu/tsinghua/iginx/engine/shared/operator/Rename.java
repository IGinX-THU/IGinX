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

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Rename extends AbstractUnaryOperator {

  private final Map<String, String> aliasMap;

  private final List<String> ignorePatterns; // 不进行重命名的列

  public Rename(Source source, Map<String, String> aliasMap) {
    this(source, aliasMap, new ArrayList<>());
  }

  public Rename(Source source, Map<String, String> aliasMap, List<String> ignorePatterns) {
    super(OperatorType.Rename, source);
    if (aliasMap == null) {
      throw new IllegalArgumentException("aliasMap shouldn't be null");
    }
    this.aliasMap = aliasMap;
    this.ignorePatterns = ignorePatterns;
  }

  public Map<String, String> getAliasMap() {
    return aliasMap;
  }

  public List<String> getIgnorePatterns() {
    return ignorePatterns;
  }

  @Override
  public Operator copy() {
    return new Rename(getSource().copy(), new HashMap<>(aliasMap));
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new Rename(source, new HashMap<>(aliasMap));
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    builder.append("AliasMap: ");
    aliasMap.forEach(
        (k, v) -> builder.append("(").append(k).append(", ").append(v).append(")").append(","));
    builder.deleteCharAt(builder.length() - 1);
    if (!ignorePatterns.isEmpty()) {
      builder.append(", IgnorePatterns: ");
      builder.append(ignorePatterns);
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
