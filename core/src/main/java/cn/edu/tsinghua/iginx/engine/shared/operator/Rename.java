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

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.List;

public class Rename extends AbstractUnaryOperator {

  private final List<Pair<String, String>> aliasList;

  private final List<String> ignorePatterns; // 不进行重命名的列

  public Rename(Source source, List<Pair<String, String>> aliasList) {
    this(source, aliasList, new ArrayList<>());
  }

  public Rename(Source source, List<Pair<String, String>> aliasList, List<String> ignorePatterns) {
    super(OperatorType.Rename, source);
    if (aliasList == null) {
      throw new IllegalArgumentException("aliasList shouldn't be null");
    }
    this.aliasList = aliasList;
    this.ignorePatterns = ignorePatterns;
  }

  public List<Pair<String, String>> getAliasList() {
    return aliasList;
  }

  public List<String> getIgnorePatterns() {
    return ignorePatterns;
  }

  @Override
  public Operator copy() {
    return new Rename(
        getSource().copy(), new ArrayList<>(aliasList), new ArrayList<>(ignorePatterns));
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new Rename(source, new ArrayList<>(aliasList));
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    builder.append("AliasList: ");
    aliasList.forEach(
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
    return aliasList.equals(rename.aliasList) && ignorePatterns.equals(rename.ignorePatterns);
  }
}
