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
import java.util.List;
import java.util.stream.Collectors;

public class FoldedOperator extends AbstractMultipleOperator {

  private final Operator incompleteRoot;

  public FoldedOperator(List<Source> sources, Operator incompleteRoot) {
    super(OperatorType.Folded, sources);
    this.incompleteRoot = incompleteRoot;
  }

  public Operator getIncompleteRoot() {
    return incompleteRoot;
  }

  @Override
  public Operator copy() {
    return copyWithSource(getSources().stream().map(Source::copy).collect(Collectors.toList()));
  }

  @Override
  public MultipleOperator copyWithSource(List<Source> sources) {
    return new FoldedOperator(sources, incompleteRoot.copy());
  }

  @Override
  public String getInfo() {
    return "";
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    return object != null && getClass() == object.getClass();
  }
}
