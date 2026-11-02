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
package cn.edu.tsinghua.iginx.engine.shared.source;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ConstantSource extends AbstractSource {
  private final List<Expression> expressionList;

  public ConstantSource(List<Expression> expressionList) {
    super(SourceType.Constant);
    this.expressionList = new ArrayList<>(expressionList);
  }

  public List<Expression> getExpressionList() {
    return expressionList;
  }

  @Override
  public Source copy() {
    return new ConstantSource(
        expressionList.stream().map(ExprUtils::copy).collect(Collectors.toList()));
  }
}
