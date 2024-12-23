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
package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.shared.operator.RemoveNullColumn;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import com.google.auto.service.AutoService;

@AutoService(Rule.class)
public class AllowNullColumnRule extends Rule {

  public AllowNullColumnRule() {
    super(
        AllowNullColumnRule.class.getSimpleName(),
        operand(RemoveNullColumn.class, any()),
        Long.MAX_VALUE,
        RuleStrategy.FIXED_POINT);
  }

  @Override
  public boolean matches(RuleCall call) {
    RemoveNullColumn removeNullColumn = (RemoveNullColumn) call.getMatchedRoot();
    return removeNullColumn.getSource().getType() == SourceType.Operator;
  }

  @Override
  public void onMatch(RuleCall call) {
    RemoveNullColumn removeNullColumn = (RemoveNullColumn) call.getMatchedRoot();
    OperatorSource source = (OperatorSource) removeNullColumn.getSource();
    call.transformTo(source.getOperator());
  }
}
