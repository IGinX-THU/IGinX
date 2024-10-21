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

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import com.google.auto.service.AutoService;

@AutoService(Rule.class)
public class FilterPushDownSelectRule extends Rule {

  public FilterPushDownSelectRule() {
    /*
     * we want to match the topology like:
     *         Select
     *           |
     *         Select
     */
    super("FilterPushDownSelectRule", operand(Select.class, operand(Select.class, any())));
  }

  @Override
  public boolean matches(RuleCall call) {
    return super.matches(call);
  }

  @Override
  public void onMatch(RuleCall call) {
    // 应当把两个Select合并，用and连接
    Select select = (Select) call.getMatchedRoot();
    Select childSelect = (Select) call.getChildrenIndex().get(select).get(0);

    childSelect.setFilter(
        LogicalFilterUtils.mergeFilter(select.getFilter(), childSelect.getFilter()));
    call.transformTo(childSelect);
  }
}
