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

import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.AddSchemaPrefix;
import cn.edu.tsinghua.iginx.engine.shared.operator.GroupBy;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import com.google.auto.service.AutoService;

@AutoService(Rule.class)
public class AggPushDownAddSchemaPrefixRule extends Rule {

  public AggPushDownAddSchemaPrefixRule() {
    /*
     * we want to match the topology like:
     *         GroupBy
     *           |
     *         AddSchemaPrefix
     */
    super(
        AggPushDownAddSchemaPrefixRule.class.getName(),
        "AggPushDownRule",
        operand(GroupBy.class, operand(AddSchemaPrefix.class, any())));
  }

  @Override
  public boolean matches(RuleCall call) {
    return super.matches(call);
  }

  @Override
  public void onMatch(RuleCall call) {
    GroupBy groupBy = (GroupBy) call.getMatchedRoot();
    AddSchemaPrefix asp = (AddSchemaPrefix) call.getChildrenIndex().get(groupBy).get(0);
    String prefix = asp.getSchemaPrefix();

    groupBy.getGroupByCols().replaceAll(col -> removePrefix(col, prefix));

    for (FunctionCall fc : groupBy.getFunctionCallList()) {
      fc.getParams().getPaths().replaceAll(path -> removePrefix(path, prefix));
    }

    groupBy.setSource(asp.getSource());
    asp.setSource(new OperatorSource(groupBy));
    call.transformTo(asp);
  }

  private String removePrefix(String path, String prefix) {
    if (prefix != null && path.startsWith(prefix + ".")) {
      return path.substring(prefix.length() + 1);
    }
    return path;
  }
}
