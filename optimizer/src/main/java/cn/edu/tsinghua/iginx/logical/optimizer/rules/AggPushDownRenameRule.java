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

import cn.edu.tsinghua.iginx.engine.logical.utils.PathUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.google.auto.service.AutoService;
import java.util.List;

@AutoService(Rule.class)
public class AggPushDownRenameRule extends Rule {

  public AggPushDownRenameRule() {
    /*
     * we want to match the topology like:
     *         GroupBy
     *           |
     *         Rename
     */
    super(
        AggPushDownRenameRule.class.getName(),
        "AggPushDownRule",
        operand(GroupBy.class, operand(Rename.class, any())));
  }

  @Override
  public boolean matches(RuleCall call) {
    return true;
  }

  @Override
  public void onMatch(RuleCall call) {
    GroupBy groupBy = (GroupBy) call.getMatchedRoot();
    Rename rename = (Rename) call.getChildrenIndex().get(groupBy).get(0);
    List<String> gbc = groupBy.getGroupByCols();
    gbc.replaceAll(pattern -> PathUtils.recoverRenamedPattern(rename.getAliasList(), pattern));

    List<FunctionCall> functionCallList = groupBy.getFunctionCallList();
    for (FunctionCall functionCall : functionCallList) {
      String oldFuncStr = functionCall.getFunctionStr();
      functionCall
          .getParams()
          .getPaths()
          .replaceAll(pattern -> PathUtils.recoverRenamedPattern(rename.getAliasList(), pattern));
      String newFuncStr = functionCall.getFunctionStr();
      if (!oldFuncStr.equals(newFuncStr)) {
        rename.getAliasList().add(new Pair<>(newFuncStr, oldFuncStr));
      }
    }

    groupBy.setSource(rename.getSource());
    rename.setSource(new OperatorSource(groupBy));
    call.transformTo(rename);
  }
}
