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

import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import com.google.auto.service.AutoService;

@AutoService(Rule.class)
public class MarkLoadRule extends Rule {

  private MarkLoadRule() {
    super("MarkLoadRule", any());
  }

  @Override
  public boolean matches(RuleCall call) {
    switch (call.getMatchedRoot().getType()) {
      case GroupBy:
      case Join:
      case InnerJoin:
      case OuterJoin:
      case CrossJoin:
      case MarkJoin:
        return true;
      default:
        return false;
    }
  }

  @Override
  public void onMatch(RuleCall call) {
    //    Operator matchedRoot = call.getMatchedRoot();
    //    if (matchedRoot instanceof UnaryOperator) {
    //      // deal with GroupBy
    //      UnaryOperator unaryOp = (UnaryOperator) matchedRoot;
    //      Source newSource = new OperatorSource(new Load(unaryOp.getSource()));
    //      unaryOp.setSource(newSource);
    //    } else if (matchedRoot instanceof BinaryOperator) {
    //      // deal with Join
    //      BinaryOperator binOp = (BinaryOperator) matchedRoot;
    //      Source newSourceA = new OperatorSource(new Load(binOp.getSourceA()));
    //      Source newSourceB = new OperatorSource(new Load(binOp.getSourceB()));
    //      binOp.setSourceA(newSourceA);
    //      binOp.setSourceB(newSourceB);
    //    }
  }
}
