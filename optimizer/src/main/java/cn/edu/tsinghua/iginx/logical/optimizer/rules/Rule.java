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
package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.logical.optimizer.core.Operand;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import java.util.Arrays;

public abstract class Rule {

  private final String ruleName;

  /** operand describes the local topology we want to match in this rule */
  private final Operand operand;

  private final RuleStrategy strategy;

  protected Rule(String ruleName, Operand operand) {
    this.ruleName = ruleName;
    this.operand = operand;
    this.strategy = RuleStrategy.FIXED_POINT;
  }

  protected Rule(String ruleName, Operand operand, RuleStrategy strategy) {
    this.ruleName = ruleName;
    this.operand = operand;
    this.strategy = strategy;
  }

  public String getRuleName() {
    return ruleName;
  }

  public Operand getOperand() {
    return operand;
  }

  public RuleStrategy getStrategy() {
    return strategy;
  }

  /**
   * Returns whether this rule could possibly match the given operands.
   *
   * <p>This method is an opportunity to apply side-conditions to a rule. The Planner calls this
   * method after matching all operands of the rule, and before calling method onMatch.
   *
   * <p>The default implementation of this method returns <code>true</code>.
   */
  public boolean matches(RuleCall call) {
    return true;
  }

  /** This method is used to modify the local topology after rule matching. */
  public abstract void onMatch(RuleCall call);

  public static Operand any() {
    return Operand.ANY_OPERAND;
  }

  public static Operand operand(Class<? extends Operator> clazz, Operand... children) {
    return new Operand(clazz, Arrays.asList(children));
  }
}
