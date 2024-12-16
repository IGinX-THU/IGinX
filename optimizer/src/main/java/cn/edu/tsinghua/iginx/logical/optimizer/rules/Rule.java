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

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.logical.optimizer.core.Operand;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import java.util.Arrays;

public abstract class Rule {

  public static final long DEFAULT_PRIORITY = 0; // 默认优先级为0,优先执行的规则设置为负数，后置执行的设置为正数

  public static final RuleStrategy DEFAULT_STRATEGY = RuleStrategy.FIXED_POINT;

  private final String ruleName;

  private final String ruleGroupName;

  /** operand describes the local topology we want to match in this rule */
  private final Operand operand;

  private final RuleStrategy strategy;

  private final long priority;

  protected Rule(String ruleName, Operand operand) {
    this(ruleName, operand, DEFAULT_PRIORITY, DEFAULT_STRATEGY);
  }

  protected Rule(String ruleName, String ruleGroupName, Operand operand) {
    this(ruleName, ruleGroupName, operand, DEFAULT_PRIORITY, DEFAULT_STRATEGY);
  }

  protected Rule(String ruleName, Operand operand, long priority) {
    this(ruleName, operand, priority, DEFAULT_STRATEGY);
  }

  protected Rule(String ruleName, Operand operand, RuleStrategy strategy) {
    this(ruleName, operand, DEFAULT_PRIORITY, strategy);
  }

  protected Rule(String ruleName, String ruleGroupName, Operand operand, RuleStrategy strategy) {
    this(ruleName, ruleGroupName, operand, DEFAULT_PRIORITY, strategy);
  }

  protected Rule(String ruleName, Operand operand, long priority, RuleStrategy strategy) {
    this.ruleName = ruleName;
    this.ruleGroupName = ruleName;
    this.operand = operand;
    this.priority = priority;
    this.strategy = strategy;
  }

  protected Rule(
      String ruleName,
      String ruleGroupName,
      Operand operand,
      long priority,
      RuleStrategy strategy) {
    this.ruleName = ruleName;
    this.ruleGroupName = ruleGroupName;
    this.operand = operand;
    this.priority = priority;
    this.strategy = strategy;
  }

  public String getRuleName() {
    return ruleName;
  }

  public String getRuleGroupName() {
    return ruleGroupName;
  }

  public Operand getOperand() {
    return operand;
  }

  public RuleStrategy getStrategy() {
    return strategy;
  }

  public long getPriority() {
    return priority;
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

  @Override
  public String toString() {
    return ruleName;
  }
}
