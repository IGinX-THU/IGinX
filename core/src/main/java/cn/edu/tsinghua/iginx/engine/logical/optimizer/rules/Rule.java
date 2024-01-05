package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.Operand;
import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.Arrays;

public abstract class Rule {

  private final String ruleName;

  /** operand describes the local topology we want to match in this rule */
  private final Operand operand;

  protected Rule(String ruleName, Operand operand) {
    this.ruleName = ruleName;
    this.operand = operand;
  }

  public String getRuleName() {
    return ruleName;
  }

  public Operand getOperand() {
    return operand;
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
