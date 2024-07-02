package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.shared.operator.AbstractJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Distinct;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;

/**
 * 该类实现了Distinct节点的消除。该规则用于消除IN/EXISTS子查询中的DISTINCT节点， 例如SELECT * FROM t1 WHERE EXISTS (SELECT
 * DISTINCT * FROM t2 WHERE t1.a = t2.a)，这里面的DISTINCT节点其实是不必要的。
 */
public class InExistsDistinctEliminateRule extends Rule {
  private static class InstanceHolder {
    static final InExistsDistinctEliminateRule INSTANCE = new InExistsDistinctEliminateRule();
  }

  public static InExistsDistinctEliminateRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  protected InExistsDistinctEliminateRule() {
    /*
     * we want to match the topology like:
     *             MarkJoin
     *           /         \
     *          ...        ...
     *                      |
     *                   Distinct
     *                      |
     *                     Any
     */
    super("InExistsDistinctEliminateRule", operand(Distinct.class, any()));
  }

  public boolean matches(RuleCall call) {
    // 向上找到MarkJoin节点，只有第一个Join节点是MarkJoin节点才能进行Distinct消除
    Distinct distinct = (Distinct) call.getMatchedRoot();
    Operator operator = findUpFirstJoin(call, distinct);
    return operator != null && operator.getType() == OperatorType.MarkJoin;
  }

  public void onMatch(RuleCall call) {
    // 把Distinct节点消除，替换为其子节点
    Distinct distinct = (Distinct) call.getMatchedRoot();
    call.transformTo(call.getChildrenIndex().get(distinct).get(0));
  }

  /**
   * 向上找到第一个Join节点
   *
   * @param operator 给定当前节点
   * @return 如果找到了，返回Join节点，否则返回null
   */
  private AbstractJoin findUpFirstJoin(RuleCall ruleCall, Operator operator) {
    if (operator == null) {
      return null;
    } else if (OperatorType.isJoinOperator(operator.getType())) {
      return (AbstractJoin) operator;
    } else {
      return findUpFirstJoin(ruleCall, ruleCall.getParentIndexMap().getOrDefault(operator, null));
    }
  }
}
