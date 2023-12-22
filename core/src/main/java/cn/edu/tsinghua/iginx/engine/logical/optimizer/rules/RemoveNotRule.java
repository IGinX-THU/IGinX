package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import static cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils.removeNot;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.visitor.OperatorVisitor;

public class RemoveNotRule extends Rule {

  private static final class InstanceHolder {
    static final RemoveNotRule INSTANCE = new RemoveNotRule();
  }

  public static RemoveNotRule getInstance() {
    return RemoveNotRule.InstanceHolder.INSTANCE;
  }

  protected RemoveNotRule() {
    /*
     * we want to match the topology like:
     *         Select
     *           |
     *              Any
     */
    super("RemoveNotRule", operand(Select.class, any()));
  }

  @Override
  public void onMatch(RuleCall call) {
    Operator root = call.getMatchedRoot();
    root.accept(
        new OperatorVisitor() {
          @Override
          public void visit(UnaryOperator unaryOperator) {
            if (unaryOperator.getType() == OperatorType.Select) {
              Select select = (Select) unaryOperator;
              select.setFilter(removeNot(select.getFilter()));
            }
          }

          @Override
          public void visit(BinaryOperator binaryOperator) {}

          @Override
          public void visit(MultipleOperator multipleOperator) {}
        });
  }
}
