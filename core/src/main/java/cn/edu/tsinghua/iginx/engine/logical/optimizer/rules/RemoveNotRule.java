package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import static cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils.removeNot;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;

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
     *          Any
     */
    super("RemoveNotRule", operand(Select.class, any()));
  }

  @Override
  public boolean matches(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    Filter filter = select.getFilter();
    final boolean[] hasNot = {false};
    filter.accept(
        new FilterVisitor() {
          @Override
          public void visit(AndFilter filter) {}

          @Override
          public void visit(OrFilter filter) {}

          @Override
          public void visit(NotFilter filter) {
            hasNot[0] = true;
          }

          @Override
          public void visit(KeyFilter filter) {}

          @Override
          public void visit(ValueFilter filter) {}

          @Override
          public void visit(PathFilter filter) {}

          @Override
          public void visit(BoolFilter filter) {}

          @Override
          public void visit(ExprFilter filter) {}
        });

    return hasNot[0];
  }

  @Override
  public void onMatch(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    select.setFilter(removeNot(select.getFilter()));
  }
}
