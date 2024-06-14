package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;

public class NotFilterRemoveRule extends Rule {

  private static final class InstanceHolder {
    static final NotFilterRemoveRule INSTANCE = new NotFilterRemoveRule();
  }

  public static NotFilterRemoveRule getInstance() {
    return NotFilterRemoveRule.InstanceHolder.INSTANCE;
  }

  protected NotFilterRemoveRule() {
    /*
     * we want to match the topology like:
     *         Select
     *           |
     *          Any
     */
    super("NotFilterRemoveRule", operand(Select.class, any()));
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
    select.setFilter(LogicalFilterUtils.removeNot(select.getFilter()));
  }
}
