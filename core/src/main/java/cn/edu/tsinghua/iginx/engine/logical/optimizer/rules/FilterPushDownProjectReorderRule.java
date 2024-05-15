package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.AbstractUnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Reorder;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class FilterPushDownProjectReorderRule extends Rule {

  private static final Set<Class> validOps =
      new HashSet<>(Arrays.asList(Project.class, Reorder.class));

  private static final class InstanceHolder {
    static final FilterPushDownProjectReorderRule INSTANCE = new FilterPushDownProjectReorderRule();
  }

  public static FilterPushDownProjectReorderRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  protected FilterPushDownProjectReorderRule() {
    /*
     * we want to match the topology like:
     *         Select
     *           |
     *      Project/Reorder
     */
    super(
        "FilterPushDownProjectReorderRule",
        operand(Select.class, operand(AbstractUnaryOperator.class)));
  }

  @Override
  public boolean matches(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    AbstractUnaryOperator operator =
        (AbstractUnaryOperator) ((OperatorSource) select.getSource()).getOperator();
    return validOps.contains(operator.getClass());
  }

  @Override
  public void onMatch(RuleCall call) {
    // 应该没什么要注意的，单纯交换位置即可
    Select select = (Select) call.getMatchedRoot();
    Project project = (Project) call.getChildrenIndex().get(select).get(0);

    select.setSource(project.getSource());
    project.setSource(new OperatorSource(select));

    call.transformTo(project);
  }
}
