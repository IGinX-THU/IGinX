package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;

public class FilterPushDownProjectRule extends Rule {

  private static final class InstanceHolder {
    static final FilterPushDownProjectRule INSTANCE = new FilterPushDownProjectRule();
  }

  public static FilterPushDownProjectRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  protected FilterPushDownProjectRule() {
    /*
     * we want to match the topology like:
     *         Select
     *           |
     *        Project
     */
    super("FilterPushDownProject", operand(Select.class, operand(Project.class)));
  }

  @Override
  public boolean matches(RuleCall call) {
    return super.matches(call);
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
