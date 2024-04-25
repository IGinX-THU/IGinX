package cn.edu.tsinghua.iginx.engine.distributedquery.coordinator;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.List;

public class Plan {

  private final Operator root;

  private final List<Operator> subPlans;

  public Plan(Operator root, List<Operator> subPlans) {
    this.root = root;
    this.subPlans = subPlans;
  }

  public Operator getRoot() {
    return root;
  }

  private Operator getSubPlan(int id) {
    assert id <= subPlans.size();
    return subPlans.get(id - 1);
  }

  public List<Operator> getSubPlans() {
    return subPlans;
  }
}
