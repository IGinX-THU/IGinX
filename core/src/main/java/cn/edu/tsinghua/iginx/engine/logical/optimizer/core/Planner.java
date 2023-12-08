package cn.edu.tsinghua.iginx.engine.logical.optimizer.core;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.iterator.MatchOrder;
import cn.edu.tsinghua.iginx.engine.logical.optimizer.rules.Rule;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.List;

public interface Planner {

  // add single rule
  void addRule(Rule rule);

  // add a set of rules，e.g.PPD
  void addRuleCollection(List<Rule> rules);

  // set up the unoptimized query tree and initialize the optimization context
  void setRoot(Operator root);

  // set the maximum number of rule matches
  void setMatchLimit(int matchLimit);

  // set the maximum time limit for rule matching, unit: ms
  void setLimitTime(long limitTime);

  // set the Rule matching order for this query tree, depth-first, leveled etc
  void setMatchOrder(MatchOrder order);

  // get the optimized query tree
  Operator findBest();
}
