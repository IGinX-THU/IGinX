package cn.edu.tsinghua.iginx.logical.optimizer.core;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.logical.optimizer.core.iterator.MatchOrder;
import cn.edu.tsinghua.iginx.logical.optimizer.rules.Rule;
import java.util.List;

public interface Planner {

  // unban a single rule
  void unbanRule(Rule rule);

  // unban a set of rules，e.g.PPD
  void unbanRuleCollection(List<Rule> rules);

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
