package cn.edu.tsinghua.iginx.engine.logical.optimizer.core;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.iterator.MatchOrder;
import cn.edu.tsinghua.iginx.engine.logical.optimizer.rules.Rule;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.List;

public interface Planner {

  // 添加规则
  void addRule(Rule rule);

  // 添加一组规则，比如 PPD
  void addRuleCollection(List<Rule> rules);

  // 设置未优化过的查询树，初始化优化上下文
  void setRoot(Operator root);

  // 设置 rule match 次数的最大限制
  void setMatchLimit(int matchLimit);

  // 设置 rule match 的最大时间限制，单位：ms
  void setLimitTime(long limitTime);

  // 设置 Rule 对这棵查询树的匹配顺序，深度优先、自顶向下、自底向上......
  void setMatchOrder(MatchOrder order);

  // 获取优化后的最优查询树
  Operator findBest();
}
