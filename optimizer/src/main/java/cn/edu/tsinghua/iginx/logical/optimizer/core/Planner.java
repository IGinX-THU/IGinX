/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
