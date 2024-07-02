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
package cn.edu.tsinghua.iginx.optimizer;

import cn.edu.tsinghua.iginx.logical.optimizer.rules.RuleCollection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RBOTestUtils {

  private static final RuleCollection ruleCollection = RuleCollection.INSTANCE;

  /**
   * 禁止给定规则之外的规则
   *
   * @param ruleNames 给定规则名列表
   * @return 被禁止的规则名列表
   */
  public static List<String> banRuleExceptGivenRule(List<String> ruleNames) {
    Map<String, Boolean> ruleMap = ruleCollection.getRulesInfo();
    List<String> bannedRules = new ArrayList<>();
    for (String rule : ruleMap.keySet()) {
      if (!ruleNames.contains(rule)) {
        bannedRules.add(rule);
      }
    }
    ruleCollection.banRulesByName(bannedRules);
    return bannedRules;
  }

  /**
   * 禁止给定规则之外的规则
   *
   * @param ruleName 给定规则名
   * @return 被禁止的规则名列表
   */
  public static List<String> banRuleExceptGivenRule(String ruleName) {
    Map<String, Boolean> ruleMap = ruleCollection.getRulesInfo();
    List<String> bannedRules = new ArrayList<>();
    for (String rule : ruleMap.keySet()) {
      if (!ruleName.equals(rule)) {
        bannedRules.add(rule);
      }
    }
    ruleCollection.banRulesByName(bannedRules);
    return bannedRules;
  }
}
