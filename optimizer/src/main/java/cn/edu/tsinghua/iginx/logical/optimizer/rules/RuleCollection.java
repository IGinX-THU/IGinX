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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.logical.optimizer.IRuleCollection;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum RuleCollection implements IRuleCollection {
  INSTANCE;

  private static final Logger LOGGER = LoggerFactory.getLogger(RuleCollection.class);

  private final Map<String, Rule> rules = new HashMap<>();

  private final Map<String, Rule> bannedRules = new HashMap<>();

  private ConfigDescriptor configDescriptor = ConfigDescriptor.getInstance();

  private RuleCollection() {
    // add rules here
    // 在这里添加规则
    addRule(NotFilterRemoveRule.getInstance());
    addRule(FragmentPruningByFilterRule.getInstance());
    addRule(ColumnPruningRule.getInstance());
    addRule(FragmentPruningByPatternRule.getInstance());
    addRule(ConstantPropagationRule.getInstance());
    addRule(FilterConstantFoldingRule.getInstance());
    addRule(RowTransformConstantFoldingRule.getInstance());
    addRule(FunctionDistinctEliminateRule.getInstance());
    addRule(InExistsDistinctEliminateRule.getInstance());
    addRule(FilterPushDownAddSchemaPrefixRule.getInstance());
    addRule(FilterPushDownGroupByRule.getInstance());
    addRule(FilterPushDownPathUnionJoinRule.getInstance());
    addRule(FilterPushDownProjectReorderSortRule.getInstance());
    addRule(FilterPushDownRenameRule.getInstance());
    addRule(FilterPushDownSelectRule.getInstance());
    addRule(FilterPushDownSetOpRule.getInstance());
    addRule(FilterPushDownTransformRule.getInstance());
    addRule(FilterPushIntoJoinConditionRule.getInstance());
    addRule(FilterPushOutJoinConditionRule.getInstance());

    setRulesByConfig();
  }

  /** 根据配置文件设置rules，未在配置文件中出现的规则默认为off */
  private void setRulesByConfig() {
    Config config = configDescriptor.getConfig();
    String[] ruleSettingList = config.getRuleBasedOptimizer().split(",");

    Set<String> banRules = new HashSet<>(rules.keySet());

    for (String ruleSetting : ruleSettingList) {
      String[] ruleInfo = ruleSetting.split("=");
      if (ruleInfo.length != 2) {
        LOGGER.error("Rule setting error: {}", ruleSetting);
        continue;
      }

      if (ruleInfo[1].equalsIgnoreCase("on")) {
        banRules.remove(ruleInfo[0]);
      }
    }

    banRulesByName(banRules);
  }

  private void addRule(Rule rule) {
    rules.put(rule.getRuleName(), rule);
  }

  public void unbanRule(Rule rule) {
    bannedRules.remove(rule.getRuleName());
  }

  public void unbanRules(List<Rule> rules) {
    rules.forEach(rule -> bannedRules.remove(rule.getRuleName()));
  }

  public boolean unbanRulesByName(List<String> ruleNames) {
    ruleNames.forEach(bannedRules::remove);
    return true;
  }

  public boolean unbanRuleByName(String ruleName) {
    bannedRules.remove(ruleName);
    return true;
  }

  public boolean banRules(Rule rule) {
    if (!rules.containsKey(rule.getRuleName())) {
      LOGGER.error("IGinX rule collection does not include rule: {}", rule.getRuleName());
      return false;
    }
    bannedRules.put(rule.getRuleName(), rule);
    return true;
  }

  public boolean banRulesByName(Collection<String> ruleNames) {
    for (String ruleName : ruleNames) {
      if (!rules.containsKey(ruleName)) {
        LOGGER.error("IGinX rule collection does not include rule: {}", ruleName);
        return false;
      }
      bannedRules.put(ruleName, rules.get(ruleName));
    }
    return true;
  }

  public boolean banRuleByName(String ruleName) {
    if (!rules.containsKey(ruleName)) {
      LOGGER.error("IGinX rule collection does not include rule: {}", ruleName);
      return false;
    }
    bannedRules.put(ruleName, rules.get(ruleName));
    return true;
  }

  public boolean setRules(Map<String, Boolean> rulesChange) {
    // Check whether any rule does not exist before setting it
    // 先检查是否有不存在的规则，再进行设置
    for (String ruleName : rulesChange.keySet()) {
      if (!rules.containsKey(ruleName)) {
        LOGGER.error("IGinX rule collection does not include rule: {}", ruleName);
        return false;
      }
    }

    for (String ruleName : rulesChange.keySet()) {
      if (rulesChange.get(ruleName)) {
        unbanRule(rules.get(ruleName));
      } else {
        banRules(rules.get(ruleName));
      }
    }

    return true;
  }

  public Map<String, Boolean> getRulesInfo() {
    Map<String, Boolean> rulesInfo = new HashMap<>();
    for (String ruleName : rules.keySet()) {
      rulesInfo.put(ruleName, !bannedRules.containsKey(ruleName));
    }
    return rulesInfo;
  }

  public Iterator<Rule> iterator() {
    // ensure that this round of optimization will not be affected by rule set modifications
    // 确保这一轮优化不会受到规则集修改的影响
    return new RuleIterator(new ArrayList<>(rules.values()), new HashSet<>(bannedRules.values()));
  }

  static class RuleIterator implements Iterator<Rule> {

    private final List<Rule> rules;

    private final Set<Rule> bannedRules;

    int index = 0;
    Rule curRule = null;

    public RuleIterator(List<Rule> rules, Set<Rule> bannedRules) {
      this.rules = rules;
      this.bannedRules = bannedRules;
    }

    @Override
    public boolean hasNext() {
      if (curRule != null) {
        return true;
      }
      for (; index < rules.size(); index++) {
        Rule rule = rules.get(index);
        if (!bannedRules.contains(rule)) {
          curRule = rule;
          index++;
          break;
        }
      }
      return curRule != null;
    }

    @Override
    public Rule next() {
      if (!hasNext()) {
        throw new NoSuchElementException("rule iterator has reached the end");
      }
      Rule ret = curRule;
      curRule = null;
      return ret;
    }
  }
}
