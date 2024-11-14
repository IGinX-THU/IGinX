/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.logical.optimizer.IRuleCollection;
import java.util.*;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuleCollection implements IRuleCollection, Iterable<Rule> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RuleCollection.class);

  public static final RuleCollection INSTANCE = new RuleCollection();

  private final Map<String, Rule> rules = new HashMap<>();

  private final Map<String, Rule> bannedRules = new HashMap<>();

  private final ConfigDescriptor configDescriptor = ConfigDescriptor.getInstance();

  protected RuleCollection() {
    addRulesBySPI();
    setRulesByConfig();
  }

  private void addRulesBySPI() {
    if (LOGGER.isDebugEnabled()) {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      LOGGER.debug("ClassLoader: {}", cl);
      String path = System.getProperty("java.class.path");
      LOGGER.debug("ClassPath: {}", path);
    }

    for (Rule rule : ServiceLoader.load(Rule.class)) {
      LOGGER.debug("Add rule by SPI: {}", rule);
      addRule(rule);
    }
  }

  /** 根据配置文件设置rules，未在配置文件中出现的规则默认为off */
  private void setRulesByConfig() {
    Config config = configDescriptor.getConfig();
    String[] ruleSettingList = config.getRuleBasedOptimizer().split(",");

    Set<String> banRules = getAllGroup();

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

    banRulesGroupByName(banRules);
  }

  private Set<String> getAllGroup() {
    Set<String> group = new HashSet<>();
    for (Rule rule : rules.values()) {
      group.add(rule.getRuleGroupName());
    }
    return group;
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

  public boolean banRulesGroupByName(Collection<String> ruleGroupNames) {
    for (String ruleGroupName : ruleGroupNames) {
      banRulesGroup(ruleGroupName);
    }
    return true;
  }

  public void banRulesGroup(String ruleGroupName) {
    for (Rule rule : rules.values()) {
      if (rule.getRuleGroupName().equals(ruleGroupName)) {
        banRules(rule);
      }
    }
  }

  public void unbanRulesGroup(String ruleGroupName) {
    for (Rule rule : rules.values()) {
      if (rule.getRuleGroupName().equals(ruleGroupName)) {
        unbanRule(rule);
      }
    }
  }

  public boolean setRules(Map<String, Boolean> rulesChange) {
    for (String ruleGroupName : rulesChange.keySet()) {
      if (rulesChange.get(ruleGroupName)) {
        unbanRulesGroup(ruleGroupName);
      } else {
        banRulesGroup(ruleGroupName);
      }
    }

    return true;
  }

  public Map<String, Boolean> getRulesInfo() {
    Map<String, Boolean> rulesInfo = new HashMap<>();
    for (Rule rule : rules.values()) {
      rulesInfo.put(rule.getRuleGroupName(), !bannedRules.containsKey(rule.getRuleName()));
    }
    return rulesInfo;
  }

  @Override
  @Nonnull
  public Iterator<Rule> iterator() {
    // ensure that this round of optimization will not be affected by rule set modifications
    // 确保这一轮优化不会受到规则集修改的影响
    List<Rule> rules = new ArrayList<>(this.rules.values());
    Set<Rule> bannedRules = new HashSet<>(this.bannedRules.values());

    // sort rules by priority and rule name
    rules.sort(Comparator.comparingLong(Rule::getPriority).thenComparing(Rule::getRuleName));
    return new RuleIterator(rules, bannedRules);
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
