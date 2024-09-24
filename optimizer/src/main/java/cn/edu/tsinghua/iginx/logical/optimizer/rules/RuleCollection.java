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
