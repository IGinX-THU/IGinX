package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuleCollection {

  private static final Logger logger = LoggerFactory.getLogger(RuleCollection.class);

  private final Map<String, Rule> rules = new HashMap<>();

  private final Map<String, Rule> bannedRules = new HashMap<>();

  private static final class InstanceHolder {
    static final RuleCollection INSTANCE = new RuleCollection();
  }

  public static RuleCollection getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private RuleCollection() {
    // add rules here
    addRule(RemoveNotRule.getInstance());
    addRule(FilterFragmentRule.getInstance());
  }

  private void addRule(Rule rule) {
    rules.put(rule.getRuleName(), rule);
  }

  public void unbanRule(Rule rule) {
    bannedRules.remove(rule);
  }

  public void unbanRules(List<Rule> rules) {
    rules.forEach(rule -> bannedRules.remove(rule.getRuleName()));
  }

  public boolean unbanRulesByName(List<String> ruleNames) {
    ruleNames.forEach(bannedRules::remove);
    return true;
  }

  public boolean banRules(Rule rule) {
    if (!rules.containsKey(rule.getRuleName())) {
      logger.error("IGinX rule collection does not include rule: " + rule.getRuleName());
      return false;
    }
    bannedRules.put(rule.getRuleName(), rule);
    return true;
  }

  public boolean banRulesByName(List<String> ruleNames) {
    for (String ruleName : ruleNames) {
      if (!rules.containsKey(ruleName)) {
        logger.error("IGinX rule collection does not include rule: " + ruleName);
        return false;
      }
      bannedRules.put(ruleName, rules.get(ruleName));
    }
    return true;
  }

  public Iterator<Rule> iterator() {
    // ensure that this round of optimization will not be affected by rule set modifications
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
