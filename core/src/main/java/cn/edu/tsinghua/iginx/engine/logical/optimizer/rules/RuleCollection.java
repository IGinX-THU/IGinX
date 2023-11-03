package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public class RuleCollection {

  private final List<Rule> rules = new ArrayList<>();

  private final Set<Rule> bannedRules = new HashSet<>();

  private static final class InstanceHolder {
    static final RuleCollection INSTANCE = new RuleCollection();
  }

  public static RuleCollection getInstance() {
    return InstanceHolder.INSTANCE;
  }

  public void addRule(Rule rule) {
    this.rules.add(rule);
  }

  public void addRules(List<Rule> rules) {
    this.rules.addAll(rules);
  }

  public void banRule(Rule rule) {
    this.bannedRules.add(rule);
  }

  public Iterator<Rule> iterator() {
    return new RuleIterator();
  }

  class RuleIterator implements Iterator<Rule> {

    int index = 0;
    Rule curRule = null;

    @Override
    public boolean hasNext() {
      if (index >= rules.size()) {
        return false;
      }
      for (int i = index; i < rules.size(); i++) {
        Rule rule = rules.get(i);
        if (!bannedRules.contains(rule)) {
          curRule = rule;
          break;
        }
      }
      return curRule != null;
    }

    @Override
    public Rule next() {
      if (hasNext()) {
        throw new NoSuchElementException("rule iterator has reached the end");
      }
      Rule ret = curRule;
      curRule = null;
      return ret;
    }
  }
}
