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
